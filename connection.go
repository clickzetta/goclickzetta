package goclickzetta

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/clickzetta/goclickzetta/protos/bulkload/ingestion"
	"github.com/clickzetta/goclickzetta/protos/bulkload/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/valyala/fastjson"
)

type requestPath string

const (
	SubmitJobRequestPath requestPath = "/lh/submitJob"
	GetJobResultPath     requestPath = "/lh/getJob"
	CancelJobPath        requestPath = "/lh/cancelJob"
	GetTokenPath         requestPath = "/clickzetta-portal/user/loginSingle"
	GETWAYPATH           requestPath = "/igs/gatewayEndpoint"
)

// HTTPTransport is the default transport configuration.
// Deprecated: kept for backward compatibility. Each connection now creates its own transport.
var HTTPTransport = newHTTPTransport()

// newHTTPTransport creates a new http.Transport for each connection,
// so that closing one connection's transport won't affect others.
func newHTTPTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // 连接超时时间
			KeepAlive: 60 * time.Second, // 保持长连接的时间
		}).DialContext, // 设置连接的参数
		MaxIdleConns:          500,              // 最大空闲连接
		IdleConnTimeout:       60 * time.Second, // 空闲连接的超时时间
		ExpectContinueTimeout: 30 * time.Second, // 等待服务第一个响应的超时时间
		MaxIdleConnsPerHost:   100,              // 每个host保持的空闲连接数
	}
}

type atomicBool = atomic.Bool

type driverTimeoutError struct {
	msg string
}

func (e driverTimeoutError) Error() string   { return e.msg }
func (e driverTimeoutError) Timeout() bool   { return true }
func (e driverTimeoutError) Temporary() bool { return true }

type ClickzettaConn struct {
	ctx      context.Context
	cfg      *Config
	internal InternalClient
}

var (
	queryIDPattern = `[\w\-_]+`
	queryIDRegexp  = regexp.MustCompile(queryIDPattern)
	errMutex       = &sync.Mutex{}
)

func (conn *ClickzettaConn) exec(
	ctx context.Context,
	query string,
	noResult bool,
	isInternal bool,
	describeOnly bool,
	bindings []driver.NamedValue) (
	*execResponse, error) {
	logger.WithContext(ctx).Infof("exec: %v", query)
	if query == "" {
		logger.WithContext(ctx).Errorf("empty SQL query")
		return nil, driver.ErrSkip
	}
	if !strings.HasSuffix(query, ";") {
		query += ";"
	}

	// CZLH-57015: at most 1 re-execute with a new jobId (aligned with Java)
	const maxReExecute = 2
	for attempt := 0; attempt < maxReExecute; attempt++ {
		id := formatJobId()
		logger.WithContext(ctx).Infof("jobId: %v (attempt %d)", id, attempt+1)
		jid := jobId{
			ID:         id,
			Workspace:  conn.cfg.Workspace,
			InstanceId: 0,
		}

		res, err := conn.execInternal(ctx, query, jid, bindings)
		if err != nil {
			// check if re-execute is needed (CZLH-57015: regenerate jobId and retry)
			if _, ok := err.(*reExecuteError); ok {
				logger.WithContext(ctx).Infof("re-executing with new jobId, attempt %d/%d", attempt+1, maxReExecute)
				continue
			}
			logger.WithContext(ctx).Errorf("execInternal error: %v", err)
			return res, err
		}
		return res, nil
	}

	return nil, fmt.Errorf("exec failed after %d re-execute attempts", maxReExecute)
}

// error codes for retry logic, aligned with Java CZStatement
const (
	errorCodeNeedReExecute        = "CZLH-57015"
	errorCodeJobNotExist          = "CZLH-60005"
	errorCodeJobAlreadyExist      = "CZLH-60007"
	errorCodeRequestStatusUnknown = "CZLH-60022"
	errorCodeRequestNotSubmitted  = "CZLH-60023"
	defaultMaxRetries             = 10
)

// getMaxRetries returns the configured max retries or the default value
func (conn *ClickzettaConn) getMaxRetries() int {
	if conn.cfg.Params != nil {
		if v, ok := conn.cfg.Params["sdk.query.max.retries"]; ok && v != nil {
			if n, err := strconv.Atoi(*v); err == nil && n > 0 {
				return n
			}
		}
	}
	return defaultMaxRetries
}

// sleepWithBackoff sleeps for the given interval and returns the next interval (exponential backoff, capped at 3s)
func sleepWithBackoff(ctx context.Context, intervalMs int) int {
	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(intervalMs) * time.Millisecond):
	}
	next := intervalMs * 2
	if next > 3000 {
		next = 3000
	}
	return next
}

// getResponseErrorCode extracts the error code from a submit job response JSON
func getResponseErrorCode(jsonValue *fastjson.Value) string {
	if jsonValue == nil {
		return ""
	}
	// check status.errorCode
	if jsonValue.Exists("status") && jsonValue.Get("status").Exists("errorCode") {
		code := strings.ReplaceAll(jsonValue.Get("status").Get("errorCode").String(), "\"", "")
		if code != "" {
			return code
		}
	}
	// check respStatus.errorCode
	if jsonValue.Exists("respStatus") && jsonValue.Get("respStatus").Exists("errorCode") {
		code := strings.ReplaceAll(jsonValue.Get("respStatus").Get("errorCode").String(), "\"", "")
		if code != "" {
			return code
		}
	}
	return ""
}

func (conn *ClickzettaConn) execInternal(ctx context.Context, query string, id jobId, bindings []driver.NamedValue) (*execResponse, error) {
	logger.WithContext(ctx).Infof("execInternal: %v with jobid: %v", query, id.ID)
	finalResponse := &execResponse{}
	finalResponse.Data.JobId = id.ID
	finalResponse.Data.QuerySQL = query
	hints := make(map[string]interface{})
	hints["cz.sql.adhoc.result.type"] = "embedded"
	hints["cz.sql.adhoc.default.format"] = "arrow"
	hints["cz.sql.job.result.file.presigned.url.enabled"] = "true"
	hints["cz.sql.job.result.file.presigned.url.ttl"] = "3600"
	sdkJobTimeout := 0
	multiQueries := splitSQL(query)

	// set query as the last query in multiQueries
	query = multiQueries[len(multiQueries)-1]
	query = query + "\n;"

	for _, hint := range multiQueries[:len(multiQueries)-1] {
		hintKV := strings.Split(hint, "=")
		if len(hintKV) != 2 {
			logger.WithContext(conn.ctx).Errorf("invalid hint: %v", hint)
			return nil, driver.ErrSkip
		}
		if hintKV[0] == "sdk.job.timeout" {
			sdkJobTimeout, _ = strconv.Atoi(hintKV[1])
			continue
		}
		hints[hintKV[0]] = hintKV[1]
	}
	flags := GetDriverFlags(ctx)
	for k, v := range flags {
		switch k {
		case "workspace", "virtualCluster", "schema":
			// these are handled separately as structured request parameters
			continue
		default:
			hints[k] = v
		}
	}

	isSeprate := false
	if conn.cfg.Params != nil {
		if _, ok := conn.cfg.Params["separate_params"]; ok {
			isSeprate = *conn.cfg.Params["separate_params"] == "true"
		}
	}

	// use bindings to fill the ? placeholders in query
	if len(bindings) > 0 && (!isSeprate || !strings.HasPrefix(query, "INSERT")) {
		query, _ = replacePlaceholders(query, bindings)
	}

	// convert bindings to Arrow IPC binary data
	arrowBinary := [][]byte{}
	err := error(nil)
	if len(bindings) > 0 && isSeprate && strings.HasPrefix(query, "INSERT") {
		arrowBinary, err = convertBindingsToArrowBinary(bindings)
		if err != nil {
			logger.WithContext(conn.ctx).Errorf("failed to convert bindings to arrow format: %v", err)
			return nil, fmt.Errorf("failed to convert bindings to arrow format: %w", err)
		}
	}

	sqlConfig := sqlJobConfig{
		TimeOut:        int64(0),
		AdhocSizeLimit: "0",
		AdhocRowLimit:  "0",
		Hint:           hints,
	}

	schema := conn.cfg.Schema
	if v, ok := flags["schema"]; ok && v != "" {
		schema = v
	}
	sqls := append(make([]string, 0), query)
	catalog := conn.cfg.Workspace
	if v, ok := conn.cfg.Params["catalog"]; ok && v != nil {
		catalog = *v
	}
	if v, ok := flags["workspace"]; ok && v != "" {
		catalog = v
	}
	virtualCluster := conn.cfg.VirtualCluster
	if v, ok := flags["virtualCluster"]; ok && v != "" {
		virtualCluster = v
	}
	sqljob := sqlJob{
		Query: sqls,
		DefaultNamespace: []string{
			catalog,
			schema,
		},
		SQLJobConfig: &sqlConfig,
		BinaryValues: &BinaryValues{
			inputs: arrowBinary,
		},
	}

	jd := jobDesc{
		VirtualCluster:       virtualCluster,
		JobType:              SQL_JOB,
		JobId:                &id,
		JobName:              "SQL_JOB",
		UserId:               0,
		JobRequestMode:       HYBRID,
		HybirdPollingTimeout: 30,
		JobConfig:            map[string]interface{}{},
		SQLJob:               &sqljob,
		JobTimeout:           3000,
		UserAgent:            "goclickzetta",
		Priority:             0,
	}

	request := jobRequest{
		JobDesc: &jd,
	}

	jsonData, err := json.Marshal(request.properties())
	if err != nil {
		logger.Errorf("parse submit job request to json error: %v", err)
		return nil, err
	}

	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["instanceName"] = conn.cfg.Instance
	headers["X-ClickZetta-Token"] = conn.cfg.Token

	// submit job with retry logic (aligned with Java CZStatement.submitJob)
	maxRetries := conn.getMaxRetries()
	sleepIntervalMs := 500
	var responseJson *fastjson.Value
	var stream []byte
	var lastErr error

	for tried := 1; tried <= maxRetries; tried++ {
		responseJson, stream, err = GetHttpResponseMsgToJson(headers, string(SubmitJobRequestPath), conn, jsonData)
		if err != nil {
			lastErr = err
			logger.WithContext(ctx).Errorf("submitJob exception, jobid: %v, tried %d/%d, error: %v", id.ID, tried, maxRetries, err)
			sleepIntervalMs = sleepWithBackoff(ctx, sleepIntervalMs)
			continue
		}

		errorCode := getResponseErrorCode(responseJson)

		// CZLH-57015: need to regenerate jobId and re-execute (not just retry submit)
		if errorCode == errorCodeNeedReExecute {
			logger.WithContext(ctx).Infof("job %v got %s, need re-execute with new jobId", id.ID, errorCodeNeedReExecute)
			return finalResponse, &reExecuteError{jobID: id.ID, errorCode: errorCode}
		}

		// REQUEST_NOT_SUBMITTED: submit explicitly failed, retry
		if errorCode == errorCodeRequestNotSubmitted {
			logger.WithContext(ctx).Errorf("job submit not submitted, jobid: %v, tried %d/%d", id.ID, tried, maxRetries)
			sleepIntervalMs = sleepWithBackoff(ctx, sleepIntervalMs)
			continue
		}

		// JOB_ALREADY_EXIST: if first attempt, it's a real conflict; otherwise the previous submit actually went through
		if errorCode == errorCodeJobAlreadyExist {
			if tried == 1 {
				errMsg := fmt.Sprintf("job %s already exists, cannot resubmit", id.ID)
				logger.WithContext(ctx).Errorf(errMsg)
				finalResponse.Success = false
				finalResponse.Message = errMsg
				return finalResponse, errors.New(errMsg)
			}
			// previous submit succeeded, go directly to poll result
			finalResponse, err = conn.retryGetResult(ctx, id, headers, sdkJobTimeout, finalResponse, maxRetries)
			if err != nil {
				if isJobNotExistError(err) {
					logger.WithContext(ctx).Errorf("job %v not found during polling after JOB_ALREADY_EXIST, retrying submit (%d/%d)", id.ID, tried, maxRetries)
					sleepIntervalMs = sleepWithBackoff(ctx, sleepIntervalMs)
					continue
				}
				return finalResponse, err
			}
			return finalResponse, nil
		}

		// check if job finished immediately in submit response
		status := ""
		if responseJson.Exists("status") && responseJson.Get("status").Exists("state") {
			status = strings.ReplaceAll(responseJson.Get("status").Get("state").String(), "\"", "")
		}

		if status == "SUCCEED" || status == "FAILED" || status == "CANCELLED" {
			// job finished immediately
			return conn.handleFinishedJob(status, id, finalResponse, stream, responseJson)
		}

		if status == "QUEUEING" || status == "RUNNING" || status == "SETUP" {
			// job is running, poll for result
			finalResponse, err = conn.retryGetResult(ctx, id, headers, sdkJobTimeout, finalResponse, maxRetries)
			if err != nil {
				// JOB_NOT_EXIST during polling: retry submit
				if isJobNotExistError(err) {
					logger.WithContext(ctx).Errorf("job %v not found during polling, retrying submit (%d/%d)", id.ID, tried, maxRetries)
					sleepIntervalMs = sleepWithBackoff(ctx, sleepIntervalMs)
					continue
				}
				return finalResponse, err
			}
			return finalResponse, nil
		}

		// unknown status or no status, retry
		logger.WithContext(ctx).Errorf("unexpected submit response for jobid: %v, status: %v, tried %d/%d", id.ID, status, tried, maxRetries)
		sleepIntervalMs = sleepWithBackoff(ctx, sleepIntervalMs)
	}

	// all retries exhausted
	if lastErr != nil {
		finalResponse.Success = false
		finalResponse.Message = fmt.Sprintf("job %s failed after %d retries: %v", id.ID, maxRetries, lastErr)
		return finalResponse, lastErr
	}
	finalResponse.Success = false
	finalResponse.Message = fmt.Sprintf("job %s failed after %d retries", id.ID, maxRetries)
	return finalResponse, fmt.Errorf("job %s failed after %d retries", id.ID, maxRetries)
}

// reExecuteError signals that the job needs to be re-executed with a new jobId
type reExecuteError struct {
	jobID     string
	errorCode string
}

func (e *reExecuteError) Error() string {
	return fmt.Sprintf("job %s needs re-execute: %s", e.jobID, e.errorCode)
}

// jobNotExistError signals that the job was not found
type jobNotExistError struct {
	jobID string
}

func (e *jobNotExistError) Error() string {
	return fmt.Sprintf("job %s not found", e.jobID)
}

func isJobNotExistError(err error) bool {
	_, ok := err.(*jobNotExistError)
	return ok
}

// handleFinishedJob processes a job that finished immediately in the submit response
func (conn *ClickzettaConn) handleFinishedJob(status string, id jobId, finalResponse *execResponse, stream []byte, responseJson *fastjson.Value) (*execResponse, error) {
	logger.WithContext(conn.ctx).Infof("job finished immediately, jobid: %v, status: %v", id.ID, status)
	switch status {
	case "SUCCEED":
		finalResponse.Success = true
		finalResponse.Message = "job succeed, jobid: " + id.ID
		if err := json.Unmarshal(stream, &finalResponse.Data.HTTPResponseMessage); err != nil {
			logger.WithContext(conn.ctx).Errorf("parse job response error: %v", err)
			finalResponse.Success = false
			finalResponse.Message = err.Error()
			return finalResponse, err
		}
		return finalResponse, nil
	case "FAILED":
		finalResponse.Success = false
		finalResponse.Message = "job failed, jobid: " + id.ID + ", error: " + responseJson.String()
		return finalResponse, errors.New(finalResponse.Message)
	case "CANCELLED":
		finalResponse.Success = false
		finalResponse.Message = "job cancelled, jobid: " + id.ID
		return finalResponse, errors.New(finalResponse.Message)
	default:
		finalResponse.Success = false
		finalResponse.Message = "unexpected job status: " + status
		return finalResponse, errors.New(finalResponse.Message)
	}
}

// isGetJobResultFailed checks if the get job result response indicates a temporary failure that should be retried
// Aligned with Java CZStatement.isGetJobResultFailed
func isGetJobResultFailed(responseJson *fastjson.Value) bool {
	if responseJson == nil {
		return true
	}
	errorCode := getResponseErrorCode(responseJson)
	return errorCode == errorCodeRequestNotSubmitted || errorCode == errorCodeRequestStatusUnknown
}

// retryGetResult polls for job result with retry and exponential backoff (aligned with Java retryGetResult)
func (conn *ClickzettaConn) retryGetResult(ctx context.Context, id jobId, headers map[string]string, timeout int, finalResponse *execResponse, maxRetries int) (*execResponse, error) {
	account := clickzettaAccoount{UserId: 0}
	getJobReq := getJobResultRequest{
		Account:   &account,
		JobId:     &id,
		Offset:    0,
		UserAgent: "",
	}
	apiReq := apiGetJobRequest{
		GetJobResultReq: &getJobReq,
		UserAgent:       "",
	}
	jsonData, err := json.Marshal(apiReq.properties())
	if err != nil {
		finalResponse.Success = false
		finalResponse.Message = err.Error()
		return finalResponse, err
	}
	getJobURL, err := url.Parse(conn.cfg.Service + string(GetJobResultPath))
	if err != nil {
		finalResponse.Success = false
		finalResponse.Message = err.Error()
		return finalResponse, err
	}

	startTime := time.Now()
	pollCount := 0           // counts polling attempts (for logging)
	exceptionRetryCount := 0 // counts network/parsing errors
	pollIntervalMs := 50
	exceptionSleepMs := 50

	for {
		// check timeout
		if conn.checkJobTimeout(timeout, startTime, id, headers) {
			logger.WithContext(ctx).Errorf("job timeout, jobid: %v", id.ID)
			finalResponse.Success = false
			finalResponse.Message = "job timeout, jobid: " + id.ID
			return finalResponse, driverTimeoutError{"job timeout, jobid: " + id.ID}
		}

		pollCount++

		res, err := conn.internal.Post(ctx, getJobURL, headers, jsonData, 0)
		if err != nil {
			exceptionRetryCount++
			logger.WithContext(ctx).Errorf("get job result error, jobid: %v, exception retry %d/%d: %v", id.ID, exceptionRetryCount, maxRetries, err)
			if exceptionRetryCount > maxRetries {
				finalResponse.Success = false
				finalResponse.Message = err.Error()
				return finalResponse, err
			}
			exceptionSleepMs = sleepWithBackoff(ctx, exceptionSleepMs)
			continue
		}

		stream, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			exceptionRetryCount++
			logger.WithContext(ctx).Errorf("read get job response error, jobid: %v, exception retry %d/%d: %v", id.ID, exceptionRetryCount, maxRetries, err)
			if exceptionRetryCount > maxRetries {
				finalResponse.Success = false
				finalResponse.Message = err.Error()
				return finalResponse, err
			}
			exceptionSleepMs = sleepWithBackoff(ctx, exceptionSleepMs)
			continue
		}

		responseJson, err := fastjson.ParseBytes(stream)
		if err != nil {
			exceptionRetryCount++
			logger.WithContext(ctx).Errorf("parse get job response error, jobid: %v, exception retry %d/%d: %v", id.ID, exceptionRetryCount, maxRetries, err)
			if exceptionRetryCount > maxRetries {
				finalResponse.Success = false
				finalResponse.Message = err.Error()
				return finalResponse, err
			}
			exceptionSleepMs = sleepWithBackoff(ctx, exceptionSleepMs)
			continue
		}

		// check for JOB_NOT_EXIST error - throw immediately, don't retry
		errorCode := getResponseErrorCode(responseJson)
		if errorCode == errorCodeJobNotExist {
			logger.WithContext(ctx).Warnf("job %v not exists", id.ID)
			return finalResponse, &jobNotExistError{jobID: id.ID}
		}

		// check if response indicates temporary failure (REQUEST_NOT_SUBMITTED or REQUEST_STATUS_UNKNOWN)
		if isGetJobResultFailed(responseJson) {
			logger.WithContext(ctx).Infof("get job result failed (temporary), jobid: %v, errorCode: %v, poll attempt %d", id.ID, errorCode, pollCount)
			exceptionSleepMs = sleepWithBackoff(ctx, exceptionSleepMs)
			continue
		}

		if !responseJson.Exists("status") {
			logger.WithContext(ctx).Errorf("get job error, no status field: %v", string(stream))
			finalResponse.Success = false
			finalResponse.Message = "get job error: " + string(stream)
			return finalResponse, driver.ErrBadConn
		}

		if responseJson.Get("status").Exists("state") {
			status := strings.ReplaceAll(responseJson.Get("status").Get("state").String(), "\"", "")

			if status == "SUCCEED" {
				finalResponse.Success = true
				finalResponse.Message = "job succeed, jobid: " + id.ID
				if err := json.Unmarshal(stream, &finalResponse.Data.HTTPResponseMessage); err != nil {
					finalResponse.Success = false
					finalResponse.Message = err.Error()
					return finalResponse, err
				}
				return finalResponse, nil
			}

			if status == "FAILED" {
				finalResponse.Success = false
				finalResponse.Message = "job failed, jobid: " + id.ID + ", error: " + responseJson.String()
				return finalResponse, errors.New(finalResponse.Message)
			}

			if status == "CANCELLED" {
				finalResponse.Success = false
				finalResponse.Message = "job cancelled, jobid: " + id.ID
				return finalResponse, errors.New(finalResponse.Message)
			}

			// still running - reset exception retry count on successful poll
			exceptionRetryCount = 0
			elapsed := time.Since(startTime)
			logger.WithContext(ctx).Infof("job %v is running, status: %v, poll attempt %d, elapsed: %v", id.ID, status, pollCount, elapsed)
		}

		// sleep before next poll
		time.Sleep(time.Duration(pollIntervalMs) * time.Millisecond)
	}
}

func GetHttpResponseMsgToJson(headers map[string]string, path string, connection *ClickzettaConn, jsonData []byte) (*fastjson.Value, []byte, error) {
	url, err := url.Parse(connection.cfg.Service + path)
	if err != nil {
		logger.WithContext(connection.ctx).Errorf("GetHttpResponseMsgToJson error:%v", err)
		return nil, nil, err
	}
	res, err := connection.internal.Post(connection.ctx, url, headers, jsonData, 0)

	if err != nil {
		logger.WithContext(connection.ctx).Errorf("GetHttpResponseMsgToJson  error: %v", err)
		return nil, nil, err
	}

	defer res.Body.Close()
	stream, err := io.ReadAll(res.Body)

	if err != nil {
		logger.WithContext(connection.ctx).Errorf("GetHttpResponseMsgToJson error: %v", err)
		return nil, nil, err
	}
	responseJson, err := fastjson.ParseBytes(stream)
	if err != nil {
		logger.WithContext(connection.ctx).Errorf("GetHttpResponseMsgToJson error: %v", err)
		return nil, nil, err
	}
	return responseJson, stream, nil
}

func (conn *ClickzettaConn) checkJobTimeout(timeout int, startTime time.Time, id jobId, headers map[string]string) bool {
	if timeout > 0 {
		if time.Now().Sub(startTime).Seconds() > float64(timeout) {
			err := conn.cancelJob(id, headers)
			if err != nil {
				logger.WithContext(conn.ctx).Errorf("cancel job error: %v", err)
			}
			return true
		}
	}
	return false
}

func (conn *ClickzettaConn) cancelJob(id jobId, headers map[string]string) error {
	account := clickzettaAccoount{
		UserId: 0,
	}
	cancelJobRequest := cancelJobRequest{
		Account:   &account,
		JobId:     &id,
		UserAgent: "",
		Force:     false,
	}
	jsonValue, err := json.Marshal(cancelJobRequest.properties())
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("parse cancel job request to json error: %v", err)
		return err
	}
	url, err := url.Parse(conn.cfg.Service + string(CancelJobPath))
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("parses cancel job url path error: %v", err)
		return err
	}
	res, err := conn.internal.Post(conn.ctx, url, headers, jsonValue, 0)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("cancel job error: %v", err)
		return err
	}
	defer res.Body.Close()
	return nil
}

func formatJobId() string {
	return getRequestIDGenerator().generate()
}

func (conn *ClickzettaConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(conn.ctx, driver.TxOptions{})
}

func (conn *ClickzettaConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions) (
	driver.Tx, error) {
	logger.WithContext(ctx).Infof("BeginTx: %#v", opts)
	if conn.internal == nil {
		return nil, driver.ErrBadConn
	}
	return &clickzettaTx{conn, ctx}, nil
}

func (conn *ClickzettaConn) cleanup() {
	logger.WithContext(conn.ctx).Infof("cleanup")
	if err := conn.internal.Close(); err != nil {
		logger.WithContext(conn.ctx).Errorf("failed to close internal client: %v", err)
	}
	conn.cfg = nil
	conn.ctx = nil
}

func (conn *ClickzettaConn) Close() (err error) {
	logger.WithContext(conn.ctx).Infof("Close")
	defer conn.cleanup()
	return nil
}

// SetSchema sets the current schema for the connection
func (conn *ClickzettaConn) SetSchema(schema string) {
	conn.cfg.Schema = schema
}

// GetSchema returns the current schema of the connection
func (conn *ClickzettaConn) GetSchema() string {
	return conn.cfg.Schema
}

// SetCatalog sets the current catalog for the connection
func (conn *ClickzettaConn) SetCatalog(catalog string) {
	if conn.cfg.Params == nil {
		conn.cfg.Params = make(map[string]*string)
	}
	conn.cfg.Params["catalog"] = &catalog
}

// GetCatalog returns the current catalog of the connection
func (conn *ClickzettaConn) GetCatalog() string {
	if conn.cfg.Params == nil {
		return conn.cfg.Workspace
	}
	if v, ok := conn.cfg.Params["catalog"]; ok && v != nil {
		return *v
	}
	return conn.cfg.Workspace
}

func (conn *ClickzettaConn) PrepareContext(
	ctx context.Context,
	query string) (
	driver.Stmt, error) {
	logger.WithContext(ctx).Infof("PrepareContext: %#v", query)
	if conn.internal == nil {
		return nil, driver.ErrBadConn
	}
	stmt := &ClickzettaStmt{
		conn:  conn,
		query: query,
	}
	return stmt, nil
}

func (std *ClickzettaConn) CheckNamedValue(nv *driver.NamedValue) error { return nil }

var _ driver.NamedValueChecker = (*ClickzettaConn)(nil)

func (conn *ClickzettaConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Result, error) {
	logger.WithContext(ctx).Infof("ExecContext: %#v, %v", query, args)
	if conn.internal == nil {
		return nil, driver.ErrBadConn
	}
	data, err := conn.exec(ctx, query, true, false, false, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("exec sql: %v error: %v", query, err)
		result := &clickzettaResult{
			queryID: data.Data.JobId,
			status:  queryStatus(QueryFailed),
			err:     err,
		}
		return result, err
	}
	result := &clickzettaResult{
		queryID: data.Data.JobId,
	}
	if data.Success {
		result.status = queryStatus(QueryStatusComplete)
		result.err = nil
	} else {
		result.status = queryStatus(QueryFailed)
		result.err = errors.New(data.Message)
	}

	return result, nil
}

func (conn *ClickzettaConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Rows, error) {
	logger.WithContext(ctx).Infof("QueryContext: %#v, %v", query, args)
	if conn.internal == nil {
		return nil, driver.ErrBadConn
	}
	data, err := conn.exec(ctx, query, false, false, false, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("exec sql error: %v", err)
		return nil, err
	}
	status := queryStatus("")
	if data.Success {
		status = QueryStatusComplete
	} else {
		return nil, &ClickzettaError{
			Message:  data.Message,
			SQLState: "FAILED",
			QueryID:  data.Data.JobId,
		}
	}
	rows := &clickzettaRows{
		queryID:  data.Data.JobId,
		status:   status,
		err:      nil,
		cn:       conn,
		response: data,
	}
	err = rows.response.Data.init()
	if err != nil {
		logger.WithContext(ctx).Errorf("init response data struct error: %v", err)
		return nil, err
	}

	return rows, nil
}

func (conn *ClickzettaConn) QueryArrowStream(ctx context.Context, query string, args []driver.NamedValue) (array.RecordReader, error) {
	logger.WithContext(ctx).Infof("QueryContext: %#v, %v", query, args)
	if conn.internal == nil {
		return nil, driver.ErrBadConn
	}
	data, err := conn.exec(ctx, query, false, false, false, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("exec sql error: %v", err)
		return nil, err
	}
	if !data.Success {
		return nil, &ClickzettaError{
			Message:  data.Message,
			SQLState: "FAILED",
			QueryID:  data.Data.JobId,
		}
	}

	err = data.Data.init()
	if err != nil {
		logger.WithContext(ctx).Errorf("init response data error: %v", err)
		return nil, err
	}

	// Use LazyStreamingReader for File type
	if data.Data.DataType == File {
		urls := data.Data.GetFileURLs()
		if len(urls) == 0 {
			// Return empty reader instead of error for empty result set
			return NewEmptyRecordReaderFromFields(data.Data.Schema), nil
		}
		lazyReader, err := NewLazyStreamingReader(urls)
		if err != nil {
			logger.WithContext(ctx).Errorf("create lazy streaming reader error: %v", err)
			return nil, err
		}
		return lazyReader, nil
	}

	// Use LazyMemoryReader for Memory type
	chunks := data.Data.GetMemoryDataChunks()
	if len(chunks) == 0 {
		// Return empty reader instead of error for empty result set
		return NewEmptyRecordReaderFromFields(data.Data.Schema), nil
	}
	memoryReader, err := NewLazyMemoryReader(chunks)
	if err != nil {
		logger.WithContext(ctx).Errorf("create lazy memory reader error: %v", err)
		return nil, err
	}

	return memoryReader, nil
}

func (conn *ClickzettaConn) queryContextInternal(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Rows, error) {
	logger.WithContext(ctx).Infof("queryContextInternal: %#v, %v", query, args)
	return nil, nil
}

func (conn *ClickzettaConn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(conn.ctx, query)
}

func (conn *ClickzettaConn) Exec(
	query string,
	args []driver.Value) (
	driver.Result, error) {
	return conn.ExecContext(conn.ctx, query, toNamedValues(args))
}

func (conn *ClickzettaConn) Query(
	query string,
	args []driver.Value) (
	driver.Rows, error) {
	return conn.QueryContext(conn.ctx, query, toNamedValues(args))
}

func (conn *ClickzettaConn) Ping(ctx context.Context) error {
	logger.WithContext(ctx).Infoln("Ping")
	_, err := conn.exec(ctx, "select 1", false, false, false, nil)
	return err
}

func (conn *ClickzettaConn) CreateBulkloadStream(option BulkloadOptions) (*BulkloadStream, error) {
	if option.Operation == "" {
		option.Operation = APPEND
	}
	createBulkloadStreamReq := ingestion.CreateBulkLoadStreamRequest{}
	account := ingestion.Account{}
	userIdent := ingestion.UserIdentifier{}
	userIdent.InstanceId = 0
	userIdent.Workspace = conn.cfg.Workspace
	userIdent.UserName = conn.cfg.UserName
	account.UserIdent = &userIdent
	account.Token = conn.cfg.Token
	createBulkloadStreamReq.Account = &account
	tableIdentifier := ingestion.TableIdentifier{}
	tableIdentifier.SchemaName = conn.cfg.Schema
	tableIdentifier.TableName = option.Table
	tableIdentifier.Workspace = conn.cfg.Workspace
	tableIdentifier.InstanceId = 0
	createBulkloadStreamReq.Identifier = &tableIdentifier
	// Bulkload v2 always creates APPEND stream; the actual operation
	// (APPEND/UPSERT/OVERWRITE) is handled at commit time via SQL.
	createBulkloadStreamReq.Operation = ingestion.BulkLoadStreamOperation_BL_APPEND
	if option.PartitionSpec != "" {
		createBulkloadStreamReq.PartitionSpec = option.PartitionSpec
	}
	if option.RecordKeys != nil {
		createBulkloadStreamReq.RecordKeys = option.RecordKeys
	}
	if option.PreferInternalEndpoint {
		createBulkloadStreamReq.PreferInternalEndpoint = true
	}
	gateWayRes, err := conn.GateWayCall(&createBulkloadStreamReq, ingestion.MethodEnum_CREATE_BULK_LOAD_STREAM_V2)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("create bulkload stream error: %v", err)
		return nil, err
	}
	val, err := gateWayRes.StringBytes()
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("create bulkload stream error: %v", err)
		return nil, err
	}

	reader := bytes.NewReader(val)
	createBLSRes := ingestion.CreateBulkLoadStreamResponse{}
	err = jsonpb.Unmarshal(reader, &createBLSRes)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("create bulkload stream error: %v", err)
		return nil, err
	}
	if createBLSRes.GetStatus().GetCode() != 0 {
		logger.WithContext(conn.ctx).Errorf("create bulkload stream error: %v", gateWayRes.String())
		return nil, errors.New(gateWayRes.String())
	}
	conn.cfg.InstanceId = createBLSRes.GetInfo().GetIdentifier().GetInstanceId()
	BMD := BulkloadMetadata{}
	BMD.InstanceId = createBLSRes.GetInfo().GetIdentifier().GetInstanceId()
	BMD.StreamInfo = createBLSRes.GetInfo()
	table := CZTable{
		TableName:  option.Table,
		SchemaName: conn.cfg.Schema,
		TableMeta:  createBLSRes.GetInfo().GetStreamSchema(),
		Schema:     make(map[string]*util.DataType),
	}
	for _, field := range table.TableMeta.GetDataFields() {
		table.Schema[field.GetName()] = field.GetType()
	}
	BMD.Table = table
	commitOpt := BulkloadCommitOptions{}
	commitOpt.Workspace = conn.cfg.Workspace
	commitOpt.VirtualCluster = conn.cfg.VirtualCluster

	bulkloadStream := BulkloadStream{}
	bulkloadStream.MetaData = &BMD
	bulkloadStream.Connection = conn
	bulkloadStream.CommitOptions = &commitOpt
	bulkloadStream.Closed = false
	bulkloadStream.StreamOptions = &option

	return &bulkloadStream, nil
}

func (conn *ClickzettaConn) GateWayCall(message proto.Message, method ingestion.MethodEnum) (*fastjson.Value, error) {
	getwayReq := ingestion.GatewayRequest{}
	getwayReq.MethodEnumValue = int32(method)
	marshaler := jsonpb.Marshaler{OrigName: true}
	jsonValue, err := marshaler.MarshalToString(message)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("json format message error: %v", err)
		return nil, err
	}
	getwayReq.Message = jsonValue
	getwayReqMes, err := marshaler.MarshalToString(&getwayReq)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("json format gateway request error: %v", err)
		return nil, err
	}
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["instanceName"] = conn.cfg.Instance
	headers["X-ClickZetta-Token"] = conn.cfg.Token
	jsonResponse, _, err := GetHttpResponseMsgToJson(headers, string(GETWAYPATH), conn, []byte(getwayReqMes))
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("gateway call error: %v", err)
		return nil, err
	}
	if !jsonResponse.Exists("status") {
		logger.WithContext(conn.ctx).Errorf("gateway call error: %v", jsonResponse.String())
		return nil, err
	}
	gateWayStatus := ingestion.GateWayResponseStatus{}
	reader := strings.NewReader(jsonResponse.Get("status").String())
	err = jsonpb.Unmarshal(reader, &gateWayStatus)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("gateway call error: %v", err)
		return nil, err
	}
	if gateWayStatus.Code == ingestion.Code_SUCCESS {
		return jsonResponse.Get("message"), nil
	} else {
		logger.WithContext(conn.ctx).Errorf("gateway call error: %v", jsonResponse.String())
		return nil, errors.New("gateway call error: " + jsonResponse.String())
	}

}

func connect(dsn string) (*ClickzettaConn, error) {
	ctx := context.TODO()
	config, err := ParseDSN(dsn)
	if err != nil {
		logger.Errorf("Parse DSN string error:%v", err)
		return nil, fmt.Errorf("parse DSN string error:%v", err.Error())
	}
	conn, err := buildClickzettaConn(ctx, *config)
	if err != nil {
		logger.WithContext(ctx).Errorf("build clickzetta conn error: %v", err)
		return nil, err
	}
	return conn, nil
}

func buildClickzettaConn(ctx context.Context, config Config) (*ClickzettaConn, error) {
	conn := &ClickzettaConn{
		ctx: ctx,
		cfg: &config,
	}

	transport := newHTTPTransport()
	cli := &httpClient{
		transport: transport,
	}

	cli.client = &http.Client{Transport: transport}
	conn.internal = cli

	loginPrarams := make(map[string]string)
	loginPrarams["username"] = config.UserName
	loginPrarams["password"] = config.Password
	loginPrarams["instanceName"] = config.Instance

	url, _ := url.Parse(config.Service + string(GetTokenPath))
	jsonValue, err := json.Marshal(loginPrarams)
	if err != nil {
		logger.WithContext(ctx).Errorf("json format login parmams error: %v", err)
		return nil, err
	}
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"

	res, err := conn.internal.Post(ctx, url, headers, jsonValue, 0)
	if err != nil {
		logger.WithContext(ctx).Errorf("login error: %v", err)
		return nil, err
	}

	defer res.Body.Close()
	stream, err := io.ReadAll(res.Body)
	if err != nil {
		logger.WithContext(ctx).Errorf("read login response error: %v", err)
		return nil, err
	}
	body, err := fastjson.ParseBytes(stream)
	if err != nil {
		logger.WithContext(ctx).Errorf("parse login response to json error: %v", err)
		return nil, err
	}
	if body.Exists("data") {
		if body.Get("data").Exists("token") {
			conn.cfg.Token = strings.ReplaceAll(body.Get("data").Get("token").String(), "\"", "")
		}
	} else {
		logger.WithContext(ctx).Errorf("login error: %v", string(stream))
		return nil, err
	}

	return conn, nil

}
