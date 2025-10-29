package goclickzetta

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

var HTTPTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second, // 连接超时时间
		KeepAlive: 60 * time.Second, // 保持长连接的时间
	}).DialContext, // 设置连接的参数
	MaxIdleConns:          500,              // 最大空闲连接
	IdleConnTimeout:       60 * time.Second, // 空闲连接的超时时间
	ExpectContinueTimeout: 30 * time.Second, // 等待服务第一个响应的超时时间
	MaxIdleConnsPerHost:   100,              // 每个host保持的空闲连接数
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
	id := formatJobId()
	logger.WithContext(ctx).Infof("jobId: %v", id)
	jobId := jobId{
		ID:         id,
		Workspace:  conn.cfg.Workspace,
		InstanceId: 0,
	}

	res, err := conn.execInternal(query, jobId, bindings)
	if err != nil {
		logger.WithContext(ctx).Errorf("execInternal error: %v", err)
		return res, err
	}

	return res, nil
}

func (conn *ClickzettaConn) execInternal(query string, id jobId, bindings []driver.NamedValue) (*execResponse, error) {
	logger.WithContext(conn.ctx).Infof("execInternal: %v with jobid: %v", query, id.ID)
	finalResponse := &execResponse{}
	finalResponse.Data.JobId = id.ID
	finalResponse.Data.QuerySQL = query
	hints := make(map[string]interface{})
	hints["cz.sql.adhoc.result.type"] = "embedded"
	// allow selecting result format via DSN param: resultFormat=arrow|text
	format := "csv"
	if conn.cfg != nil && conn.cfg.Params != nil {
		if v, ok := conn.cfg.Params["resultFormat"]; ok && v != nil && *v != "" {
			format = *v
		}
	}
	hints["cz.sql.adhoc.default.format"] = format
	hints["cz.storage.csv.asjdbc"] = "false"
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

	// 用binding来填充query中的？占位符
	if len(bindings) > 0 {
		query, _ = replacePlaceholders(query, bindings)
	}

	sqlConfig := sqlJobConfig{
		TimeOut:        int64(0),
		AdhocSizeLimit: "0",
		AdhocRowLimit:  "0",
		Hint:           hints,
	}

	schema := conn.cfg.Schema
	sqls := append(make([]string, 0), query)
	sqljob := sqlJob{
		Query: sqls,
		DefaultNamespace: []string{
			conn.cfg.Workspace,
			schema,
		},
		SQLJobConfig: &sqlConfig,
	}

	jobDesc := jobDesc{
		VirtualCluster:       conn.cfg.VirtualCluster,
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
		JobDesc: &jobDesc,
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
	responseJson, stream, err := GetHttpResponseMsgToJson(headers, string(SubmitJobRequestPath), conn, jsonData)

	finalResponse, err = conn.waitJobFinished(responseJson, id, headers, sdkJobTimeout, finalResponse, stream)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("wait job finished error: %v", err)
		return finalResponse, err
	}

	return finalResponse, nil

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

func (conn *ClickzettaConn) waitJobFinished(jsonValue *fastjson.Value, id jobId, headers map[string]string, timeout int, finalResponse *execResponse, original []byte) (*execResponse, error) {
	if jsonValue.Exists("status") {
		if jsonValue.Get("status").Exists("state") {
			status := jsonValue.Get("status").Get("state").String()
			status = strings.ReplaceAll(status, "\"", "")
			data := jsonValue.Get("result_set").Get("data")
			location := jsonValue.Get("result_set").Get("location")
			if (status == "QUEUEING" || status == "RUNNING" || status == "SETUP") && (data == nil && location == nil) {
				account := clickzettaAccoount{
					UserId: 0,
				}
				getJobRequest := getJobResultRequest{
					Account:   &account,
					JobId:     &id,
					Offset:    0,
					UserAgent: "",
				}
				apiReq := apiGetJobRequest{
					GetJobResultReq: &getJobRequest,
					UserAgent:       "",
				}
				jsonData, err := json.Marshal(apiReq.properties())
				if err != nil {
					logger.WithContext(conn.ctx).Errorf("parse get job request to json error: %v", err)
					finalResponse.Success = false
					finalResponse.Message = err.Error()
					return finalResponse, err
				}
				url, err := url.Parse(conn.cfg.Service + string(GetJobResultPath))
				if err != nil {
					logger.WithContext(conn.ctx).Errorf("parses get job url path error: %v", err)
					finalResponse.Success = false
					finalResponse.Message = err.Error()
					return finalResponse, err
				}
				startTime := time.Now()
				for {
					if conn.checkJobTimeout(timeout, startTime, id, headers) {
						logger.WithContext(conn.ctx).Errorf("job timeout, jobid: %v", id.ID)
						finalResponse.Success = false
						finalResponse.Message = "job timeout, jobid: " + id.ID
						return finalResponse, driverTimeoutError{"job timeout, jobid: " + id.ID}
					}
					res, err := conn.internal.Post(conn.ctx, url, headers, jsonData, 0)
					if err != nil {
						logger.WithContext(conn.ctx).Errorf("get job error: %v", err)
						finalResponse.Success = false
						finalResponse.Message = err.Error()
						return finalResponse, err
					}
					defer res.Body.Close()
					stream, err := io.ReadAll(res.Body)
					if err != nil {
						logger.WithContext(conn.ctx).Errorf("read get job response error: %v", err)
						finalResponse.Success = false
						finalResponse.Message = err.Error()
						return finalResponse, err
					}

					responseJson, err := fastjson.ParseBytes(stream)
					if err != nil {
						logger.WithContext(conn.ctx).Errorf("parse get job response to json error: %v", err)
						finalResponse.Success = false
						finalResponse.Message = err.Error()
						return finalResponse, err
					}
					if responseJson.Exists("status") {
						if responseJson.Get("status").Exists("state") {
							status := responseJson.Get("status").Get("state").String()
							status = strings.ReplaceAll(status, "\"", "")
							if status == "SUCCEED" || status == "FAILED" {
								logger.WithContext(conn.ctx).Infof("job finished, jobid: %v", id.ID)
								if status == "SUCCEED" {
									finalResponse.Success = true
									finalResponse.Message = "job succeed, jobid: " + id.ID
									err := json.Unmarshal(stream, &finalResponse.Data.HTTPResponseMessage)
									if err != nil {
										logger.WithContext(conn.ctx).Errorf("parse get job response to json error: %v", err)
										finalResponse.Success = false
										finalResponse.Message = err.Error()
										return finalResponse, err
									}
									return finalResponse, nil
								} else {
									finalResponse.Success = false
									finalResponse.Message = "job failed, jobid: " + id.ID + ", error: " + responseJson.String()
									return finalResponse, nil
								}
							} else {
								logger.WithContext(conn.ctx).Infof("waiting job finished, job status: %v, jobid: %v", status, id.ID)
								time.Sleep(2 * time.Second)
							}
						}
					} else {
						logger.WithContext(conn.ctx).Errorf("get job error: %v", string(stream))
						finalResponse.Success = false
						finalResponse.Message = "get job error: " + string(stream)
						return finalResponse, driver.ErrBadConn
					}
				}

			} else if status == "SUCCEED" || status == "FAILED" || status == "CANCELLED" {
				logger.WithContext(conn.ctx).Infof("job finished, jobid: %v", id.ID)
				if status == "SUCCEED" {
					finalResponse.Success = true
					finalResponse.Message = "job succeed, jobid: " + id.ID
					err := json.Unmarshal(original, &finalResponse.Data.HTTPResponseMessage)
					if err != nil {
						logger.WithContext(conn.ctx).Errorf("parse get job response to json error: %v", err)
						finalResponse.Success = false
						finalResponse.Message = err.Error()
						return finalResponse, err
					}
					return finalResponse, nil
				} else if status == "FAILED" {
					finalResponse.Success = false
					finalResponse.Message = "job failed, jobid: " + id.ID + ", error: " + jsonValue.String()
					return finalResponse, errors.New(finalResponse.Message)
				} else if status == "CANCELLED" {
					finalResponse.Success = false
					finalResponse.Message = "job cancelled, jobid: " + id.ID
					return finalResponse, errors.New(finalResponse.Message)
				}
			}

		}
	} else {
		logger.WithContext(conn.ctx).Errorf("get job error: %v", string(original))
		finalResponse.Success = false
		finalResponse.Message = "get job error: " + string(original)
		return finalResponse, driver.ErrBadConn
	}
	return nil, nil
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
	formatTime := time.Now().Format("20060102150405")
	formatTime = formatTime + strconv.Itoa(rand.Intn(100000))
	return formatTime
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
	conn.cfg = nil
	conn.ctx = nil
	if err := conn.internal.Close(); err != nil {
		logger.WithContext(conn.ctx).Errorf("failed to close internal client: %v", err)
	}
}

func (conn *ClickzettaConn) Close() (err error) {
	logger.WithContext(conn.ctx).Infof("Close")
	defer conn.cleanup()
	return nil
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
	if option.Operation == APPEND {
		createBulkloadStreamReq.Operation = ingestion.BulkLoadStreamOperation_BL_APPEND
	} else if option.Operation == UPSERT {
		createBulkloadStreamReq.Operation = ingestion.BulkLoadStreamOperation_BL_UPSERT
	} else {
		createBulkloadStreamReq.Operation = ingestion.BulkLoadStreamOperation_BL_OVERWRITE
	}
	if option.PartitionSpec != "" {
		createBulkloadStreamReq.PartitionSpec = option.PartitionSpec
	}
	if option.RecordKeys != nil {
		createBulkloadStreamReq.RecordKeys = option.RecordKeys
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

func (conn *ClickzettaConn) CommitBulkloadStream(streamId string, commitMode BulkLoadCommitMode, option BulkloadOptions) (*BulkloadMetadata, error) {
	commitBulkloadReq := ingestion.CommitBulkLoadStreamRequest{}
	account := ingestion.Account{}
	userIdent := ingestion.UserIdentifier{}
	userIdent.InstanceId = conn.cfg.InstanceId
	userIdent.Workspace = conn.cfg.Workspace
	userIdent.UserName = conn.cfg.UserName
	account.UserIdent = &userIdent
	account.Token = conn.cfg.Token
	commitBulkloadReq.Account = &account
	tableIdentifier := ingestion.TableIdentifier{}
	tableIdentifier.SchemaName = conn.cfg.Schema
	tableIdentifier.TableName = option.Table
	tableIdentifier.Workspace = conn.cfg.Workspace
	tableIdentifier.InstanceId = conn.cfg.InstanceId
	commitBulkloadReq.Identifier = &tableIdentifier
	commitBulkloadReq.StreamId = streamId
	commitBulkloadReq.ExecuteWorkspace = conn.cfg.Workspace
	commitBulkloadReq.ExecuteVcName = conn.cfg.VirtualCluster
	if commitMode == COMMIT_STREAM {
		commitBulkloadReq.CommitMode = ingestion.CommitBulkLoadStreamRequest_COMMIT_STREAM
	} else {
		commitBulkloadReq.CommitMode = ingestion.CommitBulkLoadStreamRequest_ABORT_STREAM
	}

	gateWayRes, err := conn.GateWayCall(&commitBulkloadReq, ingestion.MethodEnum_COMMIT_BULK_LOAD_STREAM_V2)

	if err != nil {
		logger.WithContext(conn.ctx).Errorf("commit bulkload stream error: %v", err)
		return nil, err
	}
	bye, err := gateWayRes.StringBytes()
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("commit bulkload stream error: %v", err)
		return nil, err
	}
	reader := bytes.NewReader(bye)
	commitBLSRes := ingestion.CommitBulkLoadStreamResponse{}
	err = jsonpb.Unmarshal(reader, &commitBLSRes)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("commit bulkload stream error: %v", err)
		return nil, err
	}
	if commitBLSRes.GetStatus().GetCode() != 0 {
		logger.WithContext(conn.ctx).Errorf("commit bulkload stream error: %v", gateWayRes.String())
		return nil, errors.New(gateWayRes.String())
	}
	BMD := BulkloadMetadata{}
	BMD.InstanceId = commitBLSRes.GetInfo().GetIdentifier().GetInstanceId()
	BMD.StreamInfo = commitBLSRes.GetInfo()
	return &BMD, nil
}

func (conn *ClickzettaConn) GetBulkloadStream(streamId string, option BulkloadOptions) (*BulkloadMetadata, error) {
	getBulkloadStreamReq := ingestion.GetBulkLoadStreamRequest{}
	account := ingestion.Account{}
	userIdent := ingestion.UserIdentifier{}
	userIdent.InstanceId = conn.cfg.InstanceId
	userIdent.Workspace = conn.cfg.Workspace
	userIdent.UserName = conn.cfg.UserName
	account.UserIdent = &userIdent
	account.Token = conn.cfg.Token
	getBulkloadStreamReq.Account = &account
	tableIdentifier := ingestion.TableIdentifier{}
	tableIdentifier.SchemaName = conn.cfg.Schema
	tableIdentifier.TableName = option.Table
	tableIdentifier.Workspace = conn.cfg.Workspace
	tableIdentifier.InstanceId = conn.cfg.InstanceId
	getBulkloadStreamReq.Identifier = &tableIdentifier
	getBulkloadStreamReq.StreamId = streamId
	getBulkloadStreamReq.NeedTableMeta = true
	jsonRes, err := conn.GateWayCall(&getBulkloadStreamReq, ingestion.MethodEnum_GET_BULK_LOAD_STREAM_V2)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", err)
		return nil, err
	}
	bye, err := jsonRes.StringBytes()
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", err)
		return nil, err
	}
	reader := bytes.NewReader(bye)
	getBLSRes := ingestion.GetBulkLoadStreamResponse{}
	err = jsonpb.Unmarshal(reader, &getBLSRes)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", err)
		return nil, err
	}
	if getBLSRes.GetStatus().GetCode() != 0 {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", jsonRes.String())
		return nil, errors.New(jsonRes.String())
	}
	BMD := BulkloadMetadata{}
	BMD.InstanceId = getBLSRes.GetInfo().GetIdentifier().GetInstanceId()
	BMD.StreamInfo = getBLSRes.GetInfo()

	return &BMD, nil
}

func (conn *ClickzettaConn) GetDistributeBulkloadStream(streamId string, option BulkloadOptions) (*BulkloadStream, error) {
	getBulkloadStreamReq := ingestion.GetBulkLoadStreamRequest{}
	account := ingestion.Account{}
	userIdent := ingestion.UserIdentifier{}
	userIdent.InstanceId = conn.cfg.InstanceId
	userIdent.Workspace = conn.cfg.Workspace
	userIdent.UserName = conn.cfg.UserName
	account.UserIdent = &userIdent
	account.Token = conn.cfg.Token
	getBulkloadStreamReq.Account = &account
	tableIdentifier := ingestion.TableIdentifier{}
	tableIdentifier.SchemaName = conn.cfg.Schema
	tableIdentifier.TableName = option.Table
	tableIdentifier.Workspace = conn.cfg.Workspace
	tableIdentifier.InstanceId = conn.cfg.InstanceId
	getBulkloadStreamReq.Identifier = &tableIdentifier
	getBulkloadStreamReq.StreamId = streamId
	getBulkloadStreamReq.NeedTableMeta = true
	jsonRes, err := conn.GateWayCall(&getBulkloadStreamReq, ingestion.MethodEnum_GET_BULK_LOAD_STREAM_V2)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", err)
		return nil, err
	}
	bye, err := jsonRes.StringBytes()
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", err)
		return nil, err
	}
	reader := bytes.NewReader(bye)
	getBLSRes := ingestion.GetBulkLoadStreamResponse{}
	err = jsonpb.Unmarshal(reader, &getBLSRes)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", err)
		return nil, err
	}
	if getBLSRes.GetStatus().GetCode() != 0 {
		logger.WithContext(conn.ctx).Errorf("get bulkload stream error: %v", jsonRes.String())
		return nil, errors.New(jsonRes.String())
	}
	BMD := BulkloadMetadata{}
	BMD.InstanceId = getBLSRes.GetInfo().GetIdentifier().GetInstanceId()
	BMD.StreamInfo = getBLSRes.GetInfo()
	table := CZTable{
		TableName:  option.Table,
		SchemaName: conn.cfg.Schema,
		TableMeta:  getBLSRes.GetInfo().GetStreamSchema(),
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

func (conn *ClickzettaConn) OpenBulkloadStreamWriter(streamId string, option BulkloadOptions, partitionId uint32) (*BulkLoadConfig, error) {
	openBulkloadStreamWriterReq := ingestion.OpenBulkLoadStreamWriterRequest{}
	account := ingestion.Account{}
	userIdent := ingestion.UserIdentifier{}
	userIdent.InstanceId = conn.cfg.InstanceId
	userIdent.Workspace = conn.cfg.Workspace
	userIdent.UserName = conn.cfg.UserName
	account.UserIdent = &userIdent
	account.Token = conn.cfg.Token
	openBulkloadStreamWriterReq.Account = &account
	tableIdentifier := ingestion.TableIdentifier{}
	tableIdentifier.SchemaName = conn.cfg.Schema
	tableIdentifier.TableName = option.Table
	tableIdentifier.Workspace = conn.cfg.Workspace
	tableIdentifier.InstanceId = conn.cfg.InstanceId
	openBulkloadStreamWriterReq.Identifier = &tableIdentifier
	openBulkloadStreamWriterReq.StreamId = streamId
	openBulkloadStreamWriterReq.PartitionId = partitionId
	jsonRes, err := conn.GateWayCall(&openBulkloadStreamWriterReq, ingestion.MethodEnum_OPEN_BULK_LOAD_STREAM_WRITER_V2)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("open bulkload stream writer error: %v", err)
		return nil, err
	}
	bye, err := jsonRes.StringBytes()
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("open bulkload stream writer error: %v", err)
		return nil, err
	}
	reader := bytes.NewReader(bye)
	openBLSWRes := ingestion.OpenBulkLoadStreamWriterResponse{}
	err = jsonpb.Unmarshal(reader, &openBLSWRes)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("open bulkload stream writer error: %v", err)
		return nil, err
	}
	if openBLSWRes.GetStatus().GetCode() != 0 {
		logger.WithContext(conn.ctx).Errorf("open bulkload stream writer error: %v", jsonRes.String())
		return nil, errors.New(jsonRes.String())
	}

	return &BulkLoadConfig{
		BLConfig: openBLSWRes.GetConfig(),
	}, nil
}

func (conn *ClickzettaConn) FinishBulkloadStreamWriter(streamId string, option BulkloadOptions, partitionId uint32, writtenFileList []string, writtenLengths []uint64) (*ingestion.ResponseStatus, error) {
	finishBulkloadStreamReq := ingestion.FinishBulkLoadStreamWriterRequest{}
	account := ingestion.Account{}
	userIdent := ingestion.UserIdentifier{}
	userIdent.InstanceId = conn.cfg.InstanceId
	userIdent.Workspace = conn.cfg.Workspace
	userIdent.UserName = conn.cfg.UserName
	account.UserIdent = &userIdent
	account.Token = conn.cfg.Token
	finishBulkloadStreamReq.Account = &account
	tableIdentifier := ingestion.TableIdentifier{}
	tableIdentifier.SchemaName = conn.cfg.Schema
	tableIdentifier.TableName = option.Table
	tableIdentifier.Workspace = conn.cfg.Workspace
	tableIdentifier.InstanceId = conn.cfg.InstanceId
	finishBulkloadStreamReq.Identifier = &tableIdentifier
	finishBulkloadStreamReq.StreamId = streamId
	finishBulkloadStreamReq.PartitionId = partitionId
	finishBulkloadStreamReq.WrittenFiles = writtenFileList
	finishBulkloadStreamReq.WrittenLengths = writtenLengths
	jsonRes, err := conn.GateWayCall(&finishBulkloadStreamReq, ingestion.MethodEnum_FINISH_BULK_LOAD_STREAM_WRITER_V2)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("finish bulkload stream writer error: %v", err)
		return nil, err
	}
	bye, err := jsonRes.StringBytes()
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("finish bulkload stream writer error: %v", err)
		return nil, err
	}

	reader := bytes.NewReader(bye)
	finishBLSWRes := ingestion.FinishBulkLoadStreamWriterResponse{}
	err = jsonpb.Unmarshal(reader, &finishBLSWRes)
	if err != nil {
		logger.WithContext(conn.ctx).Errorf("finish bulkload stream writer error: %v", err)
		return nil, err
	}
	return finishBLSWRes.GetStatus(), nil
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

	cli := &httpClient{
		transport: HTTPTransport,
	}

	cli.client = &http.Client{Transport: HTTPTransport}
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
