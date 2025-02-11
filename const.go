package goclickzetta

type jobType string

var (
	SQL_JOB        jobType = "SQL_JOB"
	COMPACTION_JOB jobType = "COMPACTION_JOB"
)

type jobRequestMode string

var (
	UNKNOWN jobRequestMode = "UNKNOWN"
	HYBRID  jobRequestMode = "HYBRID"
	ASYNC   jobRequestMode = "ASYNC"
	SYNC    jobRequestMode = "SYNC"
)

type sqlJobConfig struct {
	TimeOut        int64
	AdhocSizeLimit string
	AdhocRowLimit  string
	Hint           map[string]interface{}
}

func (cfg *sqlJobConfig) properties() map[string]interface{} {
	return map[string]interface{}{
		"timeout":        cfg.TimeOut,
		"adhocSizeLimit": cfg.AdhocSizeLimit,
		"adhocRowLimit":  cfg.AdhocRowLimit,
		"hint":           cfg.Hint,
	}
}

type sqlJob struct {
	Query            []string
	DefaultNamespace []string
	SQLJobConfig     *sqlJobConfig
}

func (job *sqlJob) properties() map[string]interface{} {
	return map[string]interface{}{
		"query":            job.Query,
		"defaultNamespace": job.DefaultNamespace,
		"sqlConfig":        job.SQLJobConfig.properties(),
	}
}

type jobId struct {
	ID         string
	Workspace  string
	InstanceId int64
}

func (id *jobId) properties() map[string]interface{} {
	return map[string]interface{}{
		"id":          id.ID,
		"workspace":   id.Workspace,
		"instance_id": id.InstanceId,
	}
}

type clickzettaAccoount struct {
	UserId int64
}

func (acc *clickzettaAccoount) properties() map[string]interface{} {
	return map[string]interface{}{
		"user_id": acc.UserId,
	}
}

type getJobResultRequest struct {
	Account   *clickzettaAccoount
	JobId     *jobId
	Offset    int64
	UserAgent string
}

func (req *getJobResultRequest) properties() map[string]interface{} {
	return map[string]interface{}{
		"account":   req.Account.properties(),
		"job_id":    req.JobId.properties(),
		"offset":    req.Offset,
		"userAgent": req.UserAgent,
	}
}

type cancelJobRequest struct {
	Account   *clickzettaAccoount
	JobId     *jobId
	UserAgent string
	Force     bool
}

func (req *cancelJobRequest) properties() map[string]interface{} {
	return map[string]interface{}{
		"account":    req.Account.properties(),
		"job_id":     req.JobId.properties(),
		"user_agent": req.UserAgent,
		"force":      req.Force,
	}
}

type jobDesc struct {
	VirtualCluster       string
	JobType              jobType
	JobId                *jobId
	JobName              string
	UserId               int64
	JobRequestMode       jobRequestMode
	HybirdPollingTimeout int64
	JobConfig            map[string]interface{}
	SQLJob               *sqlJob
	JobTimeout           int64
	UserAgent            string
	Priority             int64
}

func (desc *jobDesc) properties() map[string]interface{} {
	return map[string]interface{}{
		"virtualCluster":       desc.VirtualCluster,
		"type":                 desc.JobType,
		"jobId":                desc.JobId.properties(),
		"jobName":              desc.JobName,
		"requestMode":          desc.JobRequestMode,
		"hybridPollingTimeout": desc.HybirdPollingTimeout,
		"jobConfig":            desc.JobConfig,
		"sqlJob":               desc.SQLJob.properties(),
	}
}

type jobRequest struct {
	JobDesc *jobDesc
}

func (req *jobRequest) properties() map[string]interface{} {
	return map[string]interface{}{
		"jobDesc": req.JobDesc.properties(),
	}
}

type apiGetJobRequest struct {
	GetJobResultReq *getJobResultRequest
	UserAgent       string
}

func (req *apiGetJobRequest) properties() map[string]interface{} {
	return map[string]interface{}{
		"get_result_request": req.GetJobResultReq.properties(),
		"user_agent":         req.UserAgent,
	}
}
