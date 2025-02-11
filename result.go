package goclickzetta

type queryStatus string

const (
	// QueryStatusInProgress denotes a query execution in progress
	QueryStatusInProgress queryStatus = "queryStatusInProgress"
	// QueryStatusComplete denotes a completed query execution
	QueryStatusComplete queryStatus = "queryStatusComplete"
	// QueryFailed denotes a failed query
	QueryFailed queryStatus = "queryFailed"
)

// ClickzettaResult provides an API for methods exposed to the clients
type ClickzettaResult interface {
	GetQueryID() string
	GetStatus() queryStatus
	GetError() error
}

type clickzettaResult struct {
	affectedRows int64
	insertID     int64 // Clickzetta doesn't support last insert id
	queryID      string
	status       queryStatus
	err          error
}

func (res *clickzettaResult) LastInsertId() (int64, error) {
	return res.insertID, nil
}

func (res *clickzettaResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

func (res *clickzettaResult) GetQueryID() string {
	return res.queryID
}

func (res *clickzettaResult) GetStatus() queryStatus {
	return res.status
}

func (res *clickzettaResult) GetError() error {
	return res.err
}
