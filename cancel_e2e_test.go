package goclickzetta

import (
	"context"
	"errors"
	"testing"
)

// 取消链路的端到端测试，需要 CZ_TEST_DSN 指向真实实例。
// 运行：CZ_TEST_DSN='...' go test -vet=off -count=1 -v -run TestE2ECancel -timeout 120s .
//
// 单测已经覆盖了发出报文的内容，这里补的是单测看不到的部分：服务端是否接受
// cancelJob 报文，以及超时取消在真实链路上是否走通。

func openE2EConn(t *testing.T) *ClickzettaConn {
	t.Helper()
	dsn := getTestDSN(t) // 未设置 CZ_TEST_DSN 时跳过
	c, err := ClickzettaDriver{}.Open(dsn)
	if err != nil {
		t.Fatalf("open connection: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	conn, ok := c.(*ClickzettaConn)
	if !ok {
		t.Fatalf("unexpected driver conn type %T", c)
	}
	return conn
}

// SDK 自己的超时看门狗要能取消真实作业。
// 查询必须睡过 HybirdPollingTimeout(30s)：submit 会在服务端 hold 住这段时间，
// 只有之后进入 retryGetResult 才开始按 sdk.job.timeout 计时并触发取消。
// 取消请求由 checkJobTimeout 内部发出，带 user_agent=gosdk_by_timeout。
func TestE2ECancel_SDKTimeoutCancelsRunningJob(t *testing.T) {
	conn := openE2EConn(t)

	_, err := conn.execInternal(context.Background(),
		"sdk.job.timeout=2;select _sleep_once_ms(40000)", nil)
	if err == nil {
		t.Fatal("expected the query to time out")
	}
	var timeoutErr driverTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("error = %T: %v, want driverTimeoutError", err, err)
	}
}

// 服务端要认 cancelJob 的报文。单测只能证明我们按 user_agent 这个字段名发送，
// 证明不了服务端解析得了它 —— 同一个文件里 getJobResult 用的是 userAgent。
func TestE2ECancel_CancelJobAcceptedByServer(t *testing.T) {
	conn := openE2EConn(t)

	resp, err := conn.execInternal(context.Background(), "select 1", nil)
	if err != nil {
		t.Fatalf("baseline query failed: %v", err)
	}
	jobID := resp.Data.JobId
	if jobID == "" {
		t.Fatal("baseline query returned no job id")
	}

	if err := conn.CancelJob(jobID, "e2e-test"); err != nil {
		t.Errorf("CancelJob(%q) rejected: %v", jobID, err)
	}
}

// 取消当前作业后，连接上的取消状态要能被后续查询清掉。
func TestE2ECancel_ConnectionUsableAfterCancel(t *testing.T) {
	conn := openE2EConn(t)

	if _, err := conn.execInternal(context.Background(),
		"sdk.job.timeout=2;select _sleep_once_ms(40000)", nil); err == nil {
		t.Fatal("expected the first query to time out")
	}

	resp, err := conn.execInternal(context.Background(), "select 1", nil)
	if err != nil {
		t.Fatalf("connection unusable after a cancelled query: %v", err)
	}
	if !resp.Success {
		t.Errorf("follow-up query did not succeed: %s", resp.Message)
	}
}
