package goclickzetta

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"time"
)

// InternalClient is implemented by HTTPClient
type InternalClient interface {
	Get(context.Context, *url.URL, map[string]string, time.Duration) (*http.Response, error)
	Post(context.Context, *url.URL, map[string]string, []byte, time.Duration) (*http.Response, error)
	Close() error
}

type httpClient struct {
	client    *http.Client
	transport *http.Transport
}

func (cli *httpClient) Close() error {
	cli.transport.CloseIdleConnections()
	cli.client.CloseIdleConnections()
	return nil
}

func (cli *httpClient) Get(
	ctx context.Context,
	url *url.URL,
	headers map[string]string,
	timeout time.Duration) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url.String(), nil)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to create a new http GET request: %v", err)
		return nil, err
	}
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	return cli.client.Do(req)
}

func (cli *httpClient) Post(
	ctx context.Context,
	url *url.URL,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url.String(), bytes.NewBuffer(body))
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to create a new http POST request: %v", err)
		return nil, err
	}
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	return cli.client.Do(req)
}
