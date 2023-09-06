package goclickzetta

import (
	"context"
	"net/http"
	"net/url"
	"testing"
)

func TestClientGet(t *testing.T) {
	t.Log("TestClientGet")
	hClient := &http.Client{Transport: HTTPTransport}
	transport := HTTPTransport
	client := &httpClient{
		client:    hClient,
		transport: transport,
	}
	url, err := url.Parse("https://www.baidu.com")
	if err != nil {
		t.Error(err)
	}
	ctx := context.TODO()
	resp, err := client.Get(ctx, url, nil, 0)
	if err != nil {
		t.Error(err)
	}
	t.Log(resp)
	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestClientPost(t *testing.T) {
	t.Log("TestClientPost")
	hClient := &http.Client{Transport: HTTPTransport}
	transport := HTTPTransport
	client := &httpClient{
		client:    hClient,
		transport: transport,
	}
	url, err := url.Parse("https://www.baidu.com")
	if err != nil {
		t.Error(err)
	}
	ctx := context.TODO()
	resp, err := client.Post(ctx, url, nil, nil, 0)
	if err != nil {
		t.Error(err)
	}
	t.Log(resp)
	err = client.Close()
	if err != nil {
		t.Error(err)
	}
}
