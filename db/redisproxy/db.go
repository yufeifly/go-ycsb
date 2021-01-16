package redisproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type redisProxy struct {
	client    *http.Client
	proxyAddr string
}

func (r *redisProxy) Close() error {
	//return r.client.Close()
	return nil
}

func (r *redisProxy) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *redisProxy) CleanupThread(_ context.Context) {
}

func (r *redisProxy) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data := make(map[string][]byte, len(fields))
	fmt.Printf("read key: %s/%s\n", table, key)
	proxyUrl := "http://" + r.proxyAddr + "/redis/get"
	fmt.Printf("Read url: %v\n", proxyUrl)
	req, err := http.NewRequest(http.MethodGet, proxyUrl, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("key", table+"/"+key)
	q.Add("service", "service1")
	// do request
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	// TODO: filter by fields
	return data, err
}

func (r *redisProxy) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan of redis proxy is not supported")
}

func (r *redisProxy) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// get
	proxyUrl := "http://" + r.proxyAddr + "/redis/get"
	fmt.Printf("Read url: %v\n", proxyUrl)
	req, err := http.NewRequest(http.MethodGet, proxyUrl, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("key", table+"/"+key)
	q.Add("service", "service1")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	curVal := map[string][]byte{}
	err = json.Unmarshal(body, &curVal)
	if err != nil {
		return err
	}

	for k, v := range values {
		curVal[k] = v
	}
	var data []byte
	data, err = json.Marshal(curVal)
	if err != nil {
		return err
	}
	// set
	dataBody := url.Values{"key": {table + "/" + key}, "value": {string(data)}, "service": {"service1"}}
	proxySetUrl := "http://" + r.proxyAddr + "/redis/set"
	_, err = http.PostForm(proxySetUrl, dataBody)
	if err != nil {
		return err
	}
	return nil
}

func (r *redisProxy) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	//fmt.Printf("Insert key: %s/%s, value: %v\n", table, key, values)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	dataBody := url.Values{"key": {table + "/" + key}, "value": {string(data)}, "service": {"service1"}}
	proxySetUrl := "http://" + r.proxyAddr + "/redis/set"
	//fmt.Printf("insert url: %v\n",proxySetUrl)
	_, err = http.PostForm(proxySetUrl, dataBody)
	if err != nil {
		return err
	}
	return nil
}

func (r *redisProxy) Delete(ctx context.Context, table string, key string) error {
	fmt.Printf("Delete key: %s/%s\n", table, key)
	dataBody := url.Values{"key": {table + "/" + key}, "service": {"service1"}}
	proxySetUrl := "http://" + r.proxyAddr + "/redis/delete"
	_, err := http.PostForm(proxySetUrl, dataBody)
	if err != nil {
		return err
	}
	return nil
}

type redisProxyCreator struct{}

func (r redisProxyCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rdsProxy := &redisProxy{}

	rdsProxy.proxyAddr, _ = p.Get(proxyAddr)
	fmt.Printf("rdsProxy.proxyAddr: %v\n", rdsProxy.proxyAddr)
	// create a http client here
	rdsProxy.client = createHTTPClient()

	return rdsProxy, nil
}

const (
	proxyAddr = "redisproxy.addr"
)

const (
	MaxIdleConnections int = 20
	RequestTimeout     int = 5
)

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: MaxIdleConnections,
		},
		Timeout: time.Duration(RequestTimeout) * time.Second,
	}

	return client
}

func init() {
	logrus.Info("registering redis proxy")
	ycsb.RegisterDBCreator("redisproxy", redisProxyCreator{})
}
