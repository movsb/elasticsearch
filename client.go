package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

// Client is an Elasticsearch client.
type Client struct {
	address string
	client  *http.Client
}

// NewClient creates a new clients.
func NewClient(address string) *Client {
	client := &Client{
		address: "http://" + address,
		client:  &http.Client{},
	}
	return client
}

func (c *Client) endpoint(f string, args ...interface{}) string {
	s := c.address
	p := fmt.Sprintf(f, args...)
	if len(p) > 0 {
		if p[0] != '/' {
			s += "/"
		}
		s += p
	}
	return s
}

func (c *Client) do(method string, path string, query Q, in interface{}) (*http.Response, error) {
	u := path
	values := url.Values{}
	for k, v := range query {
		values.Add(k, fmt.Sprint(v))
	}
	if q := values.Encode(); len(q) > 0 {
		u += "?" + q
	}

	var body []byte
	if in != nil {
		bd, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bd
	}

	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// CreateTemplate puts a template.
func (c *Client) CreateTemplate(name string, template string) error {
	path := c.endpoint("/_template/%s", name)
	resp, err := c.do(http.MethodPut, path, nil, json.RawMessage(template))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 201:
		return c.checkAcknowledgement(resp, false)
	default:
		return c.reportError(resp)
	}
}

// DeleteTemplate ...
func (c *Client) DeleteTemplate(name string) error {
	path := c.endpoint("/_template/%s", name)
	resp, err := c.do(http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 204:
		return c.checkAcknowledgement(resp, false)
	default:
		return c.reportError(resp)
	}
}

// CreateIndex ...
func (c *Client) CreateIndex(name string) error {
	path := c.endpoint("/%s", name)
	resp, err := c.do(http.MethodPut, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 201:
		return c.checkAcknowledgement(resp, true)
	default:
		return c.reportError(resp)
	}
}

// DeleteIndex ...
func (c *Client) DeleteIndex(name string) error {
	path := c.endpoint("/%s", name)
	resp, err := c.do(http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 204:
		return c.checkAcknowledgement(resp, false)
	default:
		return c.reportError(resp)
	}
}

// CreateDocument ...
func (c *Client) CreateDocument(index string, id string, doc interface{}) error {
	var path string
	var method string
	if id != "" {
		path = c.endpoint(`/%s/_doc/%s`, index, id)
		method = http.MethodPut
	} else {
		path = c.endpoint(`/%s/_doc`, index)
		method = http.MethodPost
	}
	resp, err := c.do(method, path, nil, doc)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 201:
		return nil
	default:
		return c.reportError(resp)
	}
}

// DeleteDocument ...
func (c *Client) DeleteDocument(index string, id string) error {
	path := c.endpoint(`/%s/_doc/%s`, index, id)
	resp, err := c.do(http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 204:
		type Response struct {
			Result string `json:"result"`
		}
		var r Response
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return err
		}
		if r.Result == "deleted" {
			return nil
		}
		if r.Result == "not_found" {
			return nil
		}
		return fmt.Errorf("unhandled error")
	default:
		return c.reportError(resp)
	}
}

// SearchDocuments ...
func (c *Client) SearchDocuments(index string, query Q, body string) (string, error) {
	path := c.endpoint(`/%s/_search`, index)
	resp, err := c.do(http.MethodPost, path, query, json.RawMessage(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", c.reportError(resp)
	}
	bd, err := ioutil.ReadAll(resp.Body)
	return string(bd), err
}

func (c *Client) checkAcknowledgement(resp *http.Response, shards bool) error {
	var ack Acknowledgement
	if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
		return err
	}
	if ack.Acknowledged && !shards && !ack.ShardsAcknowledged {
		return nil
	}
	if ack.Acknowledged && shards && ack.ShardsAcknowledged {
		return nil
	}
	return fmt.Errorf("not acknowledged")
}

func (c *Client) reportError(resp *http.Response) error {
	var e Error
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		return err
	}
	return &e
}
