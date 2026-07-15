package elasticsearch_query

import (
	"context"
	"fmt"
	"io"

	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
)

func newClientV6(cfg clientConfig) (client, error) {
	c, err := elasticsearch6.NewClient(elasticsearch6.Config{
		Addresses: cfg.urls,
		Username:  cfg.username,
		Password:  cfg.password,
		Transport: roundTripper{client: cfg.httpClient},
	})
	if err != nil {
		cfg.httpClient.CloseIdleConnections()
		return nil, fmt.Errorf("creating Elasticsearch client failed: %w", err)
	}

	client := &queryClient{
		httpClient: cfg.httpClient,
		log:        cfg.log,
		getFieldMappingResponse: func(ctx context.Context, index, field string) (*esResponse, error) {
			res, err := c.Indices.GetFieldMapping(
				[]string{field},
				c.Indices.GetFieldMapping.WithContext(ctx),
				c.Indices.GetFieldMapping.WithIndex(index),
			)
			if res == nil {
				return nil, err
			}
			return &esResponse{statusCode: res.StatusCode, body: res.Body}, err
		},
		search: func(ctx context.Context, index string, body io.Reader) (*esResponse, error) {
			res, err := c.Search(
				c.Search.WithContext(ctx),
				c.Search.WithIndex(index),
				c.Search.WithBody(body),
			)
			if res == nil {
				return nil, err
			}
			return &esResponse{statusCode: res.StatusCode, body: res.Body}, err
		},
	}
	if cfg.enableSniffer {
		// The v6 client exposes only DiscoverNodes(), so in-flight calls cannot be canceled.
		client.startDiscovery(cfg.discoveryInterval, func(context.Context) error { return c.DiscoverNodes() })
	}
	return client, nil
}
