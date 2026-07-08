package elasticsearch_query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	elasticsearch5 "github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
)

const testDocType = "testquery_data"

type testIndexer struct {
	client *elasticsearch5.Client
	major  int
}

func newTestIndexer(ctx context.Context, baseURL string) (*testIndexer, error) {
	client, err := elasticsearch5.NewClient(elasticsearch5.Config{
		Addresses: []string{baseURL},
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		},
	})
	if err != nil {
		return nil, err
	}

	idx := &testIndexer{client: client}
	major, err := idx.probeMajor(ctx)
	if err != nil {
		return nil, err
	}
	idx.major = major

	return idx, nil
}

func (idx *testIndexer) bulkIndex(ctx context.Context, index string, docs []nginxlog) error {
	meta := map[string]any{
		"_index": index,
	}
	if idx.major <= 6 {
		meta["_type"] = testDocType
	}
	metaLine, err := json.Marshal(map[string]any{"index": meta})
	if err != nil {
		return err
	}

	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, doc := range docs {
		body.Write(metaLine)
		body.WriteByte('\n')
		if err := encoder.Encode(doc); err != nil {
			return err
		}
	}

	var result struct {
		Errors bool `json:"errors"`
	}
	res, err := idx.client.Bulk(
		&body,
		idx.client.Bulk.WithContext(ctx),
	)
	if err := idx.handleResponse(res, err, &result); err != nil {
		return err
	}
	if result.Errors {
		return errors.New("bulk indexing reported item errors")
	}

	return nil
}

func (idx *testIndexer) refresh(ctx context.Context) error {
	res, err := idx.client.Indices.Refresh(
		idx.client.Indices.Refresh.WithContext(ctx),
	)
	return idx.handleResponse(res, err, nil)
}

func (idx *testIndexer) probeMajor(ctx context.Context) (int, error) {
	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}

	res, err := idx.client.Info(idx.client.Info.WithContext(ctx))
	if err := idx.handleResponse(res, err, &info); err != nil {
		return 0, err
	}

	majorText, _, _ := strings.Cut(info.Version.Number, ".")
	major, err := strconv.Atoi(majorText)
	if err != nil {
		return 0, fmt.Errorf("parsing Elasticsearch version %q failed: %w", info.Version.Number, err)
	}

	return major, nil
}

func (*testIndexer) handleResponse(res *esapi.Response, err error, out any) error {
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		data, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%s: %s", res.Status(), strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}

	return nil
}
