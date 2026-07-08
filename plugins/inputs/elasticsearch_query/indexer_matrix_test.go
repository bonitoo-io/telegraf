//go:build elasticsearch_indexer_selftest

// Self-test verifying the official v5 client works against all supported
// Elasticsearch versions. Not run in CI; execute manually with:
//   go test -tags elasticsearch_indexer_selftest -run TestIndexerMatrix ./plugins/inputs/elasticsearch_query/

package elasticsearch_query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/influxdata/telegraf/testutil"
)

func TestIndexerMatrix(t *testing.T) {
	tests := []struct {
		name  string
		image string
		env   map[string]string
	}{
		{
			name:  "5.6.16",
			image: "docker.elastic.co/elasticsearch/elasticsearch:5.6.16",
			env: map[string]string{
				"discovery.type":         "single-node",
				"xpack.security.enabled": "false",
			},
		},
		{
			name:  "6.8.23",
			image: "docker.elastic.co/elasticsearch/elasticsearch:6.8.23",
			env: map[string]string{
				"discovery.type": "single-node",
			},
		},
		{
			name:  "7.17.29",
			image: "docker.elastic.co/elasticsearch/elasticsearch:7.17.29",
			env: map[string]string{
				"discovery.type": "single-node",
			},
		},
		{
			name:  "8.19.18",
			image: "docker.elastic.co/elasticsearch/elasticsearch:8.19.18",
			env: map[string]string{
				"discovery.type":         "single-node",
				"xpack.security.enabled": "false",
			},
		},
		{
			name:  "9.4.3",
			image: "docker.elastic.co/elasticsearch/elasticsearch:9.4.3",
			env: map[string]string{
				"discovery.type":         "single-node",
				"xpack.security.enabled": "false",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			container := &testutil.Container{
				Image:        tt.image,
				ExposedPorts: []string{servicePort},
				Env:          tt.env,
				WaitingFor: wait.ForHTTP("/").WithPort(servicePort).
					WithStartupTimeout(5 * time.Minute),
			}
			require.NoError(t, container.Start(), "failed to start container")
			defer container.Terminate()

			addr := "http://" + container.Address + ":" + container.Ports[servicePort]
			client, err := newTestIndexer(t.Context(), addr)
			require.NoError(t, err)

			expected := nginxlog{
				IPaddress:    "127.0.0.1",
				Timestamp:    time.Date(2026, 7, 6, 12, 13, 14, 0, time.UTC),
				Method:       "GET",
				URI:          "/downloads/product_1",
				Httpversion:  "HTTP/1.1",
				Response:     "200",
				Size:         490,
				ResponseTime: 1514,
			}
			require.NoError(t, client.bulkIndex(t.Context(), testindex, []nginxlog{expected}))
			require.NoError(t, client.refresh(t.Context()))

			actual, err := queryIndexedLog(t.Context(), addr, testindex)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func queryIndexedLog(ctx context.Context, baseURL, index string) (nginxlog, error) {
	body := bytes.NewBufferString(`{"query":{"match_all":{}}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/"+index+"/_search", body)
	if err != nil {
		return nginxlog{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nginxlog{}, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nginxlog{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nginxlog{}, fmt.Errorf("search failed with status %s: %s", resp.Status, string(data))
	}

	var result struct {
		Hits struct {
			Hits []struct {
				Source nginxlog `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nginxlog{}, err
	}
	if len(result.Hits.Hits) != 1 {
		return nginxlog{}, fmt.Errorf("expected 1 search hit, got %d", len(result.Hits.Hits))
	}

	return result.Hits.Hits[0].Source, nil
}
