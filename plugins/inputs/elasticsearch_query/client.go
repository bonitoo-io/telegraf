package elasticsearch_query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	elasticsearch5 "github.com/elastic/go-elasticsearch/v5"

	"github.com/influxdata/telegraf"
)

type clientConfig struct {
	urls              []string
	username          string
	password          string
	enableSniffer     bool
	discoveryInterval time.Duration
	httpClient        *http.Client
	log               telegraf.Logger
}

func (cfg clientConfig) probeVersion(ctx context.Context) (string, int, error) {
	// Use the v5 client only for the version-agnostic GET / probe.
	probe, err := elasticsearch5.NewClient(elasticsearch5.Config{
		Addresses: cfg.urls,
		Username:  cfg.username,
		Password:  cfg.password,
		Transport: roundTripper{client: cfg.httpClient},
	})
	if err != nil {
		return "", 0, fmt.Errorf("creating Elasticsearch client failed: %w", err)
	}

	res, err := probe.Info(probe.Info.WithContext(ctx))
	if err != nil {
		return "", 0, err
	}
	response := &esResponse{statusCode: res.StatusCode, body: res.Body}

	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := response.handle(nil, &info); err != nil {
		return "", 0, err
	}

	majorText, _, _ := strings.Cut(info.Version.Number, ".")
	major, err := strconv.Atoi(majorText)
	if err != nil {
		return "", 0, fmt.Errorf("parsing server version %q failed: %w", info.Version.Number, err)
	}

	return info.Version.Number, major, nil
}

type esResponse struct {
	statusCode int
	body       io.ReadCloser
}

func (r *esResponse) handle(err error, out interface{}) error {
	if err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	defer r.body.Close()

	if r.statusCode > 299 {
		data, err := io.ReadAll(r.body)
		if err != nil {
			return err
		}

		var result struct {
			Error struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		}
		if err := json.Unmarshal(data, &result); err == nil {
			if result.Error.Type != "" && result.Error.Reason != "" {
				return fmt.Errorf("%s - %s", result.Error.Type, result.Error.Reason)
			}
			if result.Error.Reason != "" {
				return fmt.Errorf("error %d (%s): %s", r.statusCode, http.StatusText(r.statusCode), result.Error.Reason)
			}
		}

		return fmt.Errorf("error %d (%s): %s", r.statusCode, http.StatusText(r.statusCode), strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.NewDecoder(r.body).Decode(out)
	}

	return nil
}

type roundTripper struct {
	client *http.Client
}

// RoundTrip implements http.RoundTripper.
func (t roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.client.Do(req)
}

// queryClient provides shared query behavior. Per-major adapters wrap their
// distinct response types, which expose status and body as fields.
type queryClient struct {
	httpClient    *http.Client
	log           telegraf.Logger
	closed        bool
	stopDiscovery func()

	getFieldMappingResponse func(context.Context, string, string) (*esResponse, error)
	search                  func(context.Context, string, io.Reader) (*esResponse, error)
}

func (e *ElasticsearchQuery) newClient() (client, error) {
	// Make sure the HTTP client exists
	httpClient, err := e.HTTPClientConfig.CreateClient(context.Background(), e.Log)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client failed: %w", err)
	}

	cfg := clientConfig{
		urls:              e.URLs,
		username:          e.Username,
		password:          e.Password,
		enableSniffer:     e.EnableSniffer,
		discoveryInterval: time.Duration(e.HealthCheckInterval),
		httpClient:        httpClient,
		log:               e.Log,
	}

	version, major, err := cfg.probeVersion(context.Background())
	if err != nil {
		httpClient.CloseIdleConnections()
		return nil, fmt.Errorf("getting server version failed: %w", err)
	}

	switch major {
	case 5:
		return newClientV5(cfg)
	case 6:
		return newClientV6(cfg)
	default:
		httpClient.CloseIdleConnections()
		return nil, fmt.Errorf("server version %q not supported (currently supported versions are 5.x and 6.x)", version)
	}
}

func (c *queryClient) close() {
	c.closed = true
	if c.stopDiscovery != nil {
		c.stopDiscovery()
		c.stopDiscovery = nil
	}
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
		c.httpClient = nil
	}
}

func (c *queryClient) startDiscovery(interval time.Duration, discover func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := discover(ctx); err != nil && ctx.Err() == nil {
			c.log.Errorf("Discovering Elasticsearch nodes failed: %v", err)
		}
		if interval <= 0 {
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := discover(ctx); err != nil && ctx.Err() == nil {
					c.log.Errorf("Discovering Elasticsearch nodes failed: %v", err)
				}
			}
		}
	})

	c.stopDiscovery = func() {
		cancel()
		wg.Wait()
	}
}

func (c *queryClient) isRunning() bool {
	return !c.closed
}

func (c *queryClient) getFieldMapping(ctx context.Context, index, field string) (map[string]interface{}, error) {
	res, err := c.getFieldMappingResponse(ctx, index, field)
	var result map[string]interface{}
	if err := res.handle(err, &result); err != nil {
		return nil, err
	}
	return result, nil
}

type queryData struct {
	measurement string
	name        string
	function    string
	isParent    bool
	aggregation map[string]interface{}
}

func (q *queryData) addSubAggregation(name string, subAggregation map[string]interface{}) {
	aggs, ok := q.aggregation["aggs"].(map[string]interface{})
	if !ok {
		aggs = make(map[string]interface{})
		q.aggregation["aggs"] = aggs
	}
	aggs[name] = subAggregation
}

func (*queryClient) buildQueries(aggregation *aggregation) error {
	// Create one aggregation per metric field found or function defined for
	// numeric fields
	queries := make([]queryData, 0, len(aggregation.mapMetricFields)+len(aggregation.Tags))
	for k, v := range aggregation.mapMetricFields {
		switch v {
		case "long", "float", "integer", "short", "double", "scaled_float":
		default:
			continue
		}

		var agg map[string]interface{}
		switch aggregation.MetricFunction {
		case "avg", "sum", "min", "max":
			agg = map[string]interface{}{
				aggregation.MetricFunction: map[string]interface{}{
					"field": k,
				},
			}
		default:
			return fmt.Errorf("aggregation function %q not supported", aggregation.MetricFunction)
		}

		query := queryData{
			measurement: aggregation.MeasurementName,
			function:    aggregation.MetricFunction,
			name:        strings.ReplaceAll(k, ".", "_") + "_" + aggregation.MetricFunction,
			isParent:    true,
			aggregation: agg,
		}
		queries = append(queries, query)
	}

	// Create a terms aggregation per tag
	for _, term := range aggregation.Tags {
		terms := map[string]interface{}{
			"field": term,
			"size":  1000,
		}
		if aggregation.IncludeMissingTag && aggregation.MissingTagValue != "" {
			terms["missing"] = aggregation.MissingTagValue
		}
		query := queryData{
			measurement: aggregation.MeasurementName,
			function:    "terms",
			name:        strings.ReplaceAll(term, ".", "_"),
			isParent:    true,
			aggregation: map[string]interface{}{"terms": terms},
		}

		// add each previous parent aggregations as subaggregations of this terms aggregation
		for key, q := range queries {
			if !q.isParent {
				continue
			}

			query.addSubAggregation(q.name, q.aggregation)

			// Update subaggregation map with parent information
			queries[key].isParent = false
		}

		queries = append(queries, query)
	}
	aggregation.queries = queries

	// Prepare measurement mapping to organize the aggregation query data
	// by measurement
	measurements := make(map[string]map[string]string, len(queries))
	for _, query := range queries {
		nameFunctions, ok := measurements[query.measurement]
		if !ok {
			nameFunctions = make(map[string]string)
			measurements[query.measurement] = nameFunctions
		}
		nameFunctions[query.name] = query.function
	}
	aggregation.measurements = measurements

	return nil
}

func (a *aggregation) buildRangeQuery(from, to time.Time) map[string]interface{} {
	rangeQuery := map[string]interface{}{
		"gte": from,
		"lte": to,
	}
	if a.DateFieldFormat != "" {
		rangeQuery["format"] = a.DateFieldFormat
	}
	return rangeQuery
}

type searchResponse struct {
	Hits struct {
		Total json.RawMessage `json:"total"`
	} `json:"hits"`
	Aggregations map[string]json.RawMessage `json:"aggregations"`
}

func (r *searchResponse) totalHits() int64 {
	var total int64
	if err := json.Unmarshal(r.Hits.Total, &total); err == nil {
		return total
	}

	// Elasticsearch 7 and later return hits.total as an object.
	var totalObject struct {
		Value int64 `json:"value"`
	}
	if err := json.Unmarshal(r.Hits.Total, &totalObject); err == nil {
		return totalObject.Value
	}

	return 0
}

func (c *queryClient) query(ctx context.Context, aggregation *aggregation) (interface{}, int64, error) {
	// buildQueries stores []queryData in this field before query execution.
	// If the assertion fails, it indicates a programming error in this package.
	queries := aggregation.queries.([]queryData)

	now := time.Now().UTC()
	from := now.Add(-time.Duration(aggregation.QueryPeriod))

	query := map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": []interface{}{
				map[string]interface{}{
					"query_string": map[string]interface{}{
						"query": aggregation.FilterQuery,
					},
				},
				map[string]interface{}{
					"range": map[string]interface{}{
						aggregation.DateField: aggregation.buildRangeQuery(from, now),
					},
				},
			},
		},
	}

	data, err := json.Marshal(query)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request failed: %w", err)
	}
	c.log.Debugf("{\"query\": %s}", string(data))

	body := map[string]interface{}{
		"query": query,
		"size":  0,
	}

	aggs := make(map[string]interface{})
	for _, v := range queries {
		if v.isParent && v.aggregation != nil {
			aggs[v.name] = v.aggregation
		}
	}
	if len(aggs) > 0 {
		body["aggs"] = aggs
	}

	data, err = json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request failed: %w", err)
	}

	res, err := c.search(ctx, aggregation.Index, bytes.NewReader(data))
	var result searchResponse
	if err := res.handle(err, &result); err != nil {
		return nil, 0, err
	}

	if len(result.Aggregations) == 0 {
		return nil, result.totalHits(), nil
	}

	return result.Aggregations, result.totalHits(), nil
}

type aggregationIterator struct {
	name   string
	fields map[string]interface{}
	tags   map[string]string
}

func (m *aggregationIterator) iterate(acc telegraf.Accumulator, nameFunction map[string]string, response map[string]json.RawMessage) error {
	names := make([]string, 0, len(response))
	for k := range response {
		if k != "key" && k != "doc_count" {
			names = append(names, k)
		}
	}
	if len(names) == 0 {
		// We've reached a single bucket or response without aggregation, i.e.
		// we've reached a leaf node. Add the accumulated metric and reset it
		if len(m.fields) > 0 {
			acc.AddFields(m.name, m.fields, m.tags)
			m.fields = make(map[string]interface{})
		}
		return nil
	}

	// Metrics aggregations response can contain multiple field values, so we
	// iterate over them
	for _, name := range names {
		function, found := nameFunction[name]
		if !found {
			return fmt.Errorf("child aggregation function %q not found %v", name, nameFunction)
		}

		// Execute the aggregation function
		switch function {
		case "avg", "sum", "min", "max":
			var result struct {
				Value *float64 `json:"value"`
			}
			if err := json.Unmarshal(response[name], &result); err != nil {
				return err
			}
			if result.Value != nil {
				m.fields[name] = *result.Value
			} else {
				m.fields[name] = float64(0)
			}
		case "terms":
			var result struct {
				Buckets []map[string]json.RawMessage `json:"buckets"`
			}
			if err := json.Unmarshal(response[name], &result); err != nil {
				return err
			}

			// We've found a terms aggregation, iterate over the buckets and try
			// to retrieve the inner aggregation values
			for _, bucket := range result.Buckets {
				var key string
				if err := json.Unmarshal(bucket["key"], &key); err != nil {
					return fmt.Errorf("bucket key is not a string (%s, %s)", name, function)
				}
				m.tags[name] = key

				var docCount int64
				if err := json.Unmarshal(bucket["doc_count"], &docCount); err != nil {
					return err
				}
				m.fields["doc_count"] = docCount

				// We need to recurse down through the buckets, as it may
				// contain another terms aggregation
				if err := m.iterate(acc, nameFunction, bucket); err != nil {
					return err
				}
				delete(m.tags, name)
			}
		default:
			return fmt.Errorf("aggregation %q not supported", function)
		}
	}

	// If there are fields here it comes from a metrics aggregation without a
	// parent terms aggregation
	if len(m.fields) > 0 {
		acc.AddFields(m.name, m.fields, m.tags)
		m.fields = make(map[string]interface{})
	}

	return nil
}

func (*queryClient) aggregate(acc telegraf.Accumulator, measurement string, nameFunction map[string]string, response interface{}) error {
	// The query method returns map[string]json.RawMessage for aggregation responses.
	r := response.(map[string]json.RawMessage)

	m := &aggregationIterator{
		name:   measurement,
		fields: make(map[string]interface{}),
		tags:   make(map[string]string),
	}

	return m.iterate(acc, nameFunction, r)
}
