package elasticsearch_query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
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

type serverVersion struct {
	Number string `json:"number"`
}

type serverInfo struct {
	Version serverVersion `json:"version"`
}

func (cfg clientConfig) probeVersion(ctx context.Context) (string, uint64, error) {
	// Use the v5 client only for the version-agnostic GET / probe.
	probe, err := elasticsearch5.NewClient(elasticsearch5.Config{
		Addresses: cfg.urls,
		Username:  cfg.username,
		Password:  cfg.password,
		Transport: roundTripper{client: cfg.httpClient},
	})
	if err != nil {
		return "", 0, fmt.Errorf("creating ElasticSearch client failed: %w", err)
	}

	res, err := probe.Info(probe.Info.WithContext(ctx))
	if err != nil {
		return "", 0, fmt.Errorf("getting server version failed: %w", err)
	}
	defer res.Body.Close()

	if err := checkForError(res.StatusCode, res.Body); err != nil {
		return "", 0, fmt.Errorf("getting server version failed: %w", err)
	}

	var info serverInfo
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return "", 0, fmt.Errorf("getting server version failed: %w", err)
	}

	version, err := semver.NewVersion(info.Version.Number)
	if err != nil {
		return "", 0, fmt.Errorf("parsing server version %q failed: %w", info.Version.Number, err)
	}

	return info.Version.Number, version.Major(), nil
}

type apiErrorDetails struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type apiErrorResponse struct {
	Error apiErrorDetails `json:"error"`
}

type apiError struct {
	statusCode int
	errorType  string
	reason     string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("received error %d (%s)", e.statusCode, http.StatusText(e.statusCode))
	if e.reason != "" {
		msg += ": " + e.reason
	}
	if e.errorType != "" {
		msg += " [type=" + e.errorType + "]"
	}
	return msg
}

func checkForError(statusCode int, body io.Reader) error {
	if statusCode >= 200 && statusCode < 300 {
		return nil
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}

	var response apiErrorResponse
	if err := json.Unmarshal(data, &response); err == nil && response.Error.Reason != "" {
		return &apiError{
			statusCode: statusCode,
			errorType:  response.Error.Type,
			reason:     response.Error.Reason,
		}
	}

	return &apiError{statusCode: statusCode, reason: strings.TrimSpace(string(data))}
}

type roundTripper struct {
	client *http.Client
}

// RoundTrip delegates to the configured HTTP client to preserve its overall
// timeout, including response-body reads, and cookie and redirect handling
// that the underlying transport does not provide.
func (t roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.client.Do(req)
}

// startDiscovery runs node discovery immediately and repeats it when the interval is positive.
// The returned function stops the loop and waits for any active call to return.
func startDiscovery(log telegraf.Logger, interval time.Duration, discover func(context.Context) error) func() {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := discover(ctx); err != nil && ctx.Err() == nil {
			log.Errorf("Discovering ElasticSearch nodes failed: %v", err)
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
					log.Errorf("Discovering ElasticSearch nodes failed: %v", err)
				}
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
	}
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

func buildQueries(aggregation *aggregation) error {
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

type searchHits struct {
	Total json.RawMessage `json:"total"`
}

type searchResponse struct {
	Hits         searchHits                 `json:"hits"`
	Aggregations map[string]json.RawMessage `json:"aggregations"`
}

type totalHits struct {
	Value int64 `json:"value"`
}

func (r *searchResponse) totalHits() int64 {
	var total int64
	if err := json.Unmarshal(r.Hits.Total, &total); err == nil {
		return total
	}

	// Elasticsearch 7 and later return hits.total as an object.
	var result totalHits
	if err := json.Unmarshal(r.Hits.Total, &result); err == nil {
		return result.Value
	}

	return 0
}

func buildSearchBody(aggregation *aggregation, log telegraf.Logger) ([]byte, error) {
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
		return nil, fmt.Errorf("marshal request failed: %w", err)
	}
	log.Debugf("{\"query\": %s}", string(data))

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
		return nil, fmt.Errorf("marshal request failed: %w", err)
	}
	return data, nil
}

type aggregationValue struct {
	Value *float64 `json:"value"`
}

type aggregationBuckets struct {
	Buckets []map[string]json.RawMessage `json:"buckets"`
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
			var result aggregationValue
			if err := json.Unmarshal(response[name], &result); err != nil {
				return err
			}
			if result.Value != nil {
				m.fields[name] = *result.Value
			} else {
				m.fields[name] = float64(0)
			}
		case "terms":
			var result aggregationBuckets
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

func aggregate(acc telegraf.Accumulator, measurement string, nameFunction map[string]string, response interface{}) error {
	// The query method returns map[string]json.RawMessage for aggregation responses.
	r := response.(map[string]json.RawMessage)

	m := &aggregationIterator{
		name:   measurement,
		fields: make(map[string]interface{}),
		tags:   make(map[string]string),
	}

	return m.iterate(acc, nameFunction, r)
}
