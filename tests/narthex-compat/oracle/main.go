// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command oracle evaluates a case file with the pinned Prometheus engine.
//
// It is the reference side of the conformance comparison: the same case file is loaded into
// GreptimeDB and queried over HTTP, and the two normalized answers are compared field by field.
// Running the real engine here - instead of comparing against checked-in expected text - is what
// makes the comparison a cross-check rather than a snapshot of whatever this repository believed at
// the time.
//
// Usage:
//
//	oracle -cases cases/selector.json [-out prometheus.json]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
)

// Float marshals to JSON so that NaN and the infinities survive: JSON has no syntax for them, and
// encoding them as `null` would make a missing sample indistinguishable from NaN.
type Float float64

func (f Float) MarshalJSON() ([]byte, error) {
	v := float64(f)
	switch {
	case math.IsNaN(v):
		return []byte(`"NaN"`), nil
	case math.IsInf(v, 1):
		return []byte(`"+Inf"`), nil
	case math.IsInf(v, -1):
		return []byte(`"-Inf"`), nil
	default:
		return []byte(strconv.FormatFloat(v, 'g', -1, 64)), nil
	}
}

// Point is one sample: its evaluation timestamp and value.
type Point struct {
	T int64 `json:"t"`
	V Float `json:"v"`
}

// Case is one group of input series and the queries to run over them.
type Case struct {
	Name            string    `json:"name"`
	Series          []Series  `json:"series"`
	LookbackDeltaMS *int64    `json:"lookback_delta_ms,omitempty"`
	Queries         []Query   `json:"queries"`
}

// Series is one input series: its labels and its samples.
type Series struct {
	Labels  map[string]string `json:"labels"`
	Samples []Point           `json:"samples"`
}

// Query is one evaluation. `kind` selects instant or range semantics.
type Query struct {
	Name  string `json:"name"`
	Expr  string `json:"expr"`
	Kind  string `json:"kind"` // "instant" or "range"
	Time  int64  `json:"time,omitempty"`
	Start int64  `json:"start,omitempty"`
	End   int64  `json:"end,omitempty"`
	Step  int64  `json:"step,omitempty"`
	// ExpectError marks a query both engines must refuse. The comparison then checks that
	// GreptimeDB refuses it too, instead of comparing values.
	ExpectError bool `json:"expect_error,omitempty"`
}

// Answer is one query's normalized result.
type Answer struct {
	Name     string       `json:"name"`
	Query    string       `json:"query"`
	Kind     string       `json:"kind"`
	Type     string       `json:"type,omitempty"`
	Series   []OutSeries  `json:"series,omitempty"`
	Scalar   *Float       `json:"scalar,omitempty"`
	Error    string       `json:"error,omitempty"`
	Warnings []string     `json:"warnings,omitempty"`
}

// OutSeries is one result series: labels plus points in time order.
type OutSeries struct {
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

func main() {
	casesPath := flag.String("cases", "", "case file to evaluate")
	outPath := flag.String("out", "", "where to write the JSON answers (default stdout)")
	flag.Parse()

	if *casesPath == "" {
		fmt.Fprintln(os.Stderr, "error: -cases is required")
		os.Exit(2)
	}

	raw, err := os.ReadFile(*casesPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: read %s: %v\n", *casesPath, err)
		os.Exit(2)
	}
	var c Case
	if err := json.Unmarshal(raw, &c); err != nil {
		fmt.Fprintf(os.Stderr, "error: parse %s: %v\n", *casesPath, err)
		os.Exit(2)
	}

	answers, err := evaluate(c)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: evaluate %s: %v\n", *casesPath, err)
		os.Exit(1)
	}

	encoded, err := json.MarshalIndent(answers, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: encode answers: %v\n", err)
		os.Exit(1)
	}
	encoded = append(encoded, '\n')
	if *outPath == "" {
		if _, err := os.Stdout.Write(encoded); err != nil {
			fmt.Fprintf(os.Stderr, "error: write stdout: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if err := os.WriteFile(*outPath, encoded, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error: write %s: %v\n", *outPath, err)
		os.Exit(1)
	}
}

func evaluate(c Case) ([]Answer, error) {
	ctx := context.Background()

	storage, err := teststorage.NewWithError()
	if err != nil {
		return nil, fmt.Errorf("create storage: %w", err)
	}
	defer storage.Close()

	app := storage.Appender(ctx)
	for _, s := range c.Series {
		lbls := buildLabels(s.Labels)
		for _, sample := range s.Samples {
			if _, err := app.Append(0, lbls, sample.T, float64(sample.V)); err != nil {
				return nil, fmt.Errorf("append %s at %d: %w", lbls, sample.T, err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		return nil, fmt.Errorf("commit samples: %w", err)
	}

	lookback := 5 * time.Minute
	if c.LookbackDeltaMS != nil {
		lookback = time.Duration(*c.LookbackDeltaMS) * time.Millisecond
	}
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:           50_000_000,
		Timeout:              30 * time.Second,
		LookbackDelta:        lookback,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		// The parser options Prometheus' own test harness enables, so a fixture may use them.
		Parser: parser.NewParser(parser.Options{
			EnableExperimentalFunctions:  true,
			ExperimentalDurationExpr:     true,
			EnableExtendedRangeSelectors: true,
			EnableBinopFillModifiers:     true,
		}),
	})

	answers := make([]Answer, 0, len(c.Queries))
	for _, q := range c.Queries {
		answers = append(answers, runOne(ctx, engine, storage, q))
	}
	return answers, nil
}

func runOne(ctx context.Context, engine *promql.Engine, q storage.Queryable, query Query) Answer {
	answer := Answer{Name: query.Name, Query: query.Expr, Kind: query.Kind}

	var (
		querier promql.Query
		err     error
	)
	switch query.Kind {
	case "instant":
		querier, err = engine.NewInstantQuery(ctx, q, nil, query.Expr, time.UnixMilli(query.Time))
	case "range":
		if query.Step <= 0 {
			answer.Error = "range query needs a positive step"
			return answer
		}
		querier, err = engine.NewRangeQuery(
			ctx, q, nil, query.Expr,
			time.UnixMilli(query.Start), time.UnixMilli(query.End),
			time.Duration(query.Step)*time.Millisecond,
		)
	default:
		answer.Error = fmt.Sprintf("unknown query kind %q", query.Kind)
		return answer
	}
	if err != nil {
		answer.Error = err.Error()
		return answer
	}
	defer querier.Close()

	result := querier.Exec(ctx)
	if result.Err != nil {
		answer.Error = result.Err.Error()
		return answer
	}
	for _, w := range result.Warnings {
		answer.Warnings = append(answer.Warnings, w.Error())
	}

	switch v := result.Value.(type) {
	case promql.Vector:
		answer.Type = "vector"
		answer.Series = vectorSeries(v)
	case promql.Matrix:
		answer.Type = "matrix"
		answer.Series = matrixSeries(v)
	case promql.Scalar:
		answer.Type = "scalar"
		value := Float(v.V)
		answer.Scalar = &value
	case promql.String:
		answer.Type = "string"
	default:
		answer.Type = fmt.Sprintf("unsupported %T", v)
		answer.Error = answer.Type
	}
	return answer
}

func buildLabels(in map[string]string) labels.Labels {
	builder := labels.NewBuilder(labels.EmptyLabels())
	names := make([]string, 0, len(in))
	for name := range in {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		builder.Set(name, in[name])
	}
	return builder.Labels()
}

func vectorSeries(v promql.Vector) []OutSeries {
	byLabels := map[string]*OutSeries{}
	for _, s := range v {
		key := labelsKey(s.Metric)
		series, ok := byLabels[key]
		if !ok {
			series = &OutSeries{Labels: labelMap(s.Metric)}
			byLabels[key] = series
		}
		series.Points = append(series.Points, Point{T: s.T, V: Float(s.F)})
	}
	out := make([]OutSeries, 0, len(byLabels))
	for _, series := range byLabels {
		sort.Slice(series.Points, func(i, j int) bool { return series.Points[i].T < series.Points[j].T })
		out = append(out, *series)
	}
	sort.Slice(out, func(i, j int) bool {
		return labelsKeyMap(out[i].Labels) < labelsKeyMap(out[j].Labels)
	})
	return out
}

func matrixSeries(m promql.Matrix) []OutSeries {
	out := make([]OutSeries, 0, len(m))
	for _, s := range m {
		series := OutSeries{Labels: labelMap(s.Metric)}
		for _, point := range s.Floats {
			series.Points = append(series.Points, Point{T: point.T, V: Float(point.F)})
		}
		// A native histogram result cannot be compared as a float. Marking the series keeps the
		// comparison from silently ignoring it: the marker appears on one side only.
		if len(s.Histograms) > 0 {
			series.Labels["__histogram__"] = "unsupported"
		}
		out = append(out, series)
	}
	sort.Slice(out, func(i, j int) bool {
		return labelsKeyMap(out[i].Labels) < labelsKeyMap(out[j].Labels)
	})
	return out
}

func labelMap(l labels.Labels) map[string]string {
	out := make(map[string]string, l.Len())
	l.Range(func(label labels.Label) {
		out[label.Name] = label.Value
	})
	return out
}

func labelsKey(l labels.Labels) string {
	return labelsKeyMap(labelMap(l))
}

func labelsKeyMap(m map[string]string) string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	key := ""
	for _, name := range names {
		key += name + "\x00" + m[name] + "\x01"
	}
	return key
}
