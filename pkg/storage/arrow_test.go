package storage

import (
	"bytes"
	"io/ioutil"
	"math"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/google/pprof/profile"
	"github.com/parca-dev/parca/pkg/storage/metastore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/context"
)

func TestAppendProfile(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	tracer := trace.NewNoopTracerProvider().Tracer("")

	b1, err := ioutil.ReadFile("testdata/profile1.pb.gz")
	require.NoError(t, err)
	b2, err := ioutil.ReadFile("testdata/profile2.pb.gz")
	require.NoError(t, err)

	p1, err := profile.Parse(bytes.NewBuffer(b1))
	require.NoError(t, err)
	p2, err := profile.Parse(bytes.NewBuffer(b2))
	require.NoError(t, err)

	ms := metastore.NewBadgerMetastore(logger, registry, tracer, metastore.NewRandomUUIDGenerator())

	fp1, err := FlatProfileFromPprof(ctx, logger, ms, p1, 0)
	require.NoError(t, err)
	fp2, err := FlatProfileFromPprof(ctx, logger, ms, p2, 0)
	require.NoError(t, err)

	db := NewArrowDB()
	appender, _ := db.Appender(ctx, labels.Labels{
		{
			Name:  "__name__",
			Value: "allocs",
		},
	})

	require.NoError(t, appender.AppendFlat(ctx, fp1))
	require.NoError(t, appender.AppendFlat(ctx, fp2))

	q := db.Querier(context.Background(), math.MinInt64, math.MaxInt64, false)
	ss := q.Select(nil, &labels.Matcher{
		Type:  labels.MatchEqual,
		Name:  "__name__",
		Value: "allocs",
	}) // select all - for now

	expectedTimestamps := []int64{1626013307085, 1626014267084}
	expectedCumulative := []int64{48, 51}

	var i int
	for ss.Next() {
		s := ss.At()
		it := s.Iterator()
		for it.Next() {
			p := it.At()
			require.Equal(t, expectedTimestamps[i], p.ProfileMeta().Timestamp)

			var cumulative int64
			for _, sample := range p.Samples() {
				cumulative += sample.Value
			}
			require.Equal(t, expectedCumulative[i], cumulative)

			i++
		}
	}
}
