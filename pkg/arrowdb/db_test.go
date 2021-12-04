package arrowdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/google/pprof/profile"
	"github.com/parca-dev/parca/pkg/storage"
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
	tracer := trace.NewNoopTracerProvider().Tracer("")

	ms := metastore.NewBadgerMetastore(
		logger,
		prometheus.NewRegistry(),
		tracer,
		metastore.NewRandomUUIDGenerator(),
	)

	b, err := ioutil.ReadFile("../storage/testdata/profile1.pb.gz")
	require.NoError(t, err)

	p, err := profile.Parse(bytes.NewBuffer(b))
	require.NoError(t, err)

	fp, err := storage.FlatProfileFromPprof(ctx, logger, ms, p, 0)
	require.NoError(t, err)

	db := NewArrowDB()
	appender, _ := db.Appender(ctx, labels.Labels{
		{
			Name:  "__name__",
			Value: "allocs",
		},
	})

	for i := 0; i < 3; i++ {
		appender.AppendFlat(ctx, fp)
	}

	// Printout database
	fmt.Println(db)
}
