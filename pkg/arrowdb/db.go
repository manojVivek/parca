package arrowdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/parca-dev/parca/pkg/storage"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	//tenantCol = iota
	//labelSetIDCol
	stackTraceIDCol = iota
	//timeStampCol
	valueCol
)

const (
	timestampMeta = "ts"
)

var schemaFields = []arrow.Field{
	//{Name: "tenant", Type: arrow.BinaryTypes.String},
	//{Name: "labelsetID", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "stackTraceID", Type: arrow.BinaryTypes.String},
	//{Name: "timestamp", Type: arrow.FixedWidthTypes.Time64us},
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}

// DB is an in memory arrow db for profile data
type DB struct {
	*memory.GoAllocator
	*arrow.Schema

	// recordList is the list of records present in the db
	// canonically a record is a profile (TODO?)
	recordList []array.Record
}

// NewArrowDB returns a new arrow db
func NewArrowDB() *DB {
	return &DB{
		GoAllocator: memory.NewGoAllocator(),
		Schema:      arrow.NewSchema(schemaFields, nil), // no metadata (TODO)
	}
}

func (db *DB) String() string {
	tbl := array.NewTableFromRecords(db.Schema, db.recordList)
	defer tbl.Release()

	tr := array.NewTableReader(tbl, -1)
	defer tr.Release()

	var s string
	for tr.Next() {
		rec := tr.Record()
		for i, col := range rec.Columns() {
			s = fmt.Sprintf("%v%q: %v\n", s, rec.ColumnName(i), col)
		}
	}

	return s
}

// Appender implements the storage.Appender interface
func (db *DB) Appender(ctx context.Context, lset labels.Labels) (storage.Appender, error) {
	return &appender{
		lsetID: lset.Hash(),
		db:     db,
	}, nil
}

type appender struct {
	lsetID uint64
	db     *DB
}

func (a *appender) Append(ctx context.Context, p *storage.Profile) error {
	panic("unimplemented")
}

// AppendFlat implements the Appender interface
func (a *appender) AppendFlat(ctx context.Context, p *storage.FlatProfile) error {
	//tenant := "tenant-placeholder"

	// Create a record builder for the profile
	md := arrow.MetadataFrom(map[string]string{
		"PeriodType":  fmt.Sprintf("%v", p.Meta.PeriodType),
		"SampleType":  fmt.Sprintf("%v", p.Meta.SampleType),
		timestampMeta: fmt.Sprintf("%v", p.Meta.Timestamp),
		"Duration":    fmt.Sprintf("%v", p.Meta.Duration),
		"Period":      fmt.Sprintf("%v", p.Meta.Period),
		"LabelsetID":  fmt.Sprintf("%v", a.lsetID),
	})
	b := array.NewRecordBuilder(a.db, arrow.NewSchema(schemaFields, &md))
	defer b.Release()

	// Iterate over all samples adding them to the record
	for id, s := range p.Samples() {

		// Append tenant
		//b.Field(tenantCol).(*array.StringBuilder).Append(tenant)
		//b.Field(labelSetIDCol).(*array.Uint64Builder).Append(a.lsetID)
		b.Field(stackTraceIDCol).(*array.StringBuilder).Append(id)
		//b.Field(timeStampCol).(*array.Time64Builder).Append(arrow.Time64(p.Meta.Timestamp))
		b.Field(valueCol).(*array.Int64Builder).Append(s.Value)
	}

	// Create and store the record
	rec := b.NewRecord()
	a.db.recordList = append(a.db.recordList, rec)

	return nil
}

func (db *DB) Querier(ctx context.Context, mint, maxt int64, _ bool) storage.Querier {
	mints, maxts := fmt.Sprintf("%v", mint), fmt.Sprintf("%v", maxt)
	min := sort.Search(len(db.recordList), func(i int) bool {
		ts := db.recordList[i].Schema().Metadata().Values()[db.recordList[i].Schema().Metadata().FindKey(timestampMeta)]
		return ts >= mints
	})
	max := sort.Search(len(db.recordList), func(i int) bool {
		ts := db.recordList[i].Schema().Metadata().Values()[db.recordList[i].Schema().Metadata().FindKey(timestampMeta)]
		return ts >= maxts
	})

	return &querier{
		ctx:    ctx,
		db:     db,
		minIdx: min,
		maxIdx: max,
	}
}

type querier struct {
	ctx    context.Context
	db     *DB
	minIdx int
	maxIdx int
}

func (q *querier) LabelValues(name string, ms ...*labels.Matcher) ([]string, storage.Warnings, error) {
	//TODO implement me
	panic("implement me")
}

func (q *querier) LabelNames(ms ...*labels.Matcher) ([]string, storage.Warnings, error) {
	//TODO implement me
	panic("implement me")
}

func (q *querier) Select(hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	itr, err := array.NewRecordReader(q.db.Schema, q.db.recordList[q.minIdx:q.maxIdx])
	if err != nil {
		return &storage.SliceSeriesSet{}
	}
	defer itr.Release()

	n := 0
	for itr.Next() {
		rec := itr.Record()
		for i, col := range rec.Columns() {
			// Now we actually need to somehow access the data
			fmt.Printf("ref[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}

	return nil
}
