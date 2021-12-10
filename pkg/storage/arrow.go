package storage

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/dgraph-io/sroar"
	"github.com/parca-dev/parca/pkg/storage/index"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	//tenantCol = iota
	//labelSetIDCol
	colStacktraceID = iota
	colTimestamp
	colValue
)

const (
	//timestampMeta = "ts"
	labelsetMeta = "ls"
)

var schemaFields = []arrow.Field{
	//{Name: "tenant", Type: arrow.BinaryTypes.String},
	//{Name: "labelsetID", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "stackTraceID", Type: arrow.BinaryTypes.String},
	{Name: "timestamp", Type: arrow.FixedWidthTypes.Time64us},
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}

// ArrowDB is an in memory arrow db for profile data
type ArrowDB struct {
	*memory.GoAllocator
	*arrow.Schema

	// recordList is the list of records present in the db
	// canonically a record is a profile (TODO?)
	recordList []array.Record

	idx *LabelIndex
}

// NewArrowDB returns a new arrow db
func NewArrowDB() *ArrowDB {
	return &ArrowDB{
		GoAllocator: memory.NewGoAllocator(),
		Schema:      arrow.NewSchema(schemaFields, nil), // no metadata (TODO)
		idx: &LabelIndex{
			postings: index.NewMemPostings(),
		},
	}
}

func (db *ArrowDB) String() string {
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
func (db *ArrowDB) Appender(ctx context.Context, lset labels.Labels) (Appender, error) {
	// TODO probably not safe to perform here; as there's no guarantee that anything is ever appended
	// TODO: We need to also store the lset itself to be able to return it in the ArrowSeries later
	db.idx.postings.Add(lset.Hash(), lset)
	return &appender{
		lsetID: lset.Hash(),
		db:     db,
	}, nil
}

type appender struct {
	lsetID uint64
	db     *ArrowDB
}

func (a *appender) Append(ctx context.Context, p *Profile) error {
	panic("unimplemented")
}

// AppendFlat implements the Appender interface
func (a *appender) AppendFlat(ctx context.Context, p *FlatProfile) error {
	//tenant := "tenant-placeholder"

	// Create a record builder for the profile
	md := arrow.MetadataFrom(map[string]string{
		"PeriodType": fmt.Sprintf("%v", p.Meta.PeriodType),
		"SampleType": fmt.Sprintf("%v", p.Meta.SampleType),
		//timestampMeta: fmt.Sprintf("%v", p.Meta.Timestamp),
		"Duration":   fmt.Sprintf("%v", p.Meta.Duration),
		"Period":     fmt.Sprintf("%v", p.Meta.Period),
		labelsetMeta: fmt.Sprintf("%v", a.lsetID),
	})
	b := array.NewRecordBuilder(a.db, arrow.NewSchema(schemaFields, &md))
	defer b.Release()

	// Iterate over all samples adding them to the record
	for id, s := range p.Samples() {
		//b.Field(tenantCol).(*array.StringBuilder).Append(tenant)
		//b.Field(labelSetIDCol).(*array.Uint64Builder).Append(a.lsetID)
		b.Field(colStacktraceID).(*array.StringBuilder).Append(id)
		b.Field(colTimestamp).(*array.Time64Builder).Append(arrow.Time64(p.Meta.Timestamp))
		b.Field(colValue).(*array.Int64Builder).Append(s.Value)
	}

	// Create and store the record
	rec := b.NewRecord()
	a.db.recordList = append(a.db.recordList, rec)

	return nil
}

func (db *ArrowDB) Querier(ctx context.Context, mint, maxt int64, _ bool) Querier {
	min := sort.Search(len(db.recordList), func(i int) bool {
		ts := array.NewInt64Data(db.recordList[i].Column(colTimestamp).Data()).Value(0)
		return ts >= mint
	})
	max := sort.Search(len(db.recordList), func(i int) bool {
		ts := array.NewInt64Data(db.recordList[i].Column(colTimestamp).Data()).Value(0)
		return ts >= maxt
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
	db     *ArrowDB
	minIdx int
	maxIdx int
}

func (q *querier) LabelValues(name string, ms ...*labels.Matcher) ([]string, Warnings, error) {
	//TODO implement me
	panic("implement me")
}

func (q *querier) LabelNames(ms ...*labels.Matcher) ([]string, Warnings, error) {
	//TODO implement me
	panic("implement me")
}

// Select will obtain a set of postings from the label index based on the given label matchers.
// Using those postings it will select a set of stack traces from records that match those postings
func (q *querier) Select(hints *SelectHints, ms ...*labels.Matcher) SeriesSet {
	postings, err := PostingsForMatchers(q.db.idx, ms...)
	if err != nil {
		return &SliceSeriesSet{}
	}

	records := make(map[uint64][]array.Record, postings.GetCardinality())

	// TODO: This might not be the best runtime.
	// Maybe arrow has another way to find records by labelset?
	for _, r := range q.db.recordList[q.minIdx:q.maxIdx] {
		s := r.Schema().Metadata().Values()[r.Schema().Metadata().FindKey(labelsetMeta)]
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return &SliceSeriesSet{}
		}
		if postings.Contains(v) {
			records[v] = append(records[v], r)
		}
	}

	ss := make([]Series, 0, postings.GetCardinality())

	for _, rs := range records {
		ss = append(ss, &ArrowSeries{
			schema:  q.db.Schema,
			meta:    q.db.Metadata(),
			records: rs,
			//mint:    q.mint, // TODO: Pass these on via querier
			//maxt:    q.maxt,
		})
	}

	return &SliceSeriesSet{
		series: ss,
		i:      -1,
	}
}

// traceIDFromMatchers finds the set of trace IDs that satisfy the label matchers
func (q *querier) traceIDFromMatchers(ms ...*labels.Matcher) *sroar.Bitmap {
	return nil
}

type ArrowSeries struct {
	schema  *arrow.Schema
	meta    arrow.Metadata
	records []array.Record
}

func (as *ArrowSeries) Labels() labels.Labels {
	// TODO: Return actual lset
	return labels.Labels{}
}

func (as *ArrowSeries) Iterator() ProfileSeriesIterator {
	table := array.NewTableFromRecords(as.schema, as.records) // TODO: Release it somewhere
	reader := array.NewTableReader(table, -1)                 // TODO: Release it somewhere
	return &ArrowSeriesIterator{
		err:    nil,
		meta:   as.meta,
		table:  table,
		reader: reader,
	}
}

type ArrowSeriesIterator struct {
	err error

	meta   arrow.Metadata
	table  array.Table
	reader *array.TableReader
}

func (it ArrowSeriesIterator) Err() error {
	return it.err
}

func (it ArrowSeriesIterator) Next() bool {
	return it.reader.Next()
}

func (it ArrowSeriesIterator) At() InstantProfile {
	r := it.reader.Record()
	s := array.NewStringData(r.Column(colStacktraceID).Data())
	t := array.NewInt64Data(r.Column(colTimestamp).Data())
	d := array.NewInt64Data(r.Column(colValue).Data())

	samples := map[string]*Sample{}
	for i := 0; i < r.Column(colStacktraceID).Len(); i++ {
		samples[s.Value(i)] = &Sample{
			Value: d.Value(i),
		}
	}

	return &FlatProfile{
		Meta: InstantProfileMeta{
			PeriodType: ValueType{},
			SampleType: ValueType{},
			Timestamp:  t.Value(0), // TODO: We store this len(samples) time but only ever want [0]
			Duration:   0,
			Period:     0,
		},
		samples: samples,
	}
}
