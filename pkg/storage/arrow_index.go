package arrowdb

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/dgraph-io/sroar"
	"github.com/parca-dev/parca/pkg/storage"
	"github.com/parca-dev/parca/pkg/storage/index"
	"github.com/prometheus/prometheus/pkg/labels"
)

// LabelIndex is a reverse index to lookup labelset ID's
// LabelIndex implements the storage.IndexReader interface
type LabelIndex struct {
	postings *index.MemPostings
}

// Close noop
func (l *LabelIndex) Close() error {
	return nil
}

// Postings returns the postings list iterator for the label pairs.
func (l *LabelIndex) Postings(name string, values ...string) (*sroar.Bitmap, error) {
	if len(values) == 1 {
		return l.postings.Get(name, values[0]), nil
	}

	b := sroar.NewBitmap()
	for _, value := range values {
		// union all postings
		b.Or(l.postings.Get(name, value))
	}

	if b.GetCardinality() == 0 {
		b.Set(math.MaxUint64) // err postings
	}

	return b, nil
}

// LabelValues returns all values that satisfy the matchers
func (l *LabelIndex) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		return l.postings.LabelValues(name), nil
	}

	return labelValuesWithMatchers(l, name, matchers...)
}

func (l *LabelIndex) LabelValueFor(id uint64, label string) (string, error) {
	panic("unimplemented")
}

// LabelNamesFor returns all the label names for the series referred to by IDs.
// The names returned are sorted.
func (l *LabelIndex) LabelNamesFor(ids ...uint64) ([]string, error) {
	panic("unimplemented")
}

// LabelNames returns all the unique label names present in the head
// that are within the time range mint to maxt.
func (l *LabelIndex) LabelNames(matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		labelNames := l.postings.LabelNames()

		sort.Strings(labelNames)
		return labelNames, nil
	}

	return labelNamesWithMatchers(l, matchers...)
}

func labelValuesWithMatchers(r storage.IndexReader, name string, matchers ...*labels.Matcher) ([]string, error) {
	// We're only interested in metrics which have the label <name>.
	requireLabel, err := labels.NewMatcher(labels.MatchNotEqual, name, "")
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate label matcher: %w", err)
	}

	bm, err := storage.PostingsForMatchers(r, append(matchers, requireLabel)...)
	if err != nil {
		return nil, err
	}

	dedupe := map[string]interface{}{}

	it := bm.NewIterator()
	for {
		id := it.Next()
		if id == 0 {
			break
		}

		v, err := r.LabelValueFor(id, name)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, err
		}
		dedupe[v] = nil
	}

	values := make([]string, 0, len(dedupe))
	for value := range dedupe {
		values = append(values, value)
	}

	return values, nil
}

func labelNamesWithMatchers(r storage.IndexReader, matchers ...*labels.Matcher) ([]string, error) {
	p, err := storage.PostingsForMatchers(r, matchers...)
	if err != nil {
		return nil, err
	}

	return r.LabelNamesFor(p.ToArray()...)
}
