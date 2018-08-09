package drainer

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
)

const (
	// DefaultCacheSize is the default cache size for every source.
	DefaultCacheSize = 10
)

// MergeItem is the item in Merger
type MergeItem interface {
	GetCommitTs() int64
}

// Merger do merge sort of binlog
type Merger struct {
	sync.RWMutex

	sources map[string]MergeSource

	binlogs map[string]MergeItem

	output chan MergeItem

	newSource      []MergeSource
	removeSource   []string
	pauseSource    []string
	continueSource []string

	// when close, close the output chan once chans is empty
	close int32

	// TODO: save the max and min binlog's ts
	window *DepositWindow
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID     string
	Source chan MergeItem
	Pause  bool
}

// NewMerger create a instance of Merger
func NewMerger(sources ...MergeSource) *Merger {
	m := &Merger{
		sources: make(map[string]MergeSource),
		output:  make(chan MergeItem, 10),
		window:  &DepositWindow{},
	}

	for i := 0; i < len(sources); i++ {
		m.sources[sources[i].ID] = sources[i]
	}

	go m.run()

	return m
}

// Close close the outpu chan when all the source id drained
func (m *Merger) Close() {
	log.Debug("close merger")
	atomic.StoreInt32(&m.close, 1)
}

func (m *Merger) isClosed() bool {
	return atomic.LoadInt32(&m.close) == 1
}

// AddSource add a source to Merger
func (m *Merger) AddSource(source MergeSource) {
	m.Lock()
	if _, ok := m.sources[source.ID]; !ok {
		m.newSource = append(m.newSource, source)
	}
	m.Unlock()
}

// RemoveSource remove a source from Merger
func (m *Merger) RemoveSource(sourceID string) {
	m.Lock()
	if _, ok := m.sources[sourceID]; ok {
		m.removeSource = append(m.removeSource, sourceID)
	}
	m.Unlock()
}

func (m *Merger) updateSource() {
	m.Lock()
	defer m.Unlock()

	// add new source
	for _, source := range m.newSource {
		m.sources[source.ID] = source
		log.Infof("merger add source %s", source.ID)
	}
	m.newSource = m.newSource[:0]

	// remove source
	for _, sourceID := range m.removeSource {
		delete(m.sources, sourceID)
		log.Infof("merger remove source %s", sourceID)
	}
	m.removeSource = m.removeSource[:0]
}

func (m *Merger) run() {
	defer close(m.output)

	var lastTS int64 = math.MinInt64
	for {
		if m.isClosed() {
			return
		}

		m.updateSource()

		skip := false
		for sourceID, source := range m.sources {
			if _, ok := m.binlogs[sourceID]; ok {
				continue
			}

			binlog, ok := <-source.Source
			if ok {
				m.binlogs[sourceID] = binlog
			} else {
				// the source is closing.
				log.Warnf("can't read binlog from pump %s", sourceID)
				skip = true
			}
		}

		if skip {
			// can't get binlog from all source, so can't run merge sort.
			// maybe the source is offline, and then collector will remove this pump in this case.
			// or meet some error, the pump's ctx is done, and drainer will exit.
			// so just wait a second and continue.
			time.Sleep(time.Second)
			continue
		}

		var minBinlog MergeItem
		var minID string

		for sourceID, binlog := range m.binlogs {
			if minBinlog == nil || binlog.GetCommitTs() < minBinlog.GetCommitTs() {
				minBinlog = binlog
				minID = sourceID
			}
		}

		if minBinlog == nil {
			continue
		}

		if minBinlog.GetCommitTs() <= lastTS {
			log.Errorf("binlog's commit ts is %d, and is greater than the last ts %d", minBinlog.GetCommitTs(), lastTS)
			continue
		}

		m.output <- minBinlog
		delete(m.binlogs, minID)
		lastTS = minBinlog.GetCommitTs()
	}
}

// Output get the output chan of binlog
func (m *Merger) Output() chan MergeItem {
	return m.output
}
