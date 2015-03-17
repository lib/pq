package pq

import "sync"

var (
	countMu       sync.Mutex // protects the following
	connCounts    = map[string]int{}
	maxConnCounts = map[string]int{}
)

// ConnCountMax returns the max connection count since last sampled
func ConnCountMax(url string) int {
	countMu.Lock()
	defer countMu.Unlock()

	n := maxConnCounts[url]
	maxConnCounts[url] = connCounts[url]

	return n
}

// ConnCount returns the current connection count
func ConnCount(url string) int {
	countMu.Lock()
	defer countMu.Unlock()

	return connCounts[url]
}

func incrCount(url string) {
	countMu.Lock()
	defer countMu.Unlock()

	connCounts[url]++

	if connCounts[url] > maxConnCounts[url] {
		maxConnCounts[url] = connCounts[url]
	}
}

func decrCount(url string) {
	countMu.Lock()
	defer countMu.Unlock()
	connCounts[url]--
}

func (cn *conn) decrement() {
	decrCount(cn.name)
}
