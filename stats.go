package lru

import (
	"time"
)

// Stats contains a number of stats pertaining to an LRU.
type Stats struct {
	StartTime    time.Time     `json:"start_time"`
	Uptime       time.Duration `json:"uptime"`
	Hits         int64         `json:"hits"`
	Misses       int64         `json:"misses"`
	GetBytes     int64         `json:"get_bytes"`
	Puts         int64         `json:"puts"`
	PutBytes     int64         `json:"put_bytes"`
	Evicted      int64         `json:"evicted"`
	EvictedBytes int64         `json:"evicted_bytes"`
	Size         int64         `json:"size"`
	Capacity     int64         `json:"capacity"`
	NumItems     int64         `json:"num_items"`
}

// Stats returns the current stats for the given LRU.
func (l *LRU) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.getStats()
}

// ResetStats resets all stats to their initial state and returns the LRU's
// stats as they were immediately before being reset.
func (l *LRU) ResetStats() Stats {
	var stats Stats
	l.mu.Lock()
	stats = l.getStats()
	l.sTime = time.Now().UTC()
	l.hits = 0
	l.misses = 0
	l.bget = 0
	l.puts = 0
	l.bput = 0
	l.evicted = 0
	l.bevict = 0
	l.mu.Unlock()
	return stats
}

// getStats returns the current LRU stats.
// Note: this method should only be called when the LRU mutex is locked!
func (l *LRU) getStats() Stats {
	return Stats{
		StartTime:    l.sTime,
		Uptime:       time.Since(l.sTime),
		Hits:         l.hits,
		Misses:       l.misses,
		GetBytes:     l.bget,
		Puts:         l.puts,
		PutBytes:     l.bput,
		Evicted:      l.evicted,
		EvictedBytes: l.bevict,
		Size:         l.lru.size(),
		Capacity:     l.cap,
		NumItems:     l.lru.len(),
	}
}
