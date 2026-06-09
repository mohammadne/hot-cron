package scheduler

import "sync"

type guard struct {
	mu    sync.Mutex
	locks map[string]struct{}
}

func newGuard() *guard {
	return &guard{
		locks: make(map[string]struct{}),
	}
}

func (g *guard) tryLock(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.locks[key]; exists {
		return false
	}

	g.locks[key] = struct{}{}
	return true
}

func (g *guard) unlock(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.locks, key)
}
