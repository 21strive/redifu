package outbox

import (
	"context"
	"sync"
)

// A read scope remembers the owners whose queue one read has already applied. A cached
// repository method opens one per call (NewReadScope): the indexes, entities and satellites one
// answer is built from then cost one queue check per owner, not one each. The guarantee Serve
// gives — a read that starts after a write committed sees it — holds for the call as a whole,
// because its first check happened after the call started. A scope must never outlive the call
// it was opened for: a later read has to ask the database again.
type readScope struct {
	mu     sync.Mutex
	served map[string]bool
}

type readScopeKey struct{}

// NewReadScope returns ctx carrying a fresh read scope.
func NewReadScope(ctx context.Context) context.Context {
	return context.WithValue(ctx, readScopeKey{}, &readScope{served: map[string]bool{}})
}

func readScopeOf(ctx context.Context) *readScope {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(readScopeKey{}).(*readScope)
	return scope
}

// has reports whether owner's queue was applied in this scope (a "*" serve is kept under its own
// key: it applies only the queues of its view's tables, so it does not stand for every owner).
func (s *readScope) has(owner string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served[owner]
}

func (s *readScope) mark(owner string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.served[owner] = true
	s.mu.Unlock()
}
