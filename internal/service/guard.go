package service

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/Bennybl/session-handler/internal/session"
)

type MutationGuard interface {
	Do(context.Context, session.SessionKey, func(context.Context) error) error
}

type NoopMutationGuard struct{}

func (NoopMutationGuard) Do(ctx context.Context, _ session.SessionKey, fn func(context.Context) error) error {
	return fn(ctx)
}

// StripedMutationGuard is a same-process fallback only. It does not coordinate
// writers in other processes.
type StripedMutationGuard struct {
	stripes []sync.Mutex
}

func NewStripedMutationGuard(count int) (*StripedMutationGuard, error) {
	if count <= 0 {
		return nil, fmt.Errorf("%w: mutation guard stripe count must be positive", session.ErrInvalidInput)
	}
	return &StripedMutationGuard{stripes: make([]sync.Mutex, count)}, nil
}

func (g *StripedMutationGuard) Do(ctx context.Context, key session.SessionKey, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stripe := &g.stripes[HashSessionKey(key)%uint64(len(g.stripes))]
	stripe.Lock()
	defer stripe.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx)
}

func HashSessionKey(key session.SessionKey) uint64 {
	hash := fnv.New64a()
	for _, field := range []string{key.TenantID, key.Username, key.IP} {
		length := uint64(len(field))
		var prefix [8]byte
		for index := range prefix {
			prefix[index] = byte(length >> (8 * index))
		}
		_, _ = hash.Write(prefix[:])
		_, _ = hash.Write([]byte(field))
	}
	return hash.Sum64()
}
