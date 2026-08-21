package query

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

type PreparedQuery[Handler any] struct {
	Filters     []ResolvedFilter[Handler]
	Limit       int
	Fingerprint string
	EvaluatedAt time.Time
	After       *SortKey
}

func Prepare[Handler any](spec session.QuerySpec, storage string, registry *Registry[Handler]) (PreparedQuery[Handler], error) {
	if storage == "" {
		return PreparedQuery[Handler]{}, invalidQuery("storage identifier is required")
	}
	limit := spec.Page.Limit
	if limit == 0 {
		limit = DefaultLimit
	}
	if limit < 1 || limit > MaxLimit {
		return PreparedQuery[Handler]{}, invalidQuery("page limit must be between 1 and %d", MaxLimit)
	}

	filters, err := registry.Resolve(spec.Filters)
	if err != nil {
		return PreparedQuery[Handler]{}, err
	}
	fingerprint, err := fingerprint(filters)
	if err != nil {
		return PreparedQuery[Handler]{}, err
	}

	prepared := PreparedQuery[Handler]{
		Filters:     filters,
		Limit:       limit,
		Fingerprint: fingerprint,
		EvaluatedAt: spec.EvaluatedAt.UTC(),
	}
	if spec.Page.Cursor == "" {
		if spec.EvaluatedAt.IsZero() {
			return PreparedQuery[Handler]{}, invalidQuery("evaluation time is required")
		}
		return prepared, nil
	}

	cursor, err := DecodeCursor(spec.Page.Cursor, storage, fingerprint)
	if err != nil {
		return PreparedQuery[Handler]{}, err
	}
	prepared.EvaluatedAt = cursor.EvaluatedAt
	prepared.After = &cursor.After
	return prepared, nil
}

type fingerprintFilter struct {
	Field    session.Field    `json:"field"`
	Operator session.Operator `json:"operator"`
	Value    any              `json:"value"`
}

func fingerprint[Handler any](filters []ResolvedFilter[Handler]) (string, error) {
	canonical := make([][]byte, 0, len(filters))
	for _, filter := range filters {
		encoded, err := json.Marshal(fingerprintFilter{Field: filter.Field, Operator: filter.Operator, Value: filter.Value})
		if err != nil {
			return "", invalidQuery("cannot fingerprint %s.%s: %v", filter.Field, filter.Operator, err)
		}
		canonical = append(canonical, encoded)
	}
	sort.Slice(canonical, func(i, j int) bool { return string(canonical[i]) < string(canonical[j]) })
	hash := sha256.New()
	for _, encoded := range canonical {
		if _, err := fmt.Fprintf(hash, "%d:", len(encoded)); err != nil {
			return "", invalidQuery("cannot create fingerprint: %v", err)
		}
		_, _ = hash.Write(encoded)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
