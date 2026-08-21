package query

import (
	"encoding/hex"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

type Scope string

const (
	ScopeSession Scope = "session"
	ScopeState   Scope = "state"
)

type ValueKind string

const (
	ValueString     ValueKind = "string"
	ValueStringList ValueKind = "string_list"
	ValueIP         ValueKind = "ip"
	ValueIPList     ValueKind = "ip_list"
	ValueUUID       ValueKind = "uuid"
	ValueUUIDList   ValueKind = "uuid_list"
	ValueTimestamp  ValueKind = "timestamp"
	ValueInterval   ValueKind = "interval"
)

type IntervalValue struct {
	From *time.Time `json:"from,omitempty"`
	To   *time.Time `json:"to,omitempty"`
}

type Entry[Handler any] struct {
	Field     session.Field
	Operator  session.Operator
	Scope     Scope
	ValueKind ValueKind
	Handler   Handler
}

type ResolvedFilter[Handler any] struct {
	Field    session.Field
	Operator session.Operator
	Scope    Scope
	Value    any
	Handler  Handler
}

type registryKey struct {
	field    session.Field
	operator session.Operator
}

type Registry[Handler any] struct {
	entries map[registryKey]Entry[Handler]
}

func NewRegistry[Handler any](entries []Entry[Handler]) (*Registry[Handler], error) {
	registry := &Registry[Handler]{entries: make(map[registryKey]Entry[Handler], len(entries))}
	for _, entry := range entries {
		if entry.Field == "" {
			return nil, invalidQuery("registry field is required")
		}
		if entry.Operator == "" {
			return nil, invalidQuery("registry operator is required for field %q", entry.Field)
		}
		if entry.Scope != ScopeSession && entry.Scope != ScopeState {
			return nil, invalidQuery("invalid scope %q for %s.%s", entry.Scope, entry.Field, entry.Operator)
		}
		if !validValueKind(entry.ValueKind) {
			return nil, invalidQuery("invalid value kind %q for %s.%s", entry.ValueKind, entry.Field, entry.Operator)
		}

		key := registryKey{field: entry.Field, operator: entry.Operator}
		if _, exists := registry.entries[key]; exists {
			return nil, invalidQuery("duplicate registry entry for %s.%s", entry.Field, entry.Operator)
		}
		registry.entries[key] = entry
	}
	return registry, nil
}

func (r *Registry[Handler]) Resolve(filters []session.Filter) ([]ResolvedFilter[Handler], error) {
	if r == nil {
		return nil, invalidQuery("query registry is nil")
	}
	resolved := make([]ResolvedFilter[Handler], 0, len(filters))
	for _, filter := range filters {
		entry, exists := r.entries[registryKey{field: filter.Field, operator: filter.Operator}]
		if !exists {
			return nil, invalidQuery("unsupported filter %s.%s", filter.Field, filter.Operator)
		}
		value, err := normalizeValue(entry.ValueKind, filter.Value)
		if err != nil {
			return nil, invalidQuery("invalid value for %s.%s: %v", filter.Field, filter.Operator, err)
		}
		resolved = append(resolved, ResolvedFilter[Handler]{
			Field:    filter.Field,
			Operator: filter.Operator,
			Scope:    entry.Scope,
			Value:    value,
			Handler:  entry.Handler,
		})
	}
	return resolved, nil
}

func validValueKind(kind ValueKind) bool {
	switch kind {
	case ValueString, ValueStringList, ValueIP, ValueIPList, ValueUUID, ValueUUIDList, ValueTimestamp, ValueInterval:
		return true
	default:
		return false
	}
}

func normalizeValue(kind ValueKind, value any) (any, error) {
	switch kind {
	case ValueString:
		return normalizeString(value)
	case ValueStringList:
		return normalizeStringList(value, normalizeString)
	case ValueIP:
		return normalizeIP(value)
	case ValueIPList:
		return normalizeStringList(value, normalizeIP)
	case ValueUUID:
		return normalizeUUID(value)
	case ValueUUIDList:
		return normalizeStringList(value, normalizeUUID)
	case ValueTimestamp:
		return normalizeTimestamp(value)
	case ValueInterval:
		return normalizeInterval(value)
	default:
		return nil, fmt.Errorf("unsupported value kind %q", kind)
	}
}

type stringNormalizer func(any) (any, error)

func normalizeString(value any) (any, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("expected a non-empty string")
	}
	return text, nil
}

func normalizeIP(value any) (any, error) {
	text, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected an IP string")
	}
	parsed := net.ParseIP(text)
	if parsed == nil {
		return nil, fmt.Errorf("invalid IP address")
	}
	return parsed.String(), nil
}

func normalizeUUID(value any) (any, error) {
	text, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected a UUID string")
	}
	compact := strings.ReplaceAll(text, "-", "")
	if len(text) != 36 || len(compact) != 32 || text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return nil, fmt.Errorf("invalid UUID")
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return nil, fmt.Errorf("invalid UUID")
	}
	return strings.ToLower(text), nil
}

func normalizeStringList(value any, normalize stringNormalizer) (any, error) {
	var values []any
	switch typed := value.(type) {
	case []string:
		values = make([]any, len(typed))
		for index, item := range typed {
			values[index] = item
		}
	case []any:
		values = typed
	default:
		return nil, fmt.Errorf("expected an array")
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("array cannot be empty")
	}

	unique := make(map[string]struct{}, len(values))
	for _, item := range values {
		normalized, err := normalize(item)
		if err != nil {
			return nil, err
		}
		unique[normalized.(string)] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for item := range unique {
		result = append(result, item)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeTimestamp(value any) (any, error) {
	var timestamp time.Time
	switch typed := value.(type) {
	case time.Time:
		timestamp = typed
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return nil, fmt.Errorf("expected an RFC 3339 timestamp")
		}
		timestamp = parsed
	default:
		return nil, fmt.Errorf("expected an RFC 3339 timestamp")
	}
	if timestamp.IsZero() {
		return nil, fmt.Errorf("timestamp cannot be zero")
	}
	return timestamp.UTC(), nil
}

func normalizeInterval(value any) (any, error) {
	var (
		rawFrom any
		rawTo   any
		hasFrom bool
		hasTo   bool
	)
	switch typed := value.(type) {
	case IntervalValue:
		if typed.From != nil {
			rawFrom = *typed.From
			hasFrom = true
		}
		if typed.To != nil {
			rawTo = *typed.To
			hasTo = true
		}
	case map[string]any:
		for key := range typed {
			if key != "from" && key != "to" {
				return nil, fmt.Errorf("unknown interval field %q", key)
			}
		}
		if raw, exists := typed["from"]; exists && raw != nil {
			rawFrom = raw
			hasFrom = true
		}
		if raw, exists := typed["to"]; exists && raw != nil {
			rawTo = raw
			hasTo = true
		}
	default:
		return nil, fmt.Errorf("expected an interval object")
	}

	if !hasFrom && !hasTo {
		return nil, fmt.Errorf("interval requires from or to")
	}

	var interval IntervalValue
	if hasFrom {
		from, err := normalizeTimestamp(rawFrom)
		if err != nil {
			return nil, fmt.Errorf("invalid from: %w", err)
		}
		normalized := from.(time.Time)
		interval.From = &normalized
	}
	if hasTo {
		to, err := normalizeTimestamp(rawTo)
		if err != nil {
			return nil, fmt.Errorf("invalid to: %w", err)
		}
		normalized := to.(time.Time)
		interval.To = &normalized
	}
	if interval.From != nil && interval.To != nil && !interval.From.Before(*interval.To) {
		return nil, fmt.Errorf("interval from must be before to")
	}
	return interval, nil
}

func invalidQuery(format string, args ...any) error {
	return fmt.Errorf("%w: %s", repository.ErrInvalidQuery, fmt.Sprintf(format, args...))
}
