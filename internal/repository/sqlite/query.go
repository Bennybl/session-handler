package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sharedquery "github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/session"
)

type predicate func(*parameters, any) string

type parameters struct{ values []any }

func (p *parameters) add(value any) string {
	p.values = append(p.values, value)
	return "?"
}

var predicateRegistry = mustPredicateRegistry()

func mustPredicateRegistry() *sharedquery.Registry[predicate] {
	entries := []sharedquery.Entry[predicate]{
		sessionEntry("sessionId", "eq", sharedquery.ValueUUID, textEqual("s.id")),
		sessionEntry("sessionId", "in", sharedquery.ValueUUIDList, textIn("s.id")),
		sessionEntry("tenantId", "eq", sharedquery.ValueString, textEqual("s.tenant_id")),
		sessionEntry("tenantId", "in", sharedquery.ValueStringList, textIn("s.tenant_id")),
		sessionEntry("username", "eq", sharedquery.ValueString, textEqual("s.username")),
		sessionEntry("username", "in", sharedquery.ValueStringList, textIn("s.username")),
		sessionEntry("ip", "eq", sharedquery.ValueIP, textEqual("s.ip")),
		sessionEntry("ip", "in", sharedquery.ValueIPList, textIn("s.ip")),
		stateEntry("tags", "containsAny", sharedquery.ValueStringList, tagsAny),
		stateEntry("tags", "containsAll", sharedquery.ValueStringList, tagsAll),
		stateEntry("activity", "at", sharedquery.ValueTimestamp, activityAt),
		stateEntry("activity", "overlaps", sharedquery.ValueInterval, activityOverlaps),
		sessionEntry("loginTime", "eq", sharedquery.ValueTimestamp, timeCompare("=")),
		sessionEntry("loginTime", "gt", sharedquery.ValueTimestamp, timeCompare(">")),
		sessionEntry("loginTime", "gte", sharedquery.ValueTimestamp, timeCompare(">=")),
		sessionEntry("loginTime", "lt", sharedquery.ValueTimestamp, timeCompare("<")),
		sessionEntry("loginTime", "lte", sharedquery.ValueTimestamp, timeCompare("<=")),
		sessionEntry("loginTime", "between", sharedquery.ValueInterval, loginBetween),
	}
	registry, err := sharedquery.NewRegistry(entries)
	if err != nil {
		panic(err)
	}
	return registry
}

func sessionEntry(field, operator string, kind sharedquery.ValueKind, handler predicate) sharedquery.Entry[predicate] {
	return sharedquery.Entry[predicate]{Field: session.Field(field), Operator: session.Operator(operator), Scope: sharedquery.ScopeSession, ValueKind: kind, Handler: handler}
}

func stateEntry(field, operator string, kind sharedquery.ValueKind, handler predicate) sharedquery.Entry[predicate] {
	return sharedquery.Entry[predicate]{Field: session.Field(field), Operator: session.Operator(operator), Scope: sharedquery.ScopeState, ValueKind: kind, Handler: handler}
}

func textEqual(column string) predicate {
	return func(p *parameters, value any) string { return column + " = " + p.add(value) }
}

func textIn(column string) predicate {
	return func(p *parameters, value any) string {
		values := value.([]string)
		marks := make([]string, len(values))
		for index, item := range values {
			marks[index] = p.add(item)
		}
		return column + " IN (" + strings.Join(marks, ", ") + ")"
	}
}

func timeCompare(operator string) predicate {
	return func(p *parameters, value any) string {
		return "s.login_at_ns " + operator + " " + p.add(toNanos(value.(time.Time)))
	}
}

func loginBetween(p *parameters, raw any) string {
	value := raw.(sharedquery.IntervalValue)
	parts := make([]string, 0, 2)
	if value.From != nil {
		parts = append(parts, "s.login_at_ns >= "+p.add(toNanos(*value.From)))
	}
	if value.To != nil {
		parts = append(parts, "s.login_at_ns < "+p.add(toNanos(*value.To)))
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

func activityAt(p *parameters, raw any) string {
	at := p.add(toNanos(raw.(time.Time)))
	atAgain := p.add(toNanos(raw.(time.Time)))
	return "(ss.valid_from_ns <= " + at + " AND (ss.valid_to_ns IS NULL OR " + atAgain + " < ss.valid_to_ns))"
}

func activityOverlaps(p *parameters, raw any) string {
	value := raw.(sharedquery.IntervalValue)
	parts := make([]string, 0, 2)
	if value.From != nil {
		parts = append(parts, "(ss.valid_to_ns IS NULL OR "+p.add(toNanos(*value.From))+" < ss.valid_to_ns)")
	}
	if value.To != nil {
		parts = append(parts, "ss.valid_from_ns < "+p.add(toNanos(*value.To)))
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

func tagsAny(p *parameters, raw any) string {
	values := raw.([]string)
	marks := make([]string, len(values))
	for index, value := range values {
		marks[index] = p.add(value)
	}
	return "EXISTS (SELECT 1 FROM session_state_tags sat WHERE sat.state_id = ss.id AND sat.tag IN (" + strings.Join(marks, ", ") + "))"
}

func tagsAll(p *parameters, raw any) string {
	values := raw.([]string)
	marks := make([]string, len(values))
	for index, value := range values {
		marks[index] = p.add(value)
	}
	count := p.add(len(values))
	return "(SELECT count(DISTINCT sat.tag) FROM session_state_tags sat WHERE sat.state_id = ss.id AND sat.tag IN (" + strings.Join(marks, ", ") + ")) = " + count
}

func (r *Repository) query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error) {
	prepared, err := sharedquery.Prepare(spec, storageID, predicateRegistry)
	if err != nil {
		return session.QueryResult{}, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return session.QueryResult{}, fmt.Errorf("begin SQLite query: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	ids, more, err := queryIDs(ctx, tx, prepared)
	if err != nil {
		return session.QueryResult{}, err
	}
	if more {
		ids = ids[:prepared.Limit]
	}
	values, err := loadSessions(ctx, tx, ids)
	if err != nil {
		return session.QueryResult{}, err
	}
	result := session.QueryResult{Sessions: values}
	if more {
		result.NextCursor, err = sharedquery.EncodeCursor(sharedquery.Cursor{
			Storage: storageID, Fingerprint: prepared.Fingerprint, EvaluatedAt: prepared.EvaluatedAt,
			After: sharedquery.SortKeyFor(values[len(values)-1]),
		})
	}
	if err != nil {
		return session.QueryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return session.QueryResult{}, fmt.Errorf("commit SQLite query: %w", err)
	}
	return result, nil
}

func queryIDs(ctx context.Context, tx *sql.Tx, prepared sharedquery.PreparedQuery[predicate]) ([]string, bool, error) {
	sessionParameters := &parameters{}
	stateParameters := &parameters{}
	sessionPredicates := []string{"1 = 1"}
	statePredicates := make([]string, 0)
	hasActivity := false
	for _, filter := range prepared.Filters {
		if filter.Scope == sharedquery.ScopeState {
			statePredicates = append(statePredicates, filter.Handler(stateParameters, filter.Value))
			if filter.Field == session.Field("activity") {
				hasActivity = true
			}
		} else {
			sessionPredicates = append(sessionPredicates, filter.Handler(sessionParameters, filter.Value))
		}
	}
	if !hasActivity {
		statePredicates = append(statePredicates, activityAt(stateParameters, prepared.EvaluatedAt))
	}
	sessionPredicates = append(sessionPredicates, "EXISTS (SELECT 1 FROM session_states ss WHERE ss.session_id = s.id AND "+strings.Join(statePredicates, " AND ")+")")
	keysetParameters := &parameters{}
	if prepared.After != nil {
		sessionPredicates = append(sessionPredicates, keysetPredicate(keysetParameters, *prepared.After))
	}
	values := append(sessionParameters.values, stateParameters.values...)
	values = append(values, keysetParameters.values...)
	values = append(values, prepared.Limit+1)
	statement := `SELECT s.id FROM sessions s WHERE ` + strings.Join(sessionPredicates, " AND ") + `
		ORDER BY s.tenant_id COLLATE BINARY, s.username COLLATE BINARY, s.ip COLLATE BINARY, s.login_at_ns, s.id LIMIT ?`
	rows, err := tx.QueryContext(ctx, statement, values...)
	if err != nil {
		return nil, false, fmt.Errorf("query SQLite session IDs: %w", err)
	}
	ids := make([]string, 0, prepared.Limit+1)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, false, fmt.Errorf("scan SQLite session ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, false, fmt.Errorf("read SQLite session IDs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, false, fmt.Errorf("close SQLite session IDs: %w", err)
	}
	return ids, len(ids) > prepared.Limit, nil
}

func keysetPredicate(p *parameters, after sharedquery.SortKey) string {
	return `(s.tenant_id COLLATE BINARY > ` + p.add(after.TenantID) + ` COLLATE BINARY
		OR (s.tenant_id = ` + p.add(after.TenantID) + ` AND s.username COLLATE BINARY > ` + p.add(after.Username) + ` COLLATE BINARY)
		OR (s.tenant_id = ` + p.add(after.TenantID) + ` AND s.username = ` + p.add(after.Username) + ` AND s.ip COLLATE BINARY > ` + p.add(after.IP) + ` COLLATE BINARY)
		OR (s.tenant_id = ` + p.add(after.TenantID) + ` AND s.username = ` + p.add(after.Username) + ` AND s.ip = ` + p.add(after.IP) + ` AND s.login_at_ns > ` + p.add(toNanos(after.LoginAt)) + `)
		OR (s.tenant_id = ` + p.add(after.TenantID) + ` AND s.username = ` + p.add(after.Username) + ` AND s.ip = ` + p.add(after.IP) + ` AND s.login_at_ns = ` + p.add(toNanos(after.LoginAt)) + ` AND s.id > ` + p.add(after.SessionID) + `))`
}

func placeholders(count int) string { return strings.TrimSuffix(strings.Repeat("?,", count), ",") }

func loadSessions(ctx context.Context, tx *sql.Tx, ids []string) ([]session.Session, error) {
	if len(ids) == 0 {
		return []session.Session{}, nil
	}
	args := make([]any, len(ids))
	for index, id := range ids {
		args[index] = id
	}
	rows, err := tx.QueryContext(ctx, `SELECT s.id, s.tenant_id, s.username, s.ip, s.login_at_ns, s.logout_at_ns,
		s.last_event_id, ss.id, ss.valid_from_ns, ss.valid_to_ns FROM sessions s
		JOIN session_states ss ON ss.session_id = s.id WHERE s.id IN (`+placeholders(len(ids))+`)
		ORDER BY ss.session_id, ss.valid_from_ns, ss.id`, args...)
	if err != nil {
		return nil, fmt.Errorf("load SQLite sessions: %w", err)
	}
	byID := make(map[string]*session.Session, len(ids))
	type stateLocation struct {
		sessionID string
		index     int
	}
	stateOwners := make(map[int64]stateLocation)
	for rows.Next() {
		var id, tenant, username, ip, eventID string
		var loginNS, stateID, fromNS int64
		var logoutNS, toNS sql.NullInt64
		if err := rows.Scan(&id, &tenant, &username, &ip, &loginNS, &logoutNS, &eventID, &stateID, &fromNS, &toNS); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan SQLite session: %w", err)
		}
		value := byID[id]
		if value == nil {
			value = &session.Session{ID: id, Key: session.SessionKey{TenantID: tenant, Username: username, IP: ip}, LoginAt: fromNanos(loginNS), LastEventID: eventID}
			if logoutNS.Valid {
				closed := fromNanos(logoutNS.Int64)
				value.LogoutAt = &closed
			}
			byID[id] = value
		}
		state := session.SessionState{ValidFrom: fromNanos(fromNS)}
		if toNS.Valid {
			closed := fromNanos(toNS.Int64)
			state.ValidTo = &closed
		}
		value.States = append(value.States, state)
		stateOwners[stateID] = stateLocation{sessionID: id, index: len(value.States) - 1}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read SQLite sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite sessions: %w", err)
	}
	tagRows, err := tx.QueryContext(ctx, `SELECT state_id, tag FROM session_state_tags WHERE state_id IN
		(SELECT id FROM session_states WHERE session_id IN (`+placeholders(len(ids))+`)) ORDER BY state_id, tag`, args...)
	if err != nil {
		return nil, fmt.Errorf("load SQLite tags: %w", err)
	}
	for tagRows.Next() {
		var stateID int64
		var tag string
		if err := tagRows.Scan(&stateID, &tag); err != nil {
			_ = tagRows.Close()
			return nil, fmt.Errorf("scan SQLite tag: %w", err)
		}
		if location, exists := stateOwners[stateID]; exists {
			value := byID[location.sessionID]
			value.States[location.index].Tags = append(value.States[location.index].Tags, tag)
		}
	}
	if err := tagRows.Err(); err != nil {
		_ = tagRows.Close()
		return nil, fmt.Errorf("read SQLite tags: %w", err)
	}
	if err := tagRows.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite tags: %w", err)
	}
	values := make([]session.Session, len(ids))
	for index, id := range ids {
		if byID[id] == nil {
			return nil, fmt.Errorf("load SQLite session %s: no rows returned", id)
		}
		values[index] = *byID[id]
	}
	return values, nil
}
