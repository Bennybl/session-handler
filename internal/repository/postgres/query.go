package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sharedquery "github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/session"
)

const queryStorageID = "postgres"

type sqlParameters struct {
	values []any
}

func (p *sqlParameters) add(value any) string {
	p.values = append(p.values, value)
	return fmt.Sprintf("$%d", len(p.values))
}

type sqlPredicate func(*sqlParameters, any) string

var sqlPredicateRegistry = mustSQLPredicateRegistry()

func mustSQLPredicateRegistry() *sharedquery.Registry[sqlPredicate] {
	entries := []sharedquery.Entry[sqlPredicate]{
		sqlSessionEntry("sessionId", "eq", sharedquery.ValueUUID, equals("s.id", "uuid")),
		sqlSessionEntry("sessionId", "in", sharedquery.ValueUUIDList, in("s.id", "uuid")),
		sqlSessionEntry("tenantId", "eq", sharedquery.ValueString, equals("s.tenant_id", "text")),
		sqlSessionEntry("tenantId", "in", sharedquery.ValueStringList, in("s.tenant_id", "text")),
		sqlSessionEntry("username", "eq", sharedquery.ValueString, equals("s.username", "text")),
		sqlSessionEntry("username", "in", sharedquery.ValueStringList, in("s.username", "text")),
		sqlSessionEntry("ip", "eq", sharedquery.ValueIP, equals("s.ip", "inet")),
		sqlSessionEntry("ip", "in", sharedquery.ValueIPList, in("s.ip", "inet")),
		sqlStateEntry("tags", "containsAll", sharedquery.ValueStringList, arrayOperator("ss.tags", "@>")),
		sqlStateEntry("tags", "containsAny", sharedquery.ValueStringList, arrayOperator("ss.tags", "&&")),
		sqlStateEntry("activity", "at", sharedquery.ValueTimestamp, timestampRangeContains("ss.activity")),
		sqlStateEntry("activity", "overlaps", sharedquery.ValueInterval, rangeOverlaps("ss.activity")),
		sqlSessionEntry("loginTime", "eq", sharedquery.ValueTimestamp, comparison("s.login_at", "=")),
		sqlSessionEntry("loginTime", "gt", sharedquery.ValueTimestamp, comparison("s.login_at", ">")),
		sqlSessionEntry("loginTime", "gte", sharedquery.ValueTimestamp, comparison("s.login_at", ">=")),
		sqlSessionEntry("loginTime", "lt", sharedquery.ValueTimestamp, comparison("s.login_at", "<")),
		sqlSessionEntry("loginTime", "lte", sharedquery.ValueTimestamp, comparison("s.login_at", "<=")),
		sqlSessionEntry("loginTime", "between", sharedquery.ValueInterval, timestampBetween("s.login_at")),
	}
	registry, err := sharedquery.NewRegistry(entries)
	if err != nil {
		panic(err)
	}
	return registry
}

func sqlSessionEntry(field, operator string, kind sharedquery.ValueKind, handler sqlPredicate) sharedquery.Entry[sqlPredicate] {
	return sharedquery.Entry[sqlPredicate]{
		Field: session.Field(field), Operator: session.Operator(operator), Scope: sharedquery.ScopeSession, ValueKind: kind, Handler: handler,
	}
}

func sqlStateEntry(field, operator string, kind sharedquery.ValueKind, handler sqlPredicate) sharedquery.Entry[sqlPredicate] {
	return sharedquery.Entry[sqlPredicate]{
		Field: session.Field(field), Operator: session.Operator(operator), Scope: sharedquery.ScopeState, ValueKind: kind, Handler: handler,
	}
}

func equals(column, cast string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		return fmt.Sprintf("%s = %s::%s", column, parameters.add(value), cast)
	}
}

func in(column, cast string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		values := value.([]string)
		placeholders := make([]string, len(values))
		for index, item := range values {
			placeholders[index] = parameters.add(item) + "::" + cast
		}
		return fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", "))
	}
}

func arrayOperator(column, operator string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		values := value.([]string)
		placeholders := make([]string, len(values))
		for index, item := range values {
			placeholders[index] = parameters.add(item) + "::text"
		}
		return fmt.Sprintf("%s %s ARRAY[%s]", column, operator, strings.Join(placeholders, ", "))
	}
}

func timestampRangeContains(column string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		return fmt.Sprintf("%s @> %s::timestamptz", column, parameters.add(value))
	}
}

func rangeOverlaps(column string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		interval := value.(sharedquery.IntervalValue)
		return fmt.Sprintf("%s && tstzrange(%s, %s, '[)')", column, intervalBound(parameters, interval.From), intervalBound(parameters, interval.To))
	}
}

func comparison(column, operator string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		return fmt.Sprintf("%s %s %s::timestamptz", column, operator, parameters.add(value))
	}
}

func timestampBetween(column string) sqlPredicate {
	return func(parameters *sqlParameters, value any) string {
		interval := value.(sharedquery.IntervalValue)
		predicates := make([]string, 0, 2)
		if interval.From != nil {
			predicates = append(predicates, fmt.Sprintf("%s >= %s::timestamptz", column, parameters.add(*interval.From)))
		}
		if interval.To != nil {
			predicates = append(predicates, fmt.Sprintf("%s < %s::timestamptz", column, parameters.add(*interval.To)))
		}
		return "(" + strings.Join(predicates, " AND ") + ")"
	}
}

func intervalBound(parameters *sqlParameters, value *time.Time) string {
	if value == nil {
		return "NULL"
	}
	return parameters.add(*value) + "::timestamptz"
}

func (r *Repository) query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error) {
	prepared, err := sharedquery.Prepare(spec, queryStorageID, sqlPredicateRegistry)
	if err != nil {
		return session.QueryResult{}, err
	}
	ids, hasMore, err := r.querySessionIDs(ctx, prepared)
	if err != nil {
		return session.QueryResult{}, err
	}
	if hasMore {
		ids = ids[:prepared.Limit]
	}
	values, err := r.loadSessions(ctx, ids)
	if err != nil {
		return session.QueryResult{}, err
	}
	result := session.QueryResult{Sessions: values}
	if !hasMore {
		return result, nil
	}
	result.NextCursor, err = sharedquery.EncodeCursor(sharedquery.Cursor{
		Storage: queryStorageID, Fingerprint: prepared.Fingerprint, EvaluatedAt: prepared.EvaluatedAt,
		After: sharedquery.SortKeyFor(values[len(values)-1]),
	})
	if err != nil {
		return session.QueryResult{}, err
	}
	return result, nil
}

func (r *Repository) querySessionIDs(ctx context.Context, prepared sharedquery.PreparedQuery[sqlPredicate]) ([]string, bool, error) {
	parameters := &sqlParameters{}
	sessionPredicates := make([]string, 0, len(prepared.Filters)+1)
	statePredicates := make([]string, 0, len(prepared.Filters)+1)
	hasActivity := false
	for _, filter := range prepared.Filters {
		predicate := filter.Handler(parameters, filter.Value)
		if filter.Scope == sharedquery.ScopeState {
			statePredicates = append(statePredicates, predicate)
			if filter.Field == session.Field("activity") {
				hasActivity = true
			}
			continue
		}
		sessionPredicates = append(sessionPredicates, predicate)
	}
	if !hasActivity {
		statePredicates = append(statePredicates, fmt.Sprintf("ss.activity @> %s::timestamptz", parameters.add(prepared.EvaluatedAt)))
	}
	sessionPredicates = append(sessionPredicates, "EXISTS (SELECT 1 FROM session_states ss WHERE ss.session_id = s.id AND "+strings.Join(statePredicates, " AND ")+")")
	if prepared.After != nil {
		sessionPredicates = append(sessionPredicates, keysetPredicate(parameters, *prepared.After))
	}
	limit := parameters.add(prepared.Limit + 1)
	statement := `
		SELECT s.id::text
		FROM sessions s
		WHERE ` + strings.Join(sessionPredicates, " AND ") + `
		ORDER BY s.tenant_id COLLATE "C", s.username COLLATE "C", host(s.ip) COLLATE "C", s.login_at, s.id
		LIMIT ` + limit

	rows, err := r.db.QueryContext(ctx, statement, parameters.values...)
	if err != nil {
		return nil, false, fmt.Errorf("query PostgreSQL session IDs: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0, prepared.Limit+1)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, false, fmt.Errorf("scan PostgreSQL session ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate PostgreSQL session IDs: %w", err)
	}
	return ids, len(ids) > prepared.Limit, nil
}

func keysetPredicate(parameters *sqlParameters, after sharedquery.SortKey) string {
	tenant := parameters.add(after.TenantID)
	username := parameters.add(after.Username)
	ip := parameters.add(after.IP)
	loginAt := parameters.add(after.LoginAt)
	id := parameters.add(after.SessionID)
	return fmt.Sprintf(`(
		s.tenant_id COLLATE "C" > %s::text COLLATE "C"
		OR (s.tenant_id = %s::text AND s.username COLLATE "C" > %s::text COLLATE "C")
		OR (s.tenant_id = %s::text AND s.username = %s::text AND host(s.ip) COLLATE "C" > %s::text COLLATE "C")
		OR (s.tenant_id = %s::text AND s.username = %s::text AND host(s.ip) = %s::text AND s.login_at > %s::timestamptz)
		OR (s.tenant_id = %s::text AND s.username = %s::text AND host(s.ip) = %s::text AND s.login_at = %s::timestamptz AND s.id > %s::uuid)
	)`, tenant, tenant, username, tenant, username, ip, tenant, username, ip, loginAt, tenant, username, ip, loginAt, id)
}

func (r *Repository) loadSessions(ctx context.Context, ids []string) ([]session.Session, error) {
	if len(ids) == 0 {
		return []session.Session{}, nil
	}
	parameters := &sqlParameters{}
	placeholders := make([]string, len(ids))
	for index, id := range ids {
		placeholders[index] = parameters.add(id) + "::uuid"
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT s.id::text, s.tenant_id, s.username, host(s.ip), s.login_at, s.logout_at, s.last_event_id::text,
		       to_json(ss.tags)::text, ss.valid_from, ss.valid_to
		FROM sessions s
		JOIN session_states ss ON ss.session_id = s.id
		WHERE s.id IN (`+strings.Join(placeholders, ", ")+`)
		ORDER BY ss.session_id, ss.valid_from, ss.id
	`, parameters.values...)
	if err != nil {
		return nil, fmt.Errorf("load PostgreSQL sessions: %w", err)
	}
	defer rows.Close()

	byID := make(map[string]*session.Session, len(ids))
	for rows.Next() {
		var (
			id, tenantID, username, ip string
			loginAt                    time.Time
			logoutAt                   sql.NullTime
			eventID                    sql.NullString
			tagsJSON                   string
			validFrom                  time.Time
			validTo                    sql.NullTime
		)
		if err := rows.Scan(&id, &tenantID, &username, &ip, &loginAt, &logoutAt, &eventID, &tagsJSON, &validFrom, &validTo); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL session: %w", err)
		}
		var tags []string
		if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
			return nil, fmt.Errorf("decode PostgreSQL state tags: %w", err)
		}
		value := byID[id]
		if value == nil {
			value = &session.Session{ID: id, Key: session.SessionKey{TenantID: tenantID, Username: username, IP: ip}, LoginAt: loginAt}
			if eventID.Valid {
				value.LastEventID = eventID.String
			}
			if logoutAt.Valid {
				closedAt := logoutAt.Time
				value.LogoutAt = &closedAt
			}
			byID[id] = value
		}
		state := session.SessionState{Tags: append([]string(nil), tags...), ValidFrom: validFrom}
		if validTo.Valid {
			closedAt := validTo.Time
			state.ValidTo = &closedAt
		}
		value.States = append(value.States, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL sessions: %w", err)
	}

	values := make([]session.Session, 0, len(ids))
	for _, id := range ids {
		value := byID[id]
		if value == nil {
			return nil, fmt.Errorf("load PostgreSQL session %s: no rows returned", id)
		}
		values = append(values, *value)
	}
	return values, nil
}
