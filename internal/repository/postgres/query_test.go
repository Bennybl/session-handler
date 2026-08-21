package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/repository/repositorytest"
	"github.com/Bennybl/session-handler/internal/session"
)

const secondSessionID = "00000000-0000-0000-0000-000000000102"

func TestQueryContract(t *testing.T) {
	queryCases := map[string]bool{
		"generic state scoped filters":  true,
		"pagination and stable cursors": true,
		"query result isolation":        true,
	}
	for _, contractCase := range repositorytest.Cases() {
		contractCase := contractCase
		if !queryCases[contractCase.Name] {
			continue
		}
		t.Run(contractCase.Name, func(t *testing.T) {
			contractCase.Run(t, func(t *testing.T) repository.SessionRepository {
				return newRepository(t, true)
			})
		})
	}
}

func TestQuerySupportsEveryRegistryEntry(t *testing.T) {
	repo := newRepository(t, true)
	ctx := context.Background()
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	evaluatedAt := loginAt.Add(30 * time.Minute)
	seedLogin(t, repo, key, firstSessionID, loginAt)

	tests := []struct {
		name   string
		filter session.Filter
		wantID string
	}{
		{name: "session id equals", filter: session.Filter{Field: "sessionId", Operator: "eq", Value: firstSessionID}, wantID: firstSessionID},
		{name: "session id in", filter: session.Filter{Field: "sessionId", Operator: "in", Value: []string{secondSessionID, firstSessionID}}, wantID: firstSessionID},
		{name: "tenant equals", filter: session.Filter{Field: "tenantId", Operator: "eq", Value: "tenant-a"}, wantID: firstSessionID},
		{name: "tenant in", filter: session.Filter{Field: "tenantId", Operator: "in", Value: []string{"tenant-b", "tenant-a"}}, wantID: firstSessionID},
		{name: "username equals", filter: session.Filter{Field: "username", Operator: "eq", Value: "alice"}, wantID: firstSessionID},
		{name: "username in", filter: session.Filter{Field: "username", Operator: "in", Value: []string{"bob", "alice"}}, wantID: firstSessionID},
		{name: "ip equals", filter: session.Filter{Field: "ip", Operator: "eq", Value: "192.0.2.10"}, wantID: firstSessionID},
		{name: "ip in", filter: session.Filter{Field: "ip", Operator: "in", Value: []string{"192.0.2.11", "192.0.2.10"}}, wantID: firstSessionID},
		{name: "tags contain all", filter: session.Filter{Field: "tags", Operator: "containsAll", Value: []string{"user"}}, wantID: firstSessionID},
		{name: "tags contain any", filter: session.Filter{Field: "tags", Operator: "containsAny", Value: []string{"missing", "user"}}, wantID: firstSessionID},
		{name: "activity at", filter: session.Filter{Field: "activity", Operator: "at", Value: evaluatedAt}, wantID: firstSessionID},
		{name: "activity overlaps", filter: session.Filter{Field: "activity", Operator: "overlaps", Value: query.IntervalValue{From: timePointer(loginAt.Add(time.Minute)), To: timePointer(evaluatedAt)}}, wantID: firstSessionID},
		{name: "login equals", filter: session.Filter{Field: "loginTime", Operator: "eq", Value: loginAt}, wantID: firstSessionID},
		{name: "login greater", filter: session.Filter{Field: "loginTime", Operator: "gt", Value: loginAt.Add(-time.Minute)}, wantID: firstSessionID},
		{name: "login greater or equal", filter: session.Filter{Field: "loginTime", Operator: "gte", Value: loginAt}, wantID: firstSessionID},
		{name: "login less", filter: session.Filter{Field: "loginTime", Operator: "lt", Value: loginAt.Add(time.Minute)}, wantID: firstSessionID},
		{name: "login less or equal", filter: session.Filter{Field: "loginTime", Operator: "lte", Value: loginAt}, wantID: firstSessionID},
		{name: "login between", filter: session.Filter{Field: "loginTime", Operator: "between", Value: query.IntervalValue{From: timePointer(loginAt.Add(-time.Minute)), To: timePointer(loginAt.Add(time.Minute))}}, wantID: firstSessionID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := repo.Query(ctx, session.QuerySpec{
				Filters: []session.Filter{tt.filter}, Page: session.PageRequest{Limit: 10}, EvaluatedAt: evaluatedAt,
			})
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			if len(result.Sessions) != 1 || result.Sessions[0].ID != tt.wantID {
				t.Fatalf("Query() sessions = %+v, want %s", result.Sessions, tt.wantID)
			}
		})
	}
}

func TestQueryUsesOneMatchingStateAndLoadsCompleteHistory(t *testing.T) {
	repo := newRepository(t, true)
	ctx := context.Background()
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	updateAt := loginAt.Add(time.Hour)
	seedLogin(t, repo, key, firstSessionID, loginAt)
	if err := repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{EventID: postgresUpdateEventID, Key: key, Tags: []string{"admin"}, Timestamp: updateAt})
	}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	result, err := repo.Query(ctx, session.QuerySpec{
		Filters: []session.Filter{
			{Field: "activity", Operator: "at", Value: loginAt.Add(30 * time.Minute)},
			{Field: "tags", Operator: "containsAll", Value: []string{"admin"}},
		},
		Page: session.PageRequest{Limit: 10}, EvaluatedAt: updateAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("mismatched-state Query() error = %v", err)
	}
	if len(result.Sessions) != 0 {
		t.Fatalf("mismatched-state sessions = %+v, want none", result.Sessions)
	}

	result, err = repo.Query(ctx, session.QuerySpec{
		Filters: []session.Filter{
			{Field: "activity", Operator: "at", Value: updateAt.Add(time.Minute)},
			{Field: "tags", Operator: "containsAll", Value: []string{"admin"}},
		},
		Page: session.PageRequest{Limit: 10}, EvaluatedAt: updateAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("matching-state Query() error = %v", err)
	}
	if len(result.Sessions) != 1 || len(result.Sessions[0].States) != 2 {
		t.Fatalf("matching-state sessions = %+v, want one complete two-state history", result.Sessions)
	}
}

func TestQueryPagesDistinctSessionsAcrossKeysetBoundaries(t *testing.T) {
	repo := newRepository(t, true)
	ctx := context.Background()
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	updateAt := loginAt.Add(time.Hour)
	alice := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	bob := mustKey(t, "tenant-a", "bob", "192.0.2.10")
	seedLogin(t, repo, alice, firstSessionID, loginAt)
	if err := repo.Mutate(ctx, alice, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{EventID: postgresUpdateEventID, Key: alice, Tags: []string{"user"}, Timestamp: updateAt})
	}); err != nil {
		t.Fatalf("seed Alice update: %v", err)
	}
	seedLogin(t, repo, bob, secondSessionID, loginAt)

	spec := session.QuerySpec{
		Filters: []session.Filter{{
			Field: "activity", Operator: "overlaps",
			Value: query.IntervalValue{From: timePointer(loginAt), To: timePointer(updateAt.Add(time.Hour))},
		}},
		Page: session.PageRequest{Limit: 1}, EvaluatedAt: updateAt,
	}
	first, err := repo.Query(ctx, spec)
	if err != nil {
		t.Fatalf("first Query() error = %v", err)
	}
	if len(first.Sessions) != 1 || first.Sessions[0].ID != firstSessionID || len(first.Sessions[0].States) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}

	spec.Page.Cursor = first.NextCursor
	second, err := repo.Query(ctx, spec)
	if err != nil {
		t.Fatalf("second Query() error = %v", err)
	}
	if len(second.Sessions) != 1 || second.Sessions[0].ID != secondSessionID || second.NextCursor != "" {
		t.Fatalf("second page = %+v", second)
	}
}

func TestQueryRejectsUnsupportedAndInjectionShapedInputs(t *testing.T) {
	repo := newRepository(t, true)
	ctx := context.Background()
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	at := mustTime(t, "2026-08-21T10:00:00Z")
	seedLogin(t, repo, key, firstSessionID, at)

	invalid := []session.Filter{
		{Field: "tenant_id) = 'tenant-a' OR TRUE --", Operator: "eq", Value: "tenant-a"},
		{Field: "tenantId", Operator: "eq OR TRUE --", Value: "tenant-a"},
		{Field: "sessionId", Operator: "eq", Value: "' OR TRUE --"},
	}
	for _, filter := range invalid {
		_, err := repo.Query(ctx, session.QuerySpec{Filters: []session.Filter{filter}, EvaluatedAt: at.Add(time.Minute)})
		if !errors.Is(err, repository.ErrInvalidQuery) {
			t.Fatalf("Query(%+v) error = %v, want ErrInvalidQuery", filter, err)
		}
	}

	result, err := repo.Query(ctx, session.QuerySpec{
		Filters:     []session.Filter{{Field: "tenantId", Operator: "eq", Value: "tenant-a' OR TRUE --"}},
		EvaluatedAt: at.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("parameter-shaped Query() error = %v", err)
	}
	if len(result.Sessions) != 0 {
		t.Fatalf("parameter-shaped Query() sessions = %+v, want none", result.Sessions)
	}

	result, err = repo.Query(ctx, session.QuerySpec{
		Filters:     []session.Filter{{Field: "tenantId", Operator: "eq", Value: "tenant-a"}},
		EvaluatedAt: at.Add(time.Minute),
	})
	if err != nil || len(result.Sessions) != 1 {
		t.Fatalf("post-injection Query() result = %+v, error = %v", result, err)
	}
}

func timePointer(value time.Time) *time.Time {
	return &value
}
