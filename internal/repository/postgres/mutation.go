package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
)

func loadSnapshot(ctx context.Context, tx *sql.Tx, key session.SessionKey) (session.CurrentSessionSnapshot, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, login_at, logout_at
		FROM sessions
		WHERE tenant_id = $1 AND username = $2 AND ip = $3::inet
		ORDER BY login_at, id
		FOR UPDATE
	`, key.TenantID, key.Username, key.IP)
	if err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("lock session history: %w", err)
	}

	var snapshot session.CurrentSessionSnapshot
	for rows.Next() {
		var (
			id       string
			loginAt  time.Time
			logoutAt sql.NullTime
		)
		if err := rows.Scan(&id, &loginAt, &logoutAt); err != nil {
			_ = rows.Close()
			return session.CurrentSessionSnapshot{}, fmt.Errorf("scan session history: %w", err)
		}
		advanceLatest(&snapshot, loginAt)
		if logoutAt.Valid {
			advanceLatest(&snapshot, logoutAt.Time)
			continue
		}
		if snapshot.Active != nil {
			_ = rows.Close()
			return session.CurrentSessionSnapshot{}, invalidMutation("session key has multiple active lifecycles")
		}
		snapshot.Active = &session.Session{ID: id, Key: key, LoginAt: loginAt}
	}
	if err := rows.Close(); err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("close session history rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("read session history: %w", err)
	}
	if snapshot.Active == nil {
		return snapshot, nil
	}

	stateRows, err := tx.QueryContext(ctx, `
		SELECT to_json(tags)::text, valid_from, valid_to
		FROM session_states
		WHERE session_id = $1::uuid
		ORDER BY valid_from, id
		FOR UPDATE
	`, snapshot.Active.ID)
	if err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("lock active session states: %w", err)
	}
	for stateRows.Next() {
		var (
			tagsJSON  string
			validFrom time.Time
			validTo   sql.NullTime
		)
		if err := stateRows.Scan(&tagsJSON, &validFrom, &validTo); err != nil {
			_ = stateRows.Close()
			return session.CurrentSessionSnapshot{}, fmt.Errorf("scan active session state: %w", err)
		}
		var tags []string
		if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
			_ = stateRows.Close()
			return session.CurrentSessionSnapshot{}, fmt.Errorf("decode active session tags: %w", err)
		}
		state := session.SessionState{Tags: append([]string(nil), tags...), ValidFrom: validFrom}
		if validTo.Valid {
			closedAt := validTo.Time
			state.ValidTo = &closedAt
			advanceLatest(&snapshot, closedAt)
		}
		snapshot.Active.States = append(snapshot.Active.States, state)
		advanceLatest(&snapshot, validFrom)
	}
	if err := stateRows.Close(); err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("close active state rows: %w", err)
	}
	if err := stateRows.Err(); err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("read active session states: %w", err)
	}
	if len(snapshot.Active.States) == 0 {
		return session.CurrentSessionSnapshot{}, invalidMutation("active session has no states")
	}
	current := snapshot.Active.States[len(snapshot.Active.States)-1]
	if current.ValidTo != nil {
		return session.CurrentSessionSnapshot{}, invalidMutation("active session has no open state")
	}
	return snapshot, nil
}

func persistMutation(ctx context.Context, tx *sql.Tx, key session.SessionKey, snapshot session.CurrentSessionSnapshot, mutation session.Mutation) error {
	switch value := mutation.(type) {
	case session.StartSession:
		return persistStart(ctx, tx, key, snapshot, value)
	case *session.StartSession:
		if value == nil {
			return invalidMutation("start mutation is nil")
		}
		return persistStart(ctx, tx, key, snapshot, *value)
	case session.ReplaceState:
		return persistReplace(ctx, tx, snapshot, value)
	case *session.ReplaceState:
		if value == nil {
			return invalidMutation("state replacement mutation is nil")
		}
		return persistReplace(ctx, tx, snapshot, *value)
	case session.EndSession:
		return persistEnd(ctx, tx, snapshot, value)
	case *session.EndSession:
		if value == nil {
			return invalidMutation("end mutation is nil")
		}
		return persistEnd(ctx, tx, snapshot, *value)
	default:
		return invalidMutation("unsupported mutation type %T", mutation)
	}
}

func persistStart(ctx context.Context, tx *sql.Tx, key session.SessionKey, snapshot session.CurrentSessionSnapshot, mutation session.StartSession) error {
	value := mutation.Session
	if snapshot.Active != nil || value.ID == "" || value.Key != key || value.LoginAt.IsZero() || value.LogoutAt != nil || len(value.States) != 1 {
		return invalidMutation("start mutation is inconsistent")
	}
	state := value.States[0]
	if state.ValidFrom.IsZero() || !state.ValidFrom.Equal(value.LoginAt) || state.ValidTo != nil {
		return invalidMutation("initial state is inconsistent")
	}
	if snapshot.LastEventAt != nil && value.LoginAt.Before(*snapshot.LastEventAt) {
		return invalidMutation("start mutation precedes latest event")
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ($1::uuid, $2, $3, $4::inet, $5)
	`, value.ID, key.TenantID, key.Username, key.IP, value.LoginAt); err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ($1::uuid, $2, $3)
	`, value.ID, state.Tags, state.ValidFrom); err != nil {
		return fmt.Errorf("insert initial session state: %w", err)
	}
	return nil
}

func persistReplace(ctx context.Context, tx *sql.Tx, snapshot session.CurrentSessionSnapshot, mutation session.ReplaceState) error {
	if snapshot.Active == nil || mutation.SessionID != snapshot.Active.ID || mutation.CloseCurrentAt.IsZero() {
		return invalidMutation("state replacement does not identify the active session")
	}
	if mutation.State.ValidFrom.IsZero() || !mutation.State.ValidFrom.Equal(mutation.CloseCurrentAt) || mutation.State.ValidTo != nil {
		return invalidMutation("replacement state is inconsistent")
	}
	if snapshot.LastEventAt != nil && mutation.CloseCurrentAt.Before(*snapshot.LastEventAt) {
		return invalidMutation("state replacement precedes latest event")
	}
	if err := closeCurrentState(ctx, tx, mutation.SessionID, mutation.CloseCurrentAt); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ($1::uuid, $2, $3)
	`, mutation.SessionID, mutation.State.Tags, mutation.State.ValidFrom); err != nil {
		return fmt.Errorf("insert replacement session state: %w", err)
	}
	return nil
}

func persistEnd(ctx context.Context, tx *sql.Tx, snapshot session.CurrentSessionSnapshot, mutation session.EndSession) error {
	if snapshot.Active == nil || mutation.SessionID != snapshot.Active.ID || mutation.CloseCurrentAt.IsZero() || !mutation.CloseCurrentAt.Equal(mutation.LogoutAt) {
		return invalidMutation("end mutation does not identify the active session")
	}
	if snapshot.LastEventAt != nil && mutation.LogoutAt.Before(*snapshot.LastEventAt) {
		return invalidMutation("end mutation precedes latest event")
	}
	if err := closeCurrentState(ctx, tx, mutation.SessionID, mutation.CloseCurrentAt); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE sessions SET logout_at = $1
		WHERE id = $2::uuid AND logout_at IS NULL
	`, mutation.LogoutAt, mutation.SessionID)
	if err != nil {
		return fmt.Errorf("close session: %w", err)
	}
	if err := requireOneRow(result, "close session"); err != nil {
		return err
	}
	return nil
}

func closeCurrentState(ctx context.Context, tx *sql.Tx, sessionID string, closedAt time.Time) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE session_states SET valid_to = $1
		WHERE session_id = $2::uuid AND valid_to IS NULL
	`, closedAt, sessionID)
	if err != nil {
		return fmt.Errorf("close current session state: %w", err)
	}
	return requireOneRow(result, "close current session state")
}

func requireOneRow(result sql.Result, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s row count: %w", operation, err)
	}
	if count != 1 {
		return invalidMutation("%s affected %d rows, want 1", operation, count)
	}
	return nil
}

func advanceLatest(snapshot *session.CurrentSessionSnapshot, candidate time.Time) {
	if candidate.IsZero() {
		return
	}
	if snapshot.LastEventAt == nil || candidate.After(*snapshot.LastEventAt) {
		value := candidate
		snapshot.LastEventAt = &value
	}
}

func cloneSnapshot(value session.CurrentSessionSnapshot) session.CurrentSessionSnapshot {
	result := value
	if value.LastEventAt != nil {
		lastEventAt := *value.LastEventAt
		result.LastEventAt = &lastEventAt
	}
	if value.Active != nil {
		active := *value.Active
		active.States = make([]session.SessionState, len(value.Active.States))
		for index := range value.Active.States {
			state := value.Active.States[index]
			state.Tags = append([]string(nil), state.Tags...)
			if state.ValidTo != nil {
				validTo := *state.ValidTo
				state.ValidTo = &validTo
			}
			active.States[index] = state
		}
		result.Active = &active
	}
	return result
}
