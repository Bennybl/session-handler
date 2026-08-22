package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
)

func (r *Repository) applyMutation(ctx context.Context, key session.SessionKey, mutation session.Mutation) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite mutation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := persistMutation(ctx, tx, key, mutation); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite mutation: %w", err)
	}
	return nil
}

func loadCurrent(ctx context.Context, db *sql.DB, key session.SessionKey) (session.CurrentSessionSnapshot, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, login_at_ns, logout_at_ns, last_event_id
		FROM sessions WHERE tenant_id = ? AND username = ? AND ip = ?
		ORDER BY login_at_ns, id`, key.TenantID, key.Username, key.IP)
	if err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("load session history: %w", err)
	}
	var snapshot session.CurrentSessionSnapshot
	for rows.Next() {
		var id, eventID string
		var loginNS int64
		var logoutNS sql.NullInt64
		if err := rows.Scan(&id, &loginNS, &logoutNS, &eventID); err != nil {
			_ = rows.Close()
			return session.CurrentSessionSnapshot{}, fmt.Errorf("scan session history: %w", err)
		}
		loginAt := fromNanos(loginNS)
		advanceLatest(&snapshot, loginAt)
		snapshot.LastEventID = eventID
		if logoutNS.Valid {
			advanceLatest(&snapshot, fromNanos(logoutNS.Int64))
			continue
		}
		if snapshot.Active != nil {
			_ = rows.Close()
			return session.CurrentSessionSnapshot{}, invalidMutation("session key has multiple active lifecycles")
		}
		snapshot.Active = &session.Session{ID: id, Key: key, LoginAt: loginAt, LastEventID: eventID}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return session.CurrentSessionSnapshot{}, fmt.Errorf("read session history: %w", err)
	}
	if err := rows.Close(); err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("close session history: %w", err)
	}
	if snapshot.Active == nil {
		return snapshot, nil
	}
	stateRows, err := db.QueryContext(ctx, `SELECT ss.id, ss.valid_from_ns, ss.valid_to_ns, t.tag
		FROM session_states ss LEFT JOIN session_state_tags t ON t.state_id = ss.id
		WHERE ss.session_id = ? ORDER BY ss.valid_from_ns, ss.id, t.tag`, snapshot.Active.ID)
	if err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("load active states: %w", err)
	}
	var lastStateID int64 = -1
	for stateRows.Next() {
		var stateID, validFromNS int64
		var validToNS sql.NullInt64
		var tag sql.NullString
		if err := stateRows.Scan(&stateID, &validFromNS, &validToNS, &tag); err != nil {
			_ = stateRows.Close()
			return session.CurrentSessionSnapshot{}, fmt.Errorf("scan active state: %w", err)
		}
		if stateID != lastStateID {
			state := session.SessionState{ValidFrom: fromNanos(validFromNS)}
			if validToNS.Valid {
				closedAt := fromNanos(validToNS.Int64)
				state.ValidTo = &closedAt
				advanceLatest(&snapshot, closedAt)
			}
			snapshot.Active.States = append(snapshot.Active.States, state)
			advanceLatest(&snapshot, state.ValidFrom)
			lastStateID = stateID
		}
		if tag.Valid {
			index := len(snapshot.Active.States) - 1
			snapshot.Active.States[index].Tags = append(snapshot.Active.States[index].Tags, tag.String)
		}
	}
	if err := stateRows.Err(); err != nil {
		_ = stateRows.Close()
		return session.CurrentSessionSnapshot{}, fmt.Errorf("read active states: %w", err)
	}
	if err := stateRows.Close(); err != nil {
		return session.CurrentSessionSnapshot{}, fmt.Errorf("close active states: %w", err)
	}
	if len(snapshot.Active.States) == 0 || snapshot.Active.States[len(snapshot.Active.States)-1].ValidTo != nil {
		return session.CurrentSessionSnapshot{}, invalidMutation("active session has no open state")
	}
	return snapshot, nil
}

func persistMutation(ctx context.Context, tx *sql.Tx, key session.SessionKey, mutation session.Mutation) error {
	switch value := mutation.(type) {
	case session.StartSession:
		return persistStart(ctx, tx, key, value)
	case *session.StartSession:
		if value == nil {
			return invalidMutation("start mutation is nil")
		}
		return persistStart(ctx, tx, key, *value)
	case session.ReplaceState:
		return persistReplace(ctx, tx, value)
	case *session.ReplaceState:
		if value == nil {
			return invalidMutation("state replacement mutation is nil")
		}
		return persistReplace(ctx, tx, *value)
	case session.EndSession:
		return persistEnd(ctx, tx, value)
	case *session.EndSession:
		if value == nil {
			return invalidMutation("end mutation is nil")
		}
		return persistEnd(ctx, tx, *value)
	case session.DuplicateEvent:
		return nil
	case *session.DuplicateEvent:
		if value == nil {
			return invalidMutation("duplicate mutation is nil")
		}
		return nil
	default:
		return invalidMutation("unsupported mutation type %T", mutation)
	}
}

func persistStart(ctx context.Context, tx *sql.Tx, key session.SessionKey, mutation session.StartSession) error {
	value := mutation.Session
	if value.ID == "" || value.Key != key || value.LoginAt.IsZero() || value.LogoutAt != nil || len(value.States) != 1 || value.LastEventID != mutation.EventID || mutation.EventID == "" {
		return invalidMutation("start mutation is inconsistent")
	}
	state := value.States[0]
	if state.ValidFrom.IsZero() || !state.ValidFrom.Equal(value.LoginAt) || state.ValidTo != nil {
		return invalidMutation("initial state is inconsistent")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions
		(id, tenant_id, username, ip, login_at_ns, last_event_id) VALUES (?, ?, ?, ?, ?, ?)`,
		value.ID, key.TenantID, key.Username, key.IP, toNanos(value.LoginAt), mutation.EventID); err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return insertState(ctx, tx, value.ID, state)
}

func persistReplace(ctx context.Context, tx *sql.Tx, mutation session.ReplaceState) error {
	if mutation.EventID == "" || mutation.SessionID == "" || mutation.CloseCurrentAt.IsZero() {
		return invalidMutation("state replacement does not identify the active session")
	}
	if mutation.State.ValidFrom.IsZero() || !mutation.State.ValidFrom.Equal(mutation.CloseCurrentAt) || mutation.State.ValidTo != nil {
		return invalidMutation("replacement state is inconsistent")
	}
	if err := closeCurrentState(ctx, tx, mutation.SessionID, mutation.CloseCurrentAt); err != nil {
		return err
	}
	if err := insertState(ctx, tx, mutation.SessionID, mutation.State); err != nil {
		return err
	}
	return updateEventID(ctx, tx, mutation.SessionID, mutation.EventID)
}

func persistEnd(ctx context.Context, tx *sql.Tx, mutation session.EndSession) error {
	if mutation.EventID == "" || mutation.SessionID == "" || mutation.CloseCurrentAt.IsZero() || !mutation.CloseCurrentAt.Equal(mutation.LogoutAt) {
		return invalidMutation("end mutation does not identify the active session")
	}
	if err := closeCurrentState(ctx, tx, mutation.SessionID, mutation.CloseCurrentAt); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE sessions SET logout_at_ns = ?, last_event_id = ?
		WHERE id = ? AND logout_at_ns IS NULL`, toNanos(mutation.LogoutAt), mutation.EventID, mutation.SessionID)
	if err != nil {
		return fmt.Errorf("close session: %w", err)
	}
	return requireOneRow(result, "close session")
}

func insertState(ctx context.Context, tx *sql.Tx, sessionID string, state session.SessionState) error {
	result, err := tx.ExecContext(ctx, `INSERT INTO session_states (session_id, valid_from_ns) VALUES (?, ?)`, sessionID, toNanos(state.ValidFrom))
	if err != nil {
		return fmt.Errorf("insert session state: %w", err)
	}
	stateID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read inserted state ID: %w", err)
	}
	for _, tag := range state.Tags {
		if _, err := tx.ExecContext(ctx, `INSERT INTO session_state_tags (state_id, tag) VALUES (?, ?)`, stateID, tag); err != nil {
			return fmt.Errorf("insert session state tag: %w", err)
		}
	}
	return nil
}

func updateEventID(ctx context.Context, tx *sql.Tx, sessionID, eventID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE sessions SET last_event_id = ? WHERE id = ?`, eventID, sessionID)
	if err != nil {
		return fmt.Errorf("update session event identity: %w", err)
	}
	return requireOneRow(result, "update session event identity")
}

func closeCurrentState(ctx context.Context, tx *sql.Tx, sessionID string, closedAt time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE session_states SET valid_to_ns = ? WHERE session_id = ? AND valid_to_ns IS NULL`, toNanos(closedAt), sessionID)
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
	if !candidate.IsZero() && (snapshot.LastEventAt == nil || candidate.After(*snapshot.LastEventAt)) {
		value := candidate
		snapshot.LastEventAt = &value
	}
}
