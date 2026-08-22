CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (trim(tenant_id) <> ''),
    username TEXT NOT NULL CHECK (trim(username) <> ''),
    ip TEXT NOT NULL CHECK (trim(ip) <> ''),
    login_at_ns INTEGER NOT NULL,
    logout_at_ns INTEGER,
    last_event_id TEXT NOT NULL CHECK (trim(last_event_id) <> ''),
    CHECK (logout_at_ns IS NULL OR logout_at_ns >= login_at_ns)
);

CREATE UNIQUE INDEX sessions_one_active_per_key_idx
    ON sessions (tenant_id, username, ip) WHERE logout_at_ns IS NULL;
CREATE INDEX sessions_sort_idx
    ON sessions (tenant_id COLLATE BINARY, username COLLATE BINARY, ip COLLATE BINARY, login_at_ns, id);
CREATE INDEX sessions_user_login_idx
    ON sessions (tenant_id, username, login_at_ns, id);

CREATE TABLE session_states (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    valid_from_ns INTEGER NOT NULL,
    valid_to_ns INTEGER,
    CHECK (valid_to_ns IS NULL OR valid_to_ns >= valid_from_ns)
);

CREATE INDEX session_states_session_order_idx
    ON session_states (session_id, valid_from_ns, id);
CREATE INDEX session_states_activity_idx
    ON session_states (valid_from_ns, valid_to_ns, session_id);
CREATE UNIQUE INDEX session_states_one_open_per_session_idx
    ON session_states (session_id) WHERE valid_to_ns IS NULL;

CREATE TABLE session_state_tags (
    state_id INTEGER NOT NULL REFERENCES session_states(id) ON DELETE CASCADE,
    tag TEXT NOT NULL CHECK (trim(tag) <> ''),
    PRIMARY KEY (state_id, tag)
);

CREATE INDEX session_state_tags_tag_idx ON session_state_tags (tag, state_id);
