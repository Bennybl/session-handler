CREATE TABLE sessions (
    id uuid PRIMARY KEY,
    tenant_id text NOT NULL,
    username text NOT NULL,
    ip inet NOT NULL,
    login_at timestamptz NOT NULL,
    logout_at timestamptz,
    lifecycle tstzrange GENERATED ALWAYS AS (
        tstzrange(login_at, logout_at, '[)')
    ) STORED,
    CONSTRAINT sessions_tenant_not_blank CHECK (btrim(tenant_id) <> ''),
    CONSTRAINT sessions_username_not_blank CHECK (btrim(username) <> ''),
    CONSTRAINT sessions_logout_not_before_login
        CHECK (logout_at IS NULL OR logout_at >= login_at)
);

CREATE UNIQUE INDEX sessions_one_active_per_key_idx
    ON sessions (tenant_id, username, ip)
    WHERE logout_at IS NULL;

CREATE INDEX sessions_sort_idx
    ON sessions (tenant_id, username, ip, login_at, id);

CREATE INDEX sessions_user_login_idx
    ON sessions (tenant_id, username, login_at, id);

CREATE INDEX sessions_lifecycle_gist_idx
    ON sessions USING gist (lifecycle);

CREATE TABLE session_states (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id uuid NOT NULL,
    tags text[] NOT NULL DEFAULT '{}',
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    activity tstzrange GENERATED ALWAYS AS (
        tstzrange(valid_from, valid_to, '[)')
    ) STORED,
    CONSTRAINT session_states_session_fk
        FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE,
    CONSTRAINT session_states_valid_range
        CHECK (valid_to IS NULL OR valid_to >= valid_from),
    CONSTRAINT session_states_tags_have_no_nulls
        CHECK (array_position(tags, NULL) IS NULL)
);

CREATE INDEX session_states_session_order_idx
    ON session_states (session_id, valid_from, id);

CREATE INDEX session_states_activity_gist_idx
    ON session_states USING gist (activity);

CREATE INDEX session_states_tags_gin_idx
    ON session_states USING gin (tags);
