-- Run once as a database administrator; safe to run again. Existing business data is not modified.
CREATE DATABASE IF NOT EXISTS cdc_platform
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

USE cdc_platform;

CREATE TABLE IF NOT EXISTS table_schema_snapshot (
    snapshot_id CHAR(64) NOT NULL PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    primary_key_name VARCHAR(64) NOT NULL,
    columns_json LONGTEXT NOT NULL,
    recovery_restriction VARCHAR(1024),
    invalidated_reason VARCHAR(512),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_snapshot_table (source_id, database_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS recovery_baseline (
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    snapshot_id CHAR(64) NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    binlog_position BIGINT NOT NULL,
    server_uuid VARCHAR(64) NOT NULL,
    verified_at TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (source_id, database_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS change_event (
    sequence_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id CHAR(64) NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    primary_key_name VARCHAR(64) NOT NULL,
    primary_key_value VARCHAR(255) NOT NULL,
    before_primary_key_value VARCHAR(255),
    event_time_ms BIGINT NOT NULL,
    archived_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    schema_snapshot_id CHAR(64) NOT NULL,
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    entry_index INT NOT NULL,
    row_index INT NOT NULL,
    event_json LONGTEXT NOT NULL,
    UNIQUE KEY uk_event_id (event_id),
    INDEX idx_event_table_key (source_id, database_name, table_name, primary_key_value, sequence_id),
    INDEX idx_event_table_old_key (source_id, database_name, table_name, before_primary_key_value, sequence_id),
    INDEX idx_event_table_time (source_id, database_name, table_name, event_time_ms, sequence_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS recovery_plan (
    plan_id CHAR(64) NOT NULL PRIMARY KEY,
    event_id CHAR(64) NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    reverse_operation VARCHAR(16) NOT NULL,
    primary_key_name VARCHAR(64) NOT NULL,
    primary_key_value VARCHAR(255) NOT NULL,
    generated_sql LONGTEXT NOT NULL,
    pre_check_summary LONGTEXT,
    schema_snapshot_id CHAR(64) NOT NULL,
    executable BOOLEAN NOT NULL DEFAULT FALSE,
    reason VARCHAR(512),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_plan_event (event_id),
    INDEX idx_plan_table (source_id, database_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS event_subscription (
    subscription_id CHAR(64) NOT NULL PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    target_name VARCHAR(64) NOT NULL,
    operations VARCHAR(64) NOT NULL,
    changed_fields VARCHAR(512),
    filter_field VARCHAR(64),
    filter_value VARCHAR(255),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    rule_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_sub_table (source_id, database_name, table_name, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS delivery_task (
    task_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    delivery_id CHAR(64) NOT NULL,
    event_id CHAR(64) NOT NULL,
    subscription_id CHAR(64) NOT NULL,
    round_type VARCHAR(16) NOT NULL DEFAULT 'INITIAL',
    round_id VARCHAR(64) NOT NULL DEFAULT 'INITIAL',
    target_name VARCHAR(64) NOT NULL,
    target_url VARCHAR(512) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    lease_until TIMESTAMP(3) NULL,
    lease_owner VARCHAR(64) NULL,
    next_retry_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    last_error VARCHAR(1024),
    event_payload LONGTEXT NOT NULL,
    subscription_snapshot LONGTEXT NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_event_sub_round (event_id, subscription_id, round_id),
    INDEX idx_task_claim (status, next_retry_at),
    INDEX idx_task_event (event_id),
    INDEX idx_task_sub (subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS delivery_attempt (
    attempt_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id BIGINT NOT NULL,
    attempt_number INT NOT NULL,
    status VARCHAR(16) NOT NULL,
    http_status INT,
    duration_ms BIGINT NOT NULL,
    error_message VARCHAR(1024),
    response_summary VARCHAR(1024),
    next_retry_at TIMESTAMP(3) NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_task_attempt (task_id, attempt_number),
    INDEX idx_attempt_task (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS replay_job (
    job_id CHAR(64) NOT NULL PRIMARY KEY,
    request_key VARCHAR(128) NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    subscription_id CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    total_events INT NOT NULL DEFAULT 0,
    matched_events INT NOT NULL DEFAULT 0,
    skipped_events INT NOT NULL DEFAULT 0,
    failed_events INT NOT NULL DEFAULT 0,
    event_ids_json LONGTEXT NOT NULL,
    subscription_snapshot LONGTEXT NOT NULL,
    target_url VARCHAR(512),
    last_error VARCHAR(1024),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_replay_request (request_key),
    INDEX idx_replay_sub (subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Grant platform access to the configured application account separately (see 04-platform-grant.sql).
-- GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX ON cdc_platform.* TO 'demo'@'%';

