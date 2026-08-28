// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Repairs production databases that had already recorded migrations whose
//! schema definitions were later extended upstream.
//!
//! The original `slots` migration gained `health`, and the new tracker/simulator
//! tables can be recorded as applied by legacy deployments without their tables
//! being present. This forward-only, idempotent migration makes those databases
//! match the current binary without rewriting migration history.

use sea_orm_migration::prelude::*;

use super::m20260711_000000_create_index_patterns_table::CREATE_SQL as CREATE_INDEX_PATTERNS_SQL;

#[derive(DeriveMigrationName)]
pub struct Migration;

const REPAIR_SQL: &str = r#"
ALTER TABLE slots
    ADD COLUMN IF NOT EXISTS health BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS recent_blockhashes (
    slot         BIGINT NOT NULL PRIMARY KEY,
    blockhash    TEXT NOT NULL,
    block_height BIGINT
);

CREATE INDEX IF NOT EXISTS recent_blockhashes_blockhash_idx
    ON recent_blockhashes (blockhash);
"#;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        connection.execute_unprepared(REPAIR_SQL).await?;
        connection
            .execute_unprepared(CREATE_INDEX_PATTERNS_SQL)
            .await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // This migration only repairs objects owned by earlier migrations. Their
        // normal rollback handlers remain responsible for removing those objects.
        Ok(())
    }
}
