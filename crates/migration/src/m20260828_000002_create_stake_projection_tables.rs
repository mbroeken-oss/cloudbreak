// SPDX-License-Identifier: AGPL-3.0-only

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- A generation is built off-path from the finalized raw account
                -- tables, then made visible by atomically switching this status
                -- row. Old generations can be removed after the switch without
                -- serving mixed pages to API clients.
                CREATE TABLE IF NOT EXISTS stake_projection_status (
                    id SMALLINT PRIMARY KEY CHECK (id = 1),
                    generation BIGINT NOT NULL,
                    context_slot BIGINT NOT NULL,
                    refreshed_at_ms BIGINT NOT NULL
                );

                CREATE UNLOGGED TABLE IF NOT EXISTS stake_accounts_current (
                    generation BIGINT NOT NULL,
                    pubkey BYTEA NOT NULL,
                    slot BIGINT NOT NULL,
                    lamports BIGINT NOT NULL,
                    data BYTEA NOT NULL,
                    PRIMARY KEY (generation, pubkey)
                );
                CREATE INDEX IF NOT EXISTS idx_stake_accounts_current_generation_pubkey
                    ON stake_accounts_current (generation, pubkey);
                "#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "DROP TABLE IF EXISTS stake_accounts_current;\n                 DROP TABLE IF EXISTS stake_projection_status;",
            )
            .await?;
        Ok(())
    }
}
