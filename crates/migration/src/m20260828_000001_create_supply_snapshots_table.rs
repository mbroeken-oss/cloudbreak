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
                CREATE TABLE IF NOT EXISTS supply_snapshots (
                    commitment SMALLINT PRIMARY KEY,
                    context_slot BIGINT NOT NULL,
                    payload TEXT NOT NULL,
                    sampled_at_ms BIGINT NOT NULL,
                    source_latency_ms BIGINT NOT NULL
                );

                -- Keyset pagination for Cloudbreak's bounded Stake-program
                -- discovery endpoint. These are partial so other program
                -- indexes do not pay the storage cost.
                CREATE INDEX IF NOT EXISTS idx_accounts_stake_pubkey_slot
                    ON accounts (pubkey, slot DESC)
                    WHERE owner = '\\x06a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc000000000'::bytea;
                CREATE INDEX IF NOT EXISTS idx_snapshot_accounts_stake_pubkey_slot
                    ON snapshot_accounts (pubkey, slot DESC)
                    WHERE owner = '\\x06a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc000000000'::bytea;
                "#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS supply_snapshots;")
            .await?;
        Ok(())
    }
}
