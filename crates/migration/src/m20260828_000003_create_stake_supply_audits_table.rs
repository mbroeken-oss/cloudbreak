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
                CREATE TABLE IF NOT EXISTS stake_supply_audits (
                    context_slot BIGINT PRIMARY KEY,
                    generation BIGINT NOT NULL,
                    block_time BIGINT NOT NULL,
                    epoch BIGINT NOT NULL,
                    non_circulating_lamports BIGINT NOT NULL,
                    account_count BIGINT NOT NULL,
                    computed_at_ms BIGINT NOT NULL
                );
                "#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS stake_supply_audits;")
            .await?;
        Ok(())
    }
}
