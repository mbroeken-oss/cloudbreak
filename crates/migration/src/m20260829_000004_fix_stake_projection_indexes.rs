// SPDX-License-Identifier: AGPL-3.0-only

use sea_orm_migration::prelude::*;

const STAKE_PROGRAM_HEX: &str = "06a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc000000000";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(&format!(
                r#"
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_index index_def
                        JOIN pg_class index_class ON index_class.oid = index_def.indexrelid
                        WHERE index_class.relname = 'idx_accounts_stake_pubkey_slot'
                          AND pg_get_expr(index_def.indpred, index_def.indrelid)
                              LIKE '%decode(''{STAKE_PROGRAM_HEX}''%'
                    ) THEN
                        DROP INDEX IF EXISTS idx_accounts_stake_pubkey_slot;
                        CREATE INDEX idx_accounts_stake_pubkey_slot
                            ON accounts (pubkey, slot DESC)
                            WHERE owner = decode('{STAKE_PROGRAM_HEX}', 'hex');
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_index index_def
                        JOIN pg_class index_class ON index_class.oid = index_def.indexrelid
                        WHERE index_class.relname = 'idx_snapshot_accounts_stake_pubkey_slot'
                          AND pg_get_expr(index_def.indpred, index_def.indrelid)
                              LIKE '%decode(''{STAKE_PROGRAM_HEX}''%'
                    ) THEN
                        DROP INDEX IF EXISTS idx_snapshot_accounts_stake_pubkey_slot;
                        CREATE INDEX idx_snapshot_accounts_stake_pubkey_slot
                            ON snapshot_accounts (pubkey, slot DESC)
                            WHERE owner = decode('{STAKE_PROGRAM_HEX}', 'hex');
                    END IF;
                END $$;
                "#,
            ))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "DROP INDEX IF EXISTS idx_accounts_stake_pubkey_slot;\n                 DROP INDEX IF EXISTS idx_snapshot_accounts_stake_pubkey_slot;",
            )
            .await?;
        Ok(())
    }
}
