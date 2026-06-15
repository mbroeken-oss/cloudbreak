use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();

        connection
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS epoch_stakes (
                    epoch              BIGINT      NOT NULL,
                    vote_pubkey        BYTEA       NOT NULL,
                    node_pubkey        BYTEA       NOT NULL,
                    activated_stake    BIGINT      NOT NULL,
                    activating_stake   BIGINT      NOT NULL DEFAULT 0,
                    deactivating_stake BIGINT      NOT NULL DEFAULT 0,
                    in_epoch_set       BOOLEAN     NOT NULL,
                    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (epoch, vote_pubkey)
                );
                CREATE INDEX IF NOT EXISTS epoch_stakes_epoch_idx
                    ON epoch_stakes (epoch DESC);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS epoch_stakes;")
            .await?;

        Ok(())
    }
}
