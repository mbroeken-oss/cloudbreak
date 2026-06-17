// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};

pub async fn get_accounts_count(database_url: &str) {
    let db = Database::connect(database_url)
        .await
        .expect("Failed to connect to database");

    let accounts_count = get_row_count(&db, "accounts").await;
    let snapshot_accounts_count = get_row_count(&db, "snapshot_accounts").await;

    println!("Row count for accounts:          {}", accounts_count);
    println!(
        "Row count for snapshot_accounts: {}",
        snapshot_accounts_count
    );
    println!("---");
    println!(
        "Total:                           {}",
        accounts_count + snapshot_accounts_count
    );
}

async fn get_row_count(db: &DatabaseConnection, table: &str) -> i64 {
    let rows = db
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            format!("SELECT COUNT(*) FROM {table}"),
        ))
        .await
        .expect("Failed to get row count");
    rows.first().unwrap().try_get::<i64>("", "count").unwrap()
}
