// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    http::{CloudbreakApiResponse, CloudbreakRpcState},
};
use cloudbreak_core::EnvironmentInfo;
use serde::{Deserialize, Serialize};
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};

const CLOUDBREAK_VERSION: &str = env!("CARGO_PKG_VERSION");
const VERSION_CACHE_TTL: Duration = Duration::from_secs(600);

static VERSION_CACHE: LazyLock<RwLock<Option<(Instant, String)>>> =
    LazyLock::new(|| RwLock::new(None));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcVersionInfo {
    #[serde(rename = "solana-core")]
    pub solana_core: String,
}

#[tracing::instrument(name = "getVersion", skip_all)]
pub async fn get_version(
    state: &CloudbreakRpcState,
) -> Result<CloudbreakApiResponse<RpcVersionInfo>, RpcError> {
    if let Ok(cache) = VERSION_CACHE.read()
        && let Some((cached_at, version)) = cache.as_ref()
        && cached_at.elapsed() < VERSION_CACHE_TTL
    {
        return Ok(CloudbreakApiResponse::Response(RpcVersionInfo {
            solana_core: version.clone(),
        }));
    }

    let grpc_version = EnvironmentInfo::load_grpc_version(&state.database)
        .await
        .map_err(|_| RpcError::InternalError)?
        .unwrap_or_else(|| "unknown".to_string());

    let version = format!("{}-cloudbreak{}", grpc_version, CLOUDBREAK_VERSION);

    if let Ok(mut cache) = VERSION_CACHE.write() {
        *cache = Some((Instant::now(), version.clone()));
    }

    Ok(CloudbreakApiResponse::Response(RpcVersionInfo {
        solana_core: version,
    }))
}
