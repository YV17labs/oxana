//! Read-only Oxana monitoring over bearer-authenticated Streamable HTTP MCP.
//!
//! The [`router`] serves `/mcp`; use HTTPS in production. The token
//! grants access to the supplied storage's namespace. Job arguments and resumable
//! state are omitted; identifiers and error messages are still returned.

mod server;

use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpService, session::local::LocalSessionManager,
};
use subtle::ConstantTimeEq;

pub use rmcp::transport::streamable_http_server::StreamableHttpServerConfig;

/// Invalid bearer-token configuration. The rejected token is never included.
#[derive(Debug)]
pub struct InvalidBearerToken;

impl std::fmt::Display for InvalidBearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bearer token must be nonempty and use the RFC 6750 token alphabet")
    }
}

impl std::error::Error for InvalidBearerToken {}

/// Build an authenticated MCP router serving `/mcp`.
///
/// Authentication covers every method, including initialization and session
/// requests. Use a randomly generated secret. The transport configuration
/// controls host/origin validation and shutdown; configure `allowed_hosts` for
/// a public hostname. This server uses stateless JSON responses.
pub fn router(
    storage: oxana::Storage,
    bearer_token: impl Into<String>,
    transport: StreamableHttpServerConfig,
) -> Result<Router, InvalidBearerToken> {
    let token = bearer_token.into();
    let unpadded = token.trim_end_matches('=');
    if unpadded.is_empty()
        || !unpadded.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
    {
        return Err(InvalidBearerToken);
    }

    let service = StreamableHttpService::new(
        move || Ok(server::OxanaMcp::new(storage.clone())),
        Arc::new(LocalSessionManager::default()),
        transport
            .with_legacy_session_mode(false)
            .with_json_response(true),
    );
    Ok(Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(
            Arc::<str>::from(token),
            authenticate,
        )))
}

async fn authenticate(State(expected): State<Arc<str>>, request: Request, next: Next) -> Response {
    let mut headers = request.headers().get_all(header::AUTHORIZATION).iter();
    let supplied = headers
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
        .map(|(_, token)| token.trim_start_matches(' '));
    let authorized = headers.next().is_none()
        && supplied.is_some_and(|token| bool::from(token.as_bytes().ct_eq(expected.as_bytes())));
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer realm=\"oxana-mcp\"")],
            "Unauthorized",
        )
            .into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests;
