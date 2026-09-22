use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::{StreamableHttpServerConfig, router};

const TOKEN: &str = "test-token-do-not-use-in-production";

fn offline_storage() -> oxana::Storage {
    oxana::Storage::from_url("redis://127.0.0.1:1").unwrap()
}

fn app(storage: oxana::Storage) -> Router {
    router(
        storage,
        TOKEN,
        StreamableHttpServerConfig::default().enforce_origin_validation(),
    )
    .unwrap()
}

fn rpc_request(message: Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::HOST, "localhost")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-06-18")
        .body(Body::from(message.to_string()))
        .unwrap()
}

async fn rpc(app: &Router, message: Value) -> Value {
    let response = app.clone().oneshot(rpc_request(message)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn call(app: &Router, name: &str, arguments: Value) -> Value {
    rpc(
        app,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": name, "arguments": arguments,
        }}),
    )
    .await
}

#[test]
fn invalid_tokens_fail_at_construction() {
    for token in ["", " ", "a b", "a\nb", "Bearer token", "=", "a=b", "é"] {
        assert!(
            router(
                offline_storage(),
                token,
                StreamableHttpServerConfig::default()
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn authentication_covers_every_method_and_rejects_ambiguous_headers() {
    let app = app(offline_storage());
    for method in [Method::POST, Method::GET, Method::DELETE, Method::OPTIONS] {
        for authorization in [
            None,
            Some("Bearer wrong"),
            Some("Basic test"),
            Some("Bearer "),
        ] {
            let mut request = Request::builder().method(method.clone()).uri("/mcp");
            if let Some(value) = authorization {
                request = request.header(header::AUTHORIZATION, value);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(
                response.headers()[header::WWW_AUTHENTICATE],
                "Bearer realm=\"oxana-mcp\""
            );
        }
    }
    let mut request = rpc_request(json!({}));
    request
        .headers_mut()
        .append(header::AUTHORIZATION, "Bearer other".parse().unwrap());
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authenticated_clients_can_initialize_and_discover_only_read_tools() {
    let app = app(offline_storage());
    let mut request = rpc_request(json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "test", "version": "1"}},
    }));
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("bearer {TOKEN}").parse().unwrap(),
    );
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(body["result"]["serverInfo"]["name"], "oxana-mcp");
    let result = rpc(
        &app,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    )
    .await;
    let tools = result["result"]["tools"].as_array().unwrap();
    let mut names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "get_job",
            "get_job_metrics",
            "get_overview",
            "get_queue_metrics",
            "list_jobs",
            "list_processes",
            "list_queues"
        ]
    );
    for tool in tools {
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["annotations"]["destructiveHint"], false);
    }
    // A successful handshake does not bypass authentication on later requests.
    let mut request = rpc_request(json!({"jsonrpc": "2.0", "id": 3, "method": "tools/list"}));
    request.headers_mut().remove(header::AUTHORIZATION);
    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn transport_rejects_untrusted_host_and_origin() {
    let app = app(offline_storage());
    for (name, value) in [
        (header::HOST, "untrusted.example"),
        (header::ORIGIN, "https://untrusted.example"),
    ] {
        let mut request = rpc_request(json!({}));
        request.headers_mut().insert(name, value.parse().unwrap());
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
}

#[tokio::test]
async fn invalid_queries_are_rejected_before_storage_access() {
    let app = app(offline_storage());
    for arguments in [
        json!({"list": "queue"}),
        json!({"list": "queue", "queue": ""}),
        json!({"list": "dead", "queue": "default"}),
        json!({"list": "dead", "limit": 0}),
        json!({"list": "dead", "limit": 101}),
        json!({"list": "dead", "offset": usize::MAX}),
    ] {
        let response = call(&app, "list_jobs", arguments).await;
        assert_eq!(response["error"]["code"], -32602, "{response}");
    }
    let response = call(&app, "get_job", json!({"id": "missing"})).await;
    assert_eq!(response["result"]["isError"], true);
    assert!(!response.to_string().contains("127.0.0.1"));
}

/// Like the main crate's storage tests, this requires REDIS_URL.
#[tokio::test]
async fn monitoring_tools_read_real_storage_and_omit_payloads() {
    let namespace = format!(
        "oxana-mcp-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let storage = oxana::Storage::builder()
        .namespace(&namespace)
        .build_from_redis_url(std::env::var("REDIS_URL").expect("REDIS_URL is not set"))
        .unwrap();
    let job: oxana::JobEnvelope = serde_json::from_value(json!({
        "id": "test-job", "queue": "mcp-test", "job": {"name": "ExampleJob", "args": {"secret": "private-payload"}},
        "meta": {"id": "test-job", "retries": 0, "unique": false, "created_at": 1,
            "scheduled_at": 1, "state": {"secret": "private-state"}, "error": "example failure"},
    })).unwrap();
    storage.enqueue_envelope(job.clone()).await.unwrap();
    let mut second = job;
    second.id = "second-job".into();
    second.meta.id = second.id.clone();
    storage.enqueue_envelope(second).await.unwrap();
    let app = app(storage.clone());

    let response = call(&app, "get_job", json!({"id": "test-job"})).await;
    let result = &response["result"]["structuredContent"];
    assert_eq!(result["namespace"], namespace);
    assert!(result["observed_at"].is_string());
    assert_eq!(result["data"]["id"], "test-job");
    assert_eq!(result["data"]["error"], "example failure");
    assert!(!response.to_string().contains("private-"));

    let first = call(
        &app,
        "list_jobs",
        json!({"list": "queue", "queue": "mcp-test", "limit": 1}),
    )
    .await;
    let second = call(
        &app,
        "list_jobs",
        json!({"list": "queue", "queue": "mcp-test", "limit": 1, "offset": 1}),
    )
    .await;
    assert_eq!(
        first["result"]["structuredContent"]["data"]["jobs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_ne!(
        first["result"]["structuredContent"]["data"]["jobs"][0]["id"],
        second["result"]["structuredContent"]["data"]["jobs"][0]["id"]
    );
    assert!(!first.to_string().contains("private-"));
    for list in ["dead", "retries", "scheduled"] {
        let response = call(&app, "list_jobs", json!({"list": list})).await;
        assert_eq!(
            response["result"]["structuredContent"]["data"]["jobs"],
            json!([])
        );
    }
    for tool in [
        "get_overview",
        "list_queues",
        "list_processes",
        "get_job_metrics",
        "get_queue_metrics",
    ] {
        let response = call(&app, tool, json!({})).await;
        assert_ne!(response["result"]["isError"], true, "{tool}: {response}");
        assert_eq!(
            response["result"]["structuredContent"]["namespace"], namespace,
            "{tool}: {response}"
        );
    }
    for tool in ["get_job_metrics", "get_queue_metrics"] {
        for (arguments, expected_minutes) in [
            (json!({}), 60),
            (json!({"minutes": 0}), 60),
            (json!({"minutes": 5}), 5),
            (json!({"minutes": 1441}), 1440),
        ] {
            let response = call(&app, tool, arguments).await;
            assert_eq!(
                response["result"]["structuredContent"]["data"]["minutes"], expected_minutes,
                "{tool}: {response}"
            );
        }
    }
    let missing = call(&app, "get_job", json!({"id": "absent"})).await;
    assert_eq!(missing["result"]["structuredContent"]["data"], Value::Null);
    storage.delete_job(&"test-job".into()).await.unwrap();
    storage.delete_job(&"second-job".into()).await.unwrap();
}
