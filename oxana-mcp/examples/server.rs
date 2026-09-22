use oxana::Storage;
use oxana_mcp::{StreamableHttpServerConfig, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("OXANA_MCP_TOKEN")
        .map_err(|_| "OXANA_MCP_TOKEN must be set to a valid UTF-8 bearer token")?;
    let address = std::env::var("OXANA_MCP_BIND").unwrap_or_else(|_| "127.0.0.1:8081".into());
    let mut storage = Storage::builder();
    if let Ok(namespace) = std::env::var("OXANA_NAMESPACE") {
        storage = storage.namespace(namespace);
    }
    let storage = storage.build_from_env()?;
    let mut transport = StreamableHttpServerConfig::default().enforce_origin_validation();
    if let Ok(host) = std::env::var("OXANA_MCP_HOST") {
        transport = transport.with_allowed_hosts([host]);
    }
    let cancellation = transport.cancellation_token.clone();
    let app = router(storage, token, transport)?;
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("Oxana MCP listening on http://{address}/mcp");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation.cancel();
        })
        .await?;
    Ok(())
}
