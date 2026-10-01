use rust_streaming_systems_lab::server::router;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    tracing::info!("streaming query service listening on 127.0.0.1:8080");

    axum::serve(listener, router()).await?;
    Ok(())
}
