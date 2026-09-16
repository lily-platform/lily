#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use lily_shutdown::{ShutdownState, SignalHandler};
    use std::io::Write;
    use std::sync::Arc;

    let state = Arc::new(ShutdownState::new());
    let mut monitor = SignalHandler::install(Arc::clone(&state)).await?;
    println!("READY");
    std::io::stdout().flush()?;

    let first = monitor.wait_for_first().await?;
    println!("FIRST:{first:?}");
    println!("ADMISSION:{}", state.is_accepting_connections());
    std::io::stdout().flush()?;
    state.wait_for_force().await;
    println!("FORCED");
    std::io::stdout().flush()?;
    monitor.stop().await?;
    Ok(())
}

#[cfg(not(unix))]
fn main() {}
