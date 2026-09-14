#![forbid(unsafe_code)]

use std::process::ExitCode;

use tokio::sync::watch;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match cerbero_normalizer::RuntimeConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("cerbero-normalizer configuration error: {error}");
            return ExitCode::from(2);
        }
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
    });
    let result = cerbero_normalizer::run(config, shutdown_rx).await;
    signal.abort();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cerbero-normalizer runtime error: {error}");
            ExitCode::from(1)
        }
    }
}
