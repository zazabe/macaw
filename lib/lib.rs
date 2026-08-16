pub use macaw_core::prelude as core;
#[cfg(feature = "http")]
pub use macaw_http::prelude as http;
#[cfg(feature = "wasm")]
pub use macaw_wasm::prelude as wasm;
#[cfg(feature = "ws")]
pub use macaw_ws::prelude as ws;

pub mod session;

pub fn handle_app_error(error: core::AppError) {
    match error {
        core::AppError::ExitWithError(e) => {
            tracing::error!("Exit with error: {}", e);
            #[cfg(debug_assertions)]
            {
                eprintln!("Backtrace: {}", e.backtrace());
            }
        }
        core::AppError::UnexpectedError(e) => {
            tracing::error!("{}", e);
        }
    }

    std::process::exit(1);
}
