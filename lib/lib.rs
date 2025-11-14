pub use macaw_core::prelude as core;
pub use macaw_http::prelude as http;

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
