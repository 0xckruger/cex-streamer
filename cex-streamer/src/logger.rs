///! logger.rs imported from an ongoing personal project
use chrono::{Datelike, Timelike, Utc};
use once_cell::sync::OnceCell;
use std::fs;
use std::path::PathBuf;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

/// Internal static to hold the guard
static LOG_GUARD: OnceCell<Option<WorkerGuard>> = OnceCell::new();

/// Initialize the global logger
/// `log_dir`: optional folder for file logging; if `None`, logs only to console
pub fn init_logger(log_dir: Option<PathBuf>, binary_name: &str, args_verbose: bool) {
    if LOG_GUARD.get().is_some() {
        return;
    }

    let console_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_thread_ids(true)
        .with_thread_names(true);

    let log_level: EnvFilter = if args_verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    let guard = if let Some(log_dir) = log_dir {
        fs::create_dir_all(&log_dir).expect("Failed to create log directory");

        let now = Utc::now();
        let timestamped_file_name = format!(
            "{}.log.{:04}-{:02}-{:02}-{:02}-{:02}-{:02}-{:03}",
            binary_name,
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            now.timestamp_subsec_millis()
        );
        let log_path = log_dir.join(&timestamped_file_name);

        let file = fs::File::create(&log_path).expect("Failed to create log file");
        let (non_blocking, guard) = NonBlocking::new(file);

        let current_symlink = log_dir.join(format!("{}.current.log", binary_name));
        #[cfg(unix)]
        {
            let _ = fs::remove_file(&current_symlink);
            let _ = std::os::unix::fs::symlink(&timestamped_file_name, &current_symlink);
        }
        #[cfg(windows)]
        {
            let _ = fs::remove_file(&current_symlink);
            let _ = std::os::windows::fs::symlink_file(&timestamped_file_name, &current_symlink);
        }

        let file_layer = fmt::layer().with_writer(non_blocking).with_ansi(false);

        tracing_subscriber::registry()
            .with(log_level)
            .with(console_layer)
            .with(file_layer)
            .init();

        Some(guard)
    } else {
        tracing_subscriber::registry()
            .with(log_level)
            .with(console_layer)
            .init();

        None
    };

    LOG_GUARD.set(guard).ok();
}
