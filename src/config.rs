//! Three-layer configuration system for the application.
//!
//! The module implements CLI > environment > file precedence because this ordering matches the
//! 12-factor app pattern and allows container deployments to override file-based defaults without
//! rebuilding images. A manual implementation using `serde` and `toml` keeps the dependency
//! footprint smaller than full frameworks like `figment` or `config-rs`.
//!
//! Configuration flows through a [`PartialConfig`] intermediate where all fields are optional.
//! Each layer contributes only the values it specifies, and the final merge produces an
//! [`AppConfig`] with validated, required fields. The TOML schema uses `deny_unknown_fields`
//! to reject typos rather than silently ignoring them.

use std::path::{Path, PathBuf};
use std::{env, fmt, fs, io};

use lib_kifa::{FlushMode, KIBI, MEBI, map_err};
use serde::Deserialize;

const ENV_PREFIX: &str = "KIFA_";
const CONFIG_FILE_NAME: &str = "kifa.toml";

const MIN_MEMTABLE_THRESHOLD: usize = MEBI;
const MIN_COMPACTION_THRESHOLD: usize = 2;
const MIN_SEGMENT_SIZE: usize = MEBI;
const MIN_CHANNEL_CAPACITY: usize = 1;
// Segment sizes must be 4KiB-aligned because some platforms require sector-aligned I/O
// for direct writes that bypass the page cache.
const SECTOR_ALIGNMENT: usize = 4 * KIBI;

const DEFAULT_MEMTABLE_THRESHOLD: usize = 4 * MEBI;
const DEFAULT_COMPACTION_THRESHOLD: usize = 4;
const DEFAULT_SEGMENT_SIZE: usize = 16 * MEBI;
const DEFAULT_CHANNEL_CAPACITY: usize = KIBI;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Parse(toml::de::Error),
    Validation(ValidationError),
}

#[derive(Debug)]
pub struct ValidationError {
    pub field: &'static str,
    pub message: String,
}

map_err!(Io, io::Error);
map_err!(Parse, toml::de::Error);
map_err!(Validation, ValidationError);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Parse(e) => e.fmt(f),
            Self::Validation(e) => write!(f, "{}: {}", e.field, e.message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlConfig {
    storage: Option<TomlStorageConfig>,
    wal: Option<TomlWalConfig>,
    ingester: Option<TomlIngesterConfig>,
    sources: Option<TomlSourcesConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlStorageConfig {
    memtable_flush_threshold_mib: Option<usize>,
    compaction_threshold: Option<usize>,
    compaction_enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlWalConfig {
    flush_mode: Option<String>,
    segment_size_mib: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlIngesterConfig {
    channel_capacity: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlSourcesConfig {
    stdin: Option<bool>,
    files: Option<Vec<String>>,
    tcp: Option<Vec<String>>,
    udp: Option<Vec<String>>,
}

impl From<TomlConfig> for PartialConfig {
    fn from(toml: TomlConfig) -> Self {
        let mut config = Self::default();

        if let Some(storage) = toml.storage {
            config.memtable_flush_threshold =
                storage.memtable_flush_threshold_mib.map(|v| v * MEBI);
            config.compaction_threshold = storage.compaction_threshold;
            config.compaction_enabled = storage.compaction_enabled;
        }

        if let Some(wal) = toml.wal {
            config.flush_mode = wal.flush_mode.as_deref().and_then(parse_flush_mode);
            config.segment_size = wal.segment_size_mib.map(|v| v * MEBI);
        }

        if let Some(ingester) = toml.ingester {
            config.channel_capacity = ingester.channel_capacity;
        }

        if let Some(sources) = toml.sources {
            config.stdin = sources.stdin;
            config.files =
                sources.files.map(|files| files.into_iter().map(|f| expand_path(&f)).collect());
            config.tcp = sources.tcp;
            config.udp = sources.udp;
        }

        config
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub memtable_flush_threshold: usize,
    pub compaction_threshold: usize,
    pub compaction_enabled: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            memtable_flush_threshold: DEFAULT_MEMTABLE_THRESHOLD,
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            compaction_enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub flush_mode: FlushMode,
    pub segment_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self { flush_mode: FlushMode::Normal, segment_size: DEFAULT_SEGMENT_SIZE }
    }
}

#[derive(Debug, Clone)]
pub struct IngesterConfig {
    pub channel_capacity: usize,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        Self { channel_capacity: DEFAULT_CHANNEL_CAPACITY }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SourcesConfig {
    pub stdin: bool,
    pub files: Vec<PathBuf>,
    pub tcp: Vec<String>,
    pub udp: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    pub ingester: IngesterConfig,
    pub sources: SourcesConfig,
}

#[derive(Debug, Default)]
pub struct PartialConfig {
    pub data_dir: Option<PathBuf>,
    pub config_file: Option<PathBuf>,
    pub memtable_flush_threshold: Option<usize>,
    pub compaction_threshold: Option<usize>,
    pub compaction_enabled: Option<bool>,
    pub flush_mode: Option<FlushMode>,
    pub segment_size: Option<usize>,
    pub channel_capacity: Option<usize>,
    pub stdin: Option<bool>,
    pub files: Option<Vec<PathBuf>>,
    pub tcp: Option<Vec<String>>,
    pub udp: Option<Vec<String>>,
}

impl PartialConfig {
    fn merge_from(&mut self, other: Self) {
        if other.data_dir.is_some() {
            self.data_dir = other.data_dir;
        }
        if other.config_file.is_some() {
            self.config_file = other.config_file;
        }
        if other.memtable_flush_threshold.is_some() {
            self.memtable_flush_threshold = other.memtable_flush_threshold;
        }
        if other.compaction_threshold.is_some() {
            self.compaction_threshold = other.compaction_threshold;
        }
        if other.compaction_enabled.is_some() {
            self.compaction_enabled = other.compaction_enabled;
        }
        if other.flush_mode.is_some() {
            self.flush_mode = other.flush_mode;
        }
        if other.segment_size.is_some() {
            self.segment_size = other.segment_size;
        }
        if other.channel_capacity.is_some() {
            self.channel_capacity = other.channel_capacity;
        }
        if other.stdin.is_some() {
            self.stdin = other.stdin;
        }
        if other.files.is_some() {
            self.files = other.files;
        }
        if other.tcp.is_some() {
            self.tcp = other.tcp;
        }
        if other.udp.is_some() {
            self.udp = other.udp;
        }
    }

    fn into_app_config(self) -> Result<AppConfig, ValidationError> {
        let data_dir = self
            .data_dir
            .ok_or(ValidationError { field: "data_dir", message: "data_dir is required".into() })?;

        let storage = StorageConfig {
            memtable_flush_threshold: self
                .memtable_flush_threshold
                .unwrap_or(DEFAULT_MEMTABLE_THRESHOLD),
            compaction_threshold: self.compaction_threshold.unwrap_or(DEFAULT_COMPACTION_THRESHOLD),
            compaction_enabled: self.compaction_enabled.unwrap_or(true),
        };

        let wal = WalConfig {
            flush_mode: self.flush_mode.unwrap_or(FlushMode::Normal),
            segment_size: self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE),
        };

        let ingester = IngesterConfig {
            channel_capacity: self.channel_capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY),
        };

        let sources = SourcesConfig {
            stdin: self.stdin.unwrap_or_default(),
            files: self.files.unwrap_or_default(),
            tcp: self.tcp.unwrap_or_default(),
            udp: self.udp.unwrap_or_default(),
        };

        Ok(AppConfig { data_dir, storage, wal, ingester, sources })
    }
}

pub fn load_from_toml(path: &Path) -> Result<PartialConfig, Error> {
    let content = fs::read_to_string(path)?;
    let toml_config: TomlConfig = toml::from_str(&content)?;
    Ok(toml_config.into())
}

pub fn load_from_env() -> PartialConfig {
    let mut config = PartialConfig::default();

    if let Some(v) = env_var("DATA_DIR") {
        config.data_dir = Some(expand_path(&v));
    }

    if let Some(v) = env_var("MEMTABLE_FLUSH_THRESHOLD_MIB") {
        config.memtable_flush_threshold = v.parse::<usize>().ok().map(|v| v * MEBI);
    }

    if let Some(v) = env_var("COMPACTION_THRESHOLD") {
        config.compaction_threshold = v.parse().ok();
    }

    if let Some(v) = env_var("COMPACTION_ENABLED") {
        config.compaction_enabled = parse_bool(&v);
    }

    if let Some(v) = env_var("FLUSH_MODE") {
        config.flush_mode = parse_flush_mode(&v);
    }

    if let Some(v) = env_var("SEGMENT_SIZE_MIB") {
        config.segment_size = v.parse::<usize>().ok().map(|v| v * MEBI);
    }

    if let Some(v) = env_var("CHANNEL_CAPACITY") {
        config.channel_capacity = v.parse().ok();
    }

    if let Some(v) = env_var("STDIN") {
        config.stdin = parse_bool(&v);
    }

    // Filtering empty strings handles trailing commas (e.g., "a,b,") and explicitly empty
    // environment variables that should not produce phantom entries.
    if let Some(v) = env_var("FILES") {
        config.files = Some(v.split(',').filter(|s| !s.is_empty()).map(expand_path).collect());
    }

    if let Some(v) = env_var("TCP") {
        config.tcp = Some(v.split(',').filter(|s| !s.is_empty()).map(String::from).collect());
    }

    if let Some(v) = env_var("UDP") {
        config.udp = Some(v.split(',').filter(|s| !s.is_empty()).map(String::from).collect());
    }

    config
}

fn env_var(name: &str) -> Option<String> {
    env::var(format!("{ENV_PREFIX}{name}")).ok()
}

// Returning None on unrecognized input allows the merge system to fall back to defaults
// rather than failing on typos in environment variables.
fn parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

// Returning None on unrecognized input allows the merge system to fall back to defaults
// rather than failing on typos in environment variables.
pub fn parse_flush_mode(s: &str) -> Option<FlushMode> {
    match s.to_lowercase().as_str() {
        "normal" => Some(FlushMode::Normal),
        "cautious" => Some(FlushMode::Cautious),
        "emergency" => Some(FlushMode::Emergency),
        _ => None,
    }
}

#[must_use]
pub fn expand_path(path: &str) -> PathBuf {
    if path == "~" {
        if let Some(home) = env::home_dir() {
            return home;
        }
    } else if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = env::home_dir()
    {
        return home.join(rest);
    }
    PathBuf::from(path)
}

pub fn validate(config: &AppConfig) -> Result<(), ValidationError> {
    if config.storage.memtable_flush_threshold < MIN_MEMTABLE_THRESHOLD {
        return Err(ValidationError {
            field: "memtable_flush_threshold_mib",
            message: "must be at least 1".into(),
        });
    }

    if config.storage.compaction_threshold < MIN_COMPACTION_THRESHOLD {
        return Err(ValidationError {
            field: "compaction_threshold",
            message: format!("must be at least {MIN_COMPACTION_THRESHOLD}"),
        });
    }

    if config.wal.segment_size < MIN_SEGMENT_SIZE {
        return Err(ValidationError {
            field: "segment_size_mib",
            message: "must be at least 1".into(),
        });
    }

    if !config.wal.segment_size.is_multiple_of(SECTOR_ALIGNMENT) {
        return Err(ValidationError {
            field: "segment_size_mib",
            message: "must be aligned to 4 KiB boundaries".into(),
        });
    }

    if config.ingester.channel_capacity < MIN_CHANNEL_CAPACITY {
        return Err(ValidationError {
            field: "channel_capacity",
            message: format!("must be at least {MIN_CHANNEL_CAPACITY}"),
        });
    }

    for addr in &config.sources.tcp {
        if addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(ValidationError {
                field: "tcp",
                message: format!("invalid address: '{addr}'"),
            });
        }
    }

    for addr in &config.sources.udp {
        if addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(ValidationError {
                field: "udp",
                message: format!("invalid address: '{addr}'"),
            });
        }
    }

    Ok(())
}

pub fn load(cli: PartialConfig) -> Result<AppConfig, Error> {
    let mut merged = PartialConfig::default();

    let config_path =
        cli.config_file.clone().or_else(|| cli.data_dir.as_ref().map(|d| d.join(CONFIG_FILE_NAME)));

    if let Some(ref path) = config_path
        && path.exists()
    {
        let toml_config = load_from_toml(path)?;
        merged.merge_from(toml_config);
    }

    let env_config = load_from_env();
    merged.merge_from(env_config);

    merged.merge_from(cli);

    let config = merged.into_app_config()?;
    validate(&config)?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_tilde() {
        let path = expand_path("~/test/path");
        if let Some(home) = env::home_dir() {
            assert_eq!(path, home.join("test/path"));
        }
    }

    #[test]
    fn test_expand_bare_tilde() {
        let path = expand_path("~");
        if let Some(home) = env::home_dir() {
            assert_eq!(path, home);
        }
    }

    #[test]
    fn test_expand_no_tilde() {
        let path = expand_path("/absolute/path");
        assert_eq!(path, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn test_validate_memtable_threshold_too_small() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig { memtable_flush_threshold: 1000, ..Default::default() },
            wal: WalConfig::default(),
            ingester: IngesterConfig::default(),
            sources: SourcesConfig::default(),
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "memtable_flush_threshold_mib");
    }

    #[test]
    fn test_validate_compaction_threshold_too_small() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig { compaction_threshold: 1, ..Default::default() },
            wal: WalConfig::default(),
            ingester: IngesterConfig::default(),
            sources: SourcesConfig::default(),
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "compaction_threshold");
    }

    #[test]
    fn test_validate_segment_size_not_aligned() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig::default(),
            wal: WalConfig { segment_size: MEBI + 1, ..Default::default() },
            ingester: IngesterConfig::default(),
            sources: SourcesConfig::default(),
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "segment_size_mib");
    }

    #[test]
    fn test_validate_invalid_tcp_address() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
            ingester: IngesterConfig::default(),
            sources: SourcesConfig { tcp: vec!["not-an-address".into()], ..Default::default() },
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "tcp");
    }

    #[test]
    fn test_validate_valid_config() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
            ingester: IngesterConfig::default(),
            sources: SourcesConfig { tcp: vec!["127.0.0.1:5514".into()], ..Default::default() },
        };
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_parse_flush_mode() {
        assert_eq!(parse_flush_mode("normal"), Some(FlushMode::Normal));
        assert_eq!(parse_flush_mode("CAUTIOUS"), Some(FlushMode::Cautious));
        assert_eq!(parse_flush_mode("Emergency"), Some(FlushMode::Emergency));
        assert_eq!(parse_flush_mode("invalid"), None);
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("no"), Some(false));
        assert_eq!(parse_bool("invalid"), None);
    }

    #[test]
    fn test_partial_config_merge() {
        let mut base = PartialConfig {
            memtable_flush_threshold: Some(1000),
            compaction_threshold: Some(2),
            ..Default::default()
        };

        let overlay = PartialConfig {
            memtable_flush_threshold: Some(2000),
            channel_capacity: Some(500),
            ..Default::default()
        };

        base.merge_from(overlay);

        assert_eq!(base.memtable_flush_threshold, Some(2000));
        assert_eq!(base.compaction_threshold, Some(2));
        assert_eq!(base.channel_capacity, Some(500));
    }

    #[test]
    fn test_validate_segment_size_too_small() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig::default(),
            wal: WalConfig { segment_size: 1000, ..Default::default() },
            ingester: IngesterConfig::default(),
            sources: SourcesConfig::default(),
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "segment_size_mib");
    }

    #[test]
    fn test_validate_channel_capacity_zero() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
            ingester: IngesterConfig { channel_capacity: 0 },
            sources: SourcesConfig::default(),
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "channel_capacity");
    }

    #[test]
    fn test_validate_invalid_udp_address() {
        let config = AppConfig {
            data_dir: PathBuf::from("/tmp"),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
            ingester: IngesterConfig::default(),
            sources: SourcesConfig { udp: vec!["not-an-address".into()], ..Default::default() },
        };
        let err = validate(&config).unwrap_err();
        assert_eq!(err.field, "udp");
    }

    #[test]
    fn test_toml_empty_file() {
        let toml_str = "";
        let toml_config: TomlConfig = toml::from_str(toml_str).unwrap();
        let partial: PartialConfig = toml_config.into();
        assert!(partial.memtable_flush_threshold.is_none());
        assert!(partial.flush_mode.is_none());
    }

    #[test]
    fn test_toml_full_config() {
        let toml_str = r#"
[storage]
memtable_flush_threshold_mib = 2
compaction_threshold = 8
compaction_enabled = false

[wal]
flush_mode = "cautious"
segment_size_mib = 32

[ingester]
channel_capacity = 512

[sources]
stdin = true
files = ["~/logs/app.log"]
tcp = ["127.0.0.1:5514"]
udp = ["0.0.0.0:5515"]
"#;
        let toml_config: TomlConfig = toml::from_str(toml_str).unwrap();
        let partial: PartialConfig = toml_config.into();

        assert_eq!(partial.memtable_flush_threshold, Some(2 * MEBI));
        assert_eq!(partial.compaction_threshold, Some(8));
        assert_eq!(partial.compaction_enabled, Some(false));
        assert_eq!(partial.flush_mode, Some(FlushMode::Cautious));
        assert_eq!(partial.segment_size, Some(32 * MEBI));
        assert_eq!(partial.channel_capacity, Some(512));
        assert_eq!(partial.stdin, Some(true));
        assert!(partial.files.is_some());
        assert_eq!(partial.tcp, Some(vec!["127.0.0.1:5514".into()]));
        assert_eq!(partial.udp, Some(vec!["0.0.0.0:5515".into()]));
    }

    #[test]
    fn test_toml_unknown_section_rejected() {
        let toml_str = r#"
[unknown_section]
foo = "bar"
"#;
        let result: Result<TomlConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_toml_unknown_key_rejected() {
        let toml_str = r"
[storage]
unknown_key = 123
";
        let result: Result<TomlConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_toml_invalid_flush_mode_uses_default() {
        let toml_str = r#"
[wal]
flush_mode = "invalid_mode"
"#;
        let toml_config: TomlConfig = toml::from_str(toml_str).unwrap();
        let partial: PartialConfig = toml_config.into();
        assert!(partial.flush_mode.is_none());
    }
}
