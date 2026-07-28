mod schema;

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use schema::{APPLICATION_ID, COMPONENT_VERSION, PRODUCT_ID, SCHEMA_SQL, SCHEMA_VERSION};

const CONTROL_DIR: &str = "control-v1";
const STORE_FILE: &str = "store.sqlite3";
const OWNER_LOCK_FILE: &str = "store.sqlite3.owner.lock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectControlLayout {
    pub(crate) state_root: PathBuf,
    pub(crate) runtime_root: PathBuf,
    pub(crate) config_file: PathBuf,
}

impl ProjectControlLayout {
    pub(crate) fn discover() -> Result<Self> {
        let state_home = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(dirs::state_dir)
            .ok_or_else(|| anyhow!("XDG state home is unavailable"))?;
        let runtime_home = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("XDG_RUNTIME_DIR is required for durable project control"))?;
        let config_home = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(dirs::config_dir)
            .ok_or_else(|| anyhow!("XDG config home is unavailable"))?;
        Ok(Self::from_app_roots(
            state_home.join("hcom-project-control"),
            runtime_home.join("hcom-project-control"),
            config_home.join("hcom-project-control/config.toml"),
        ))
    }

    pub(crate) fn from_app_roots(
        state_root: impl AsRef<Path>,
        runtime_root: impl AsRef<Path>,
        config_file: impl AsRef<Path>,
    ) -> Self {
        Self {
            state_root: state_root.as_ref().to_path_buf(),
            runtime_root: runtime_root.as_ref().to_path_buf(),
            config_file: config_file.as_ref().to_path_buf(),
        }
    }

    pub(crate) fn control_root(&self) -> PathBuf {
        self.state_root.join(CONTROL_DIR)
    }

    pub(crate) fn store_path(&self) -> PathBuf {
        self.control_root().join(STORE_FILE)
    }

    pub(crate) fn owner_lock_path(&self) -> PathBuf {
        self.control_root().join(OWNER_LOCK_FILE)
    }

    pub(crate) fn control_socket_path(&self) -> PathBuf {
        self.runtime_root.join("control.sock")
    }

    fn validate(&self) -> Result<()> {
        for (label, path) in [
            ("state root", &self.state_root),
            ("runtime root", &self.runtime_root),
            ("config file", &self.config_file),
        ] {
            if !path.is_absolute() {
                bail!("{label} must be an absolute path");
            }
        }
        if self.state_root == self.runtime_root {
            bail!("state and runtime roots must be distinct");
        }
        Ok(())
    }
}

pub(crate) struct DaemonStore {
    connection: Connection,
    _owner_lock: File,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingArchitectBinding<'a> {
    pub(crate) id: &'a str,
    pub(crate) repo_root: &'a Path,
    pub(crate) architect_name: &'a str,
    pub(crate) architect_adapter: &'a str,
    pub(crate) launch_nonce_hash: &'a str,
    pub(crate) control_capability_hash: &'a str,
    pub(crate) action_set_json: &'a str,
    pub(crate) action_set_hash: &'a str,
}

#[derive(Debug, Clone)]
pub(crate) struct ArchitectProcessBinding<'a> {
    pub(crate) architect_pid: u32,
    pub(crate) architect_process_birth: &'a str,
    pub(crate) bridge_pid: u32,
    pub(crate) bridge_process_birth: &'a str,
    pub(crate) relay_executable_contract_hash: &'a str,
    pub(crate) relay_runtime_scope_hash: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArchitectAuthorization {
    pub(crate) id: String,
    pub(crate) repo_root: String,
    pub(crate) project_id: Option<String>,
    pub(crate) architect_pid: u32,
    pub(crate) architect_process_birth: String,
    pub(crate) bridge_pid: u32,
    pub(crate) bridge_process_birth: String,
    pub(crate) launch_nonce_hash: String,
    pub(crate) control_capability_hash: String,
    pub(crate) architect_native_session_id: Option<String>,
    pub(crate) action_set_json: String,
    pub(crate) action_set_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestReplay {
    New,
    Completed(String),
    InProgress,
    Conflict,
}

type StoredControlRequest = (String, String, String, Option<String>, Option<String>);

impl DaemonStore {
    pub(crate) fn open(layout: &ProjectControlLayout) -> Result<Self> {
        layout.validate()?;
        ensure_private_dir(&layout.state_root)?;
        ensure_private_dir(&layout.control_root())?;
        ensure_private_dir(&layout.runtime_root)?;
        let config_root = layout
            .config_file
            .parent()
            .ok_or_else(|| anyhow!("config file has no parent directory"))?;
        ensure_private_dir(config_root)?;

        let store_path = layout.store_path();
        let existed = store_path.exists();
        if existed {
            preflight_existing_store(&store_path)?;
        }

        let owner_lock = acquire_owner_lock(&layout.owner_lock_path())?;
        let exists_after_lock = store_path.exists();
        if existed != exists_after_lock {
            bail!("Store v1 changed while acquiring its owner lock");
        }
        if exists_after_lock {
            preflight_existing_store(&store_path)?;
        }

        let flags = if exists_after_lock {
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW
        };
        let mut connection = Connection::open_with_flags(&store_path, flags)
            .with_context(|| format!("failed to open Store v1 at {}", store_path.display()))?;
        set_private_file_mode(&store_path)?;
        configure_common_connection(&connection)?;

        if exists_after_lock {
            enable_wal(&connection)?;
            verify_store(&connection, true)?;
        } else if let Err(error) = initialize_store(&mut connection) {
            drop(connection);
            cleanup_failed_initialization(&store_path);
            return Err(error);
        } else {
            enable_wal(&connection)?;
        }
        verify_store(&connection, true)?;

        Ok(Self {
            connection,
            _owner_lock: owner_lock,
        })
    }

    pub(crate) fn insert_pending_architect_binding(
        &mut self,
        binding: &PendingArchitectBinding<'_>,
    ) -> Result<()> {
        let now = now_epoch_seconds()?;
        self.connection
            .execute(
                "INSERT INTO architect_bindings (
                     id, version, repo_root, project_id, architect_name,
                     architect_adapter, architect_pid, architect_process_birth,
                     bridge_pid, bridge_process_birth,
                     relay_executable_contract_hash, relay_runtime_scope_hash,
                     launch_nonce_hash, architect_native_session_id, binding_state,
                     control_capability_hash, action_set_json, action_set_hash,
                     created_at, updated_at
                 ) VALUES (
                     ?1, 0, ?2, NULL, ?3, ?4, NULL, NULL, NULL, NULL,
                     NULL, NULL, ?5, NULL, 'pending', ?6, ?7, ?8, ?9, ?9
                 )",
                params![
                    binding.id,
                    binding.repo_root.to_string_lossy(),
                    binding.architect_name,
                    binding.architect_adapter,
                    binding.launch_nonce_hash,
                    binding.control_capability_hash,
                    binding.action_set_json,
                    binding.action_set_hash,
                    now,
                ],
            )
            .context("failed to insert pending architect binding")?;
        Ok(())
    }

    pub(crate) fn bind_architect_process(
        &mut self,
        binding_id: &str,
        expected_version: i64,
        binding: &ArchitectProcessBinding<'_>,
    ) -> Result<()> {
        let now = now_epoch_seconds()?;
        let changed = self
            .connection
            .execute(
                "UPDATE architect_bindings
                 SET architect_pid = ?1,
                     architect_process_birth = ?2,
                     bridge_pid = ?3,
                     bridge_process_birth = ?4,
                     relay_executable_contract_hash = ?5,
                     relay_runtime_scope_hash = ?6,
                     binding_state = 'bound',
                     version = version + 1,
                     updated_at = ?7
                 WHERE id = ?8 AND version = ?9 AND binding_state = 'pending'",
                params![
                    binding.architect_pid,
                    binding.architect_process_birth,
                    binding.bridge_pid,
                    binding.bridge_process_birth,
                    binding.relay_executable_contract_hash,
                    binding.relay_runtime_scope_hash,
                    now,
                    binding_id,
                    expected_version,
                ],
            )
            .context("failed to bind architect process")?;
        if changed != 1 {
            bail!("architect binding CAS failed");
        }
        Ok(())
    }

    pub(crate) fn bind_architect_native_session(
        &mut self,
        binding_id: &str,
        expected_version: i64,
        native_session_id: &str,
    ) -> Result<()> {
        let now = now_epoch_seconds()?;
        let changed = self.connection.execute(
            "UPDATE architect_bindings
             SET architect_native_session_id = ?1,
                 version = version + 1,
                 updated_at = ?2
             WHERE id = ?3 AND version = ?4 AND binding_state = 'bound'
               AND architect_native_session_id IS NULL",
            params![native_session_id, now, binding_id, expected_version],
        )?;
        if changed != 1 {
            bail!("architect native-session CAS failed");
        }
        Ok(())
    }

    pub(crate) fn bind_architect_project(
        &mut self,
        binding_id: &str,
        expected_version: i64,
        project_id: &str,
    ) -> Result<()> {
        let changed = self.connection.execute(
            "UPDATE architect_bindings
             SET project_id = ?1, version = version + 1, updated_at = ?2
             WHERE id = ?3 AND version = ?4 AND binding_state = 'bound'
               AND project_id IS NULL",
            params![
                project_id,
                now_epoch_seconds()?,
                binding_id,
                expected_version
            ],
        )?;
        if changed != 1 {
            bail!("architect project CAS failed");
        }
        Ok(())
    }

    pub(crate) fn architect_authorization(
        &self,
        binding_id: &str,
    ) -> Result<Option<ArchitectAuthorization>> {
        self.connection
            .query_row(
                "SELECT id, repo_root, project_id, architect_pid,
                        architect_process_birth, bridge_pid, bridge_process_birth,
                        launch_nonce_hash, control_capability_hash,
                        architect_native_session_id, action_set_json, action_set_hash
                 FROM architect_bindings
                 WHERE id = ?1 AND binding_state = 'bound'",
                [binding_id],
                |row| {
                    let architect_pid: i64 = row.get(3)?;
                    let bridge_pid: i64 = row.get(5)?;
                    Ok(ArchitectAuthorization {
                        id: row.get(0)?,
                        repo_root: row.get(1)?,
                        project_id: row.get(2)?,
                        architect_pid: u32::try_from(architect_pid).map_err(|_| {
                            rusqlite::Error::IntegralValueOutOfRange(3, architect_pid)
                        })?,
                        architect_process_birth: row.get(4)?,
                        bridge_pid: u32::try_from(bridge_pid)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, bridge_pid))?,
                        bridge_process_birth: row.get(6)?,
                        launch_nonce_hash: row.get(7)?,
                        control_capability_hash: row.get(8)?,
                        architect_native_session_id: row.get(9)?,
                        action_set_json: row.get(10)?,
                        action_set_hash: row.get(11)?,
                    })
                },
            )
            .optional()
            .context("failed to read architect authorization")
    }

    pub(crate) fn begin_control_request(
        &mut self,
        caller_key_hash: &str,
        request_id: &str,
        action: &str,
        payload_hash: &str,
    ) -> Result<RequestReplay> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<StoredControlRequest> = transaction
            .query_row(
                "SELECT action, payload_hash, state, response_json, response_hash
                 FROM control_requests
                 WHERE caller_key_hash = ?1 AND request_id = ?2",
                params![caller_key_hash, request_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let outcome = match existing {
            Some((stored_action, stored_hash, _, _, _))
                if stored_action != action || stored_hash != payload_hash =>
            {
                RequestReplay::Conflict
            }
            Some((_, _, state, Some(response), Some(response_hash))) if state == "completed" => {
                if sha256_hex(response.as_bytes()) != response_hash {
                    bail!("stored control response hash mismatch");
                }
                RequestReplay::Completed(response)
            }
            Some(_) => RequestReplay::InProgress,
            None => {
                transaction.execute(
                    "INSERT INTO control_requests (
                         caller_key_hash, request_id, action, payload_hash, state,
                         response_json, response_hash, created_at, completed_at
                     ) VALUES (?1, ?2, ?3, ?4, 'accepted', NULL, NULL, ?5, NULL)",
                    params![
                        caller_key_hash,
                        request_id,
                        action,
                        payload_hash,
                        now_epoch_seconds()?,
                    ],
                )?;
                RequestReplay::New
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    pub(crate) fn complete_control_request(
        &mut self,
        caller_key_hash: &str,
        request_id: &str,
        payload_hash: &str,
        response_json: &str,
        response_hash: &str,
    ) -> Result<()> {
        let changed = self.connection.execute(
            "UPDATE control_requests
             SET state = 'completed', response_json = ?1, response_hash = ?2,
                 completed_at = ?3
             WHERE caller_key_hash = ?4 AND request_id = ?5
               AND payload_hash = ?6 AND state = 'accepted'",
            params![
                response_json,
                response_hash,
                now_epoch_seconds()?,
                caller_key_hash,
                request_id,
                payload_hash,
            ],
        )?;
        if changed != 1 {
            bail!("control request completion CAS failed");
        }
        Ok(())
    }

    // Phase 2 locks the CAS/audit primitive before later scheduler phases call it.
    #[allow(dead_code)]
    pub(crate) fn transition_project(&mut self, transition: &StateTransition<'_>) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE project_runs
             SET state = ?1, version = version + 1, updated_at = ?2
             WHERE id = ?3 AND version = ?4 AND state = ?5",
            params![
                transition.to_state,
                now_epoch_seconds()?,
                transition.scope_id,
                transition.from_version,
                transition.from_state,
            ],
        )?;
        if changed != 1 {
            bail!("project transition CAS failed");
        }
        insert_transition(&transaction, "project", transition)?;
        transaction.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn transition_task(&mut self, transition: &StateTransition<'_>) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_epoch_seconds()?;
        let completed_at = (transition.to_state == "completed").then_some(now);
        let changed = transaction.execute(
            "UPDATE project_tasks
             SET state = ?1, version = version + 1, updated_at = ?2,
                 completed_at = ?3
             WHERE id = ?4 AND project_id = ?5
               AND version = ?6 AND state = ?7",
            params![
                transition.to_state,
                now,
                completed_at,
                transition.scope_id,
                transition.project_id,
                transition.from_version,
                transition.from_state,
            ],
        )?;
        if changed != 1 {
            bail!("task transition CAS failed");
        }
        insert_transition(&transaction, "task", transition)?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    #[cfg(test)]
    pub(crate) fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }
}

#[allow(dead_code)]
pub(crate) struct StateTransition<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) scope_id: &'a str,
    pub(crate) from_version: i64,
    pub(crate) from_state: &'a str,
    pub(crate) to_state: &'a str,
    pub(crate) action: &'a str,
    pub(crate) actor_kind: &'a str,
    pub(crate) actor_identity: &'a str,
    pub(crate) payload_hash: &'a str,
    pub(crate) turn_id: Option<&'a str>,
    pub(crate) result_hash: Option<&'a str>,
}

#[allow(dead_code)]
fn insert_transition(
    transaction: &rusqlite::Transaction<'_>,
    scope_kind: &str,
    transition: &StateTransition<'_>,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO state_transitions (
             project_id, scope_kind, scope_id, from_version, to_version,
             from_state, to_state, action, actor_kind, actor_identity,
             payload_hash, turn_id, result_hash, created_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?4 + 1, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13
         )",
        params![
            transition.project_id,
            scope_kind,
            transition.scope_id,
            transition.from_version,
            transition.from_state,
            transition.to_state,
            transition.action,
            transition.actor_kind,
            transition.actor_identity,
            transition.payload_hash,
            transition.turn_id,
            transition.result_hash,
            now_epoch_seconds()?,
        ],
    )?;
    Ok(())
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create private directory {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| {
                format!(
                    "private directory is not a real directory: {}",
                    path.display()
                )
            })?;
        // SAFETY: directory owns a live O_DIRECTORY descriptor.
        if unsafe { libc::fchmod(directory.as_raw_fd(), 0o700) } != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("failed to set mode 0700 on {}", path.display()));
        }
        verify_owned_mode(path, &directory.metadata()?, 0o700, true)
    }
    #[cfg(not(unix))]
    {
        let metadata = fs::symlink_metadata(path)?;
        verify_owned_mode(path, &metadata, 0o700, true)
    }
}

fn set_private_file_mode(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("failed to open private file {}", path.display()))?;
        // SAFETY: file owns a live regular-file descriptor.
        if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("failed to set mode 0600 on {}", path.display()));
        }
        verify_owned_mode(path, &file.metadata()?, 0o600, false)?;
    }
    Ok(())
}

#[cfg(unix)]
fn verify_owned_mode(
    path: &Path,
    metadata: &fs::Metadata,
    expected_mode: u32,
    expect_dir: bool,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if metadata.file_type().is_symlink()
        || (expect_dir && !metadata.is_dir())
        || (!expect_dir && !metadata.is_file())
    {
        bail!("unexpected file type for {}", path.display());
    }
    // SAFETY: geteuid has no preconditions.
    let expected_uid = unsafe { libc::geteuid() };
    if metadata.uid() != expected_uid {
        bail!("{} is not owned by the current uid", path.display());
    }
    if metadata.permissions().mode() & 0o777 != expected_mode {
        bail!(
            "{} has mode {:o}, expected {:o}",
            path.display(),
            metadata.permissions().mode() & 0o777,
            expected_mode
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_owned_mode(
    _path: &Path,
    _metadata: &fs::Metadata,
    _expected_mode: u32,
    _expect_dir: bool,
) -> Result<()> {
    Ok(())
}

fn acquire_owner_lock(path: &Path) -> Result<File> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .with_context(|| format!("failed to open Store v1 owner lock {}", path.display()))?;
    set_private_file_mode(path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: flock receives a live file descriptor and non-blocking lock flags.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!("Store v1 already has a live writer");
        }
    }
    Ok(file)
}

fn configure_common_connection(connection: &Connection) -> Result<()> {
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn enable_wal(connection: &Connection) -> Result<()> {
    let journal_mode: String =
        connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        bail!("Store v1 requires SQLite WAL mode");
    }
    Ok(())
}

fn initialize_store(connection: &mut Connection) -> Result<()> {
    connection.pragma_update(None, "application_id", APPLICATION_ID)?;
    connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction
        .execute_batch(SCHEMA_SQL)
        .context("failed to initialize Store v1 schema")?;
    let schema_digest = schema_digest(&transaction)?;
    transaction.execute(
        "INSERT INTO store_meta (
             singleton, product_id, schema_version, installation_id,
             created_by_version, schema_digest, created_at
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            PRODUCT_ID,
            SCHEMA_VERSION,
            Uuid::new_v4().to_string(),
            COMPONENT_VERSION,
            schema_digest,
            now_epoch_seconds()?,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

fn preflight_existing_store(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect existing Store v1 {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("Store v1 path is not a regular file");
    }
    verify_owned_mode(path, &metadata, 0o600, false)?;
    let (application_id, schema_version) = read_sqlite_header(path)?;
    if application_id != APPLICATION_ID {
        bail!("Store v1 application ID mismatch");
    }
    if schema_version != SCHEMA_VERSION {
        bail!("Store v1 schema version mismatch");
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .context("failed to inspect existing Store v1 read-only")?;
    verify_store(&connection, false)
}

fn read_sqlite_header(path: &Path) -> Result<(i32, i32)> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options.open(path)?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header)
        .context("Store v1 has a truncated SQLite header")?;
    if &header[..16] != b"SQLite format 3\0" {
        bail!("Store v1 has an invalid SQLite header");
    }
    let user_version = i32::from_be_bytes(header[60..64].try_into().expect("fixed header slice"));
    let application_id = i32::from_be_bytes(header[68..72].try_into().expect("fixed header slice"));
    Ok((application_id, user_version))
}

fn verify_store(connection: &Connection, require_runtime_pragmas: bool) -> Result<()> {
    let application_id: i32 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    let user_version: i32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if application_id != APPLICATION_ID || user_version != SCHEMA_VERSION {
        bail!("Store v1 header identity mismatch");
    }
    let meta: (String, i32, String, String, String, i64) = connection
        .query_row(
            "SELECT product_id, schema_version, created_by_version, schema_digest,
                    installation_id, created_at
             FROM store_meta WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .context("Store v1 metadata is missing or malformed")?;
    if meta.0 != PRODUCT_ID
        || meta.1 != SCHEMA_VERSION
        || meta.2 != COMPONENT_VERSION
        || meta.3 != expected_schema_digest()?
        || meta.3 != schema_digest(connection)?
        || Uuid::parse_str(&meta.4).is_err()
        || meta.5 <= 0
    {
        bail!("Store v1 component/schema metadata mismatch");
    }
    let attached_count: i64 = connection.query_row(
        "SELECT count(*) FROM pragma_database_list WHERE name NOT IN ('main', 'temp')",
        [],
        |row| row.get(0),
    )?;
    if attached_count != 0 {
        bail!("Store v1 must not ATTACH another database");
    }
    if require_runtime_pragmas {
        let foreign_keys: i64 =
            connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        if foreign_keys != 1 || synchronous != 2 || !journal_mode.eq_ignore_ascii_case("wal") {
            bail!("Store v1 runtime pragma contract is not active");
        }
    }
    let quick_check: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if quick_check != "ok" {
        bail!("Store v1 quick_check failed");
    }
    let foreign_key_errors: i64 =
        connection.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        bail!("Store v1 foreign-key integrity check failed");
    }
    Ok(())
}

fn expected_schema_digest() -> Result<String> {
    let connection = Connection::open_in_memory()?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.execute_batch(SCHEMA_SQL)?;
    schema_digest(&connection)
}

fn schema_digest(connection: &Connection) -> Result<String> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, sql
         FROM sqlite_schema
         WHERE name NOT LIKE 'sqlite_%'
         ORDER BY type, name, tbl_name",
    )?;
    let mut rows = statement.query([])?;
    let mut hasher = Sha256::new();
    while let Some(row) = rows.next()? {
        for value in [
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ] {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
    Ok(hex_bytes(&hasher.finalize()))
}

fn now_epoch_seconds() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_secs()).context("system clock does not fit SQLite integer")
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn cleanup_failed_initialization(store_path: &Path) {
    for path in [
        store_path.with_extension("sqlite3-wal"),
        store_path.with_extension("sqlite3-shm"),
        store_path.to_path_buf(),
    ] {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::io::Write;

    fn layout(temp: &tempfile::TempDir) -> ProjectControlLayout {
        ProjectControlLayout::from_app_roots(
            temp.path().join("state/hcom-project-control"),
            temp.path().join("run/hcom-project-control"),
            temp.path().join("config/hcom-project-control/config.toml"),
        )
    }

    fn sha(character: char) -> String {
        std::iter::repeat_n(character, 40).collect()
    }

    fn digest(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }

    #[test]
    fn empty_store_initializes_with_v1_identity() {
        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        let store = DaemonStore::open(&layout).expect("empty Store v1 must initialize");

        assert_eq!(
            store
                .connection()
                .query_row("PRAGMA application_id", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            APPLICATION_ID
        );
        assert_eq!(
            store
                .connection()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let meta: (String, i32, String) = store
            .connection()
            .query_row(
                "SELECT product_id, schema_version, schema_digest
                 FROM store_meta WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(meta.0, PRODUCT_ID);
        assert_eq!(meta.1, SCHEMA_VERSION);
        assert_eq!(meta.2, expected_schema_digest().unwrap());
        assert_eq!(
            store
                .connection()
                .query_row(
                    "SELECT count(*) FROM pragma_database_list
                     WHERE name NOT IN ('main', 'temp')",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn schema_inventory_is_complete_and_has_no_retained_tables() {
        let temp = tempfile::tempdir().unwrap();
        let store = DaemonStore::open(&layout(&temp)).unwrap();
        let mut statement = store
            .connection()
            .prepare(
                "SELECT name FROM sqlite_schema
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                 ORDER BY name",
            )
            .unwrap();
        let tables: Vec<String> = statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            tables,
            vec![
                "architect_bindings",
                "control_requests",
                "project_apply_ops",
                "project_plans",
                "project_runs",
                "project_tasks",
                "state_transitions",
                "store_meta",
                "task_dependencies",
                "worker_profiles",
                "worker_sessions",
                "worker_turns",
            ]
        );
        for retained in [
            "instances",
            "events",
            "review_runs",
            "messages",
            "bundles",
            "review_workers",
        ] {
            assert!(!tables.iter().any(|table| table == retained));
        }
        assert!(!SCHEMA_SQL.to_ascii_uppercase().contains("ATTACH"));
    }

    #[test]
    fn malformed_or_mixed_store_fails_before_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        ensure_private_dir(&layout.control_root()).unwrap();
        let path = layout.store_path();
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "application_id", 7).unwrap();
        connection.pragma_update(None, "user_version", 24).unwrap();
        connection
            .execute("CREATE TABLE sentinel(value TEXT NOT NULL)", [])
            .unwrap();
        connection
            .execute("INSERT INTO sentinel VALUES ('unchanged')", [])
            .unwrap();
        drop(connection);
        set_private_file_mode(&path).unwrap();
        let before = file_sha256(&path);

        let error = DaemonStore::open(&layout).err().unwrap().to_string();
        assert!(error.contains("application ID mismatch"), "{error}");
        assert_eq!(file_sha256(&path), before);
        let connection =
            Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT value FROM sentinel", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "unchanged"
        );
    }

    #[test]
    fn store_v1_initialization_never_reads_or_writes_a_v24_path() {
        let temp = tempfile::tempdir().unwrap();
        let legacy_root = temp.path().join("legacy-hcom");
        ensure_private_dir(&legacy_root).unwrap();
        let legacy_path = legacy_root.join("hcom.db");
        let legacy = Connection::open(&legacy_path).unwrap();
        legacy.pragma_update(None, "user_version", 24).unwrap();
        legacy
            .execute(
                "CREATE TABLE instances(id TEXT PRIMARY KEY, status TEXT NOT NULL)",
                [],
            )
            .unwrap();
        legacy
            .execute(
                "INSERT INTO instances VALUES ('dev1-test', 'listening')",
                [],
            )
            .unwrap();
        drop(legacy);
        set_private_file_mode(&legacy_path).unwrap();
        let legacy_before = file_sha256(&legacy_path);

        let layout = layout(&temp);
        let store = DaemonStore::open(&layout).unwrap();
        assert_eq!(file_sha256(&legacy_path), legacy_before);
        assert_eq!(
            store
                .connection()
                .query_row(
                    "SELECT count(*) FROM pragma_database_list
                     WHERE file = ?1",
                    [legacy_path.to_string_lossy().as_ref()],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let legacy =
            Connection::open_with_flags(&legacy_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            legacy
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            24
        );
        assert_eq!(
            legacy
                .query_row("SELECT status FROM instances", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "listening"
        );
    }

    #[cfg(unix)]
    #[test]
    fn store_layout_uses_private_directory_and_file_modes() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        let _store = DaemonStore::open(&layout).unwrap();
        for path in [
            layout.state_root.as_path(),
            layout.control_root().as_path(),
            layout.runtime_root.as_path(),
            layout.config_file.parent().unwrap(),
        ] {
            assert_eq!(
                fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        for path in [layout.store_path(), layout.owner_lock_path()] {
            assert_eq!(
                fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn schema_tamper_and_second_writer_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        let first = DaemonStore::open(&layout).unwrap();
        let lock_error = DaemonStore::open(&layout).err().unwrap().to_string();
        assert!(lock_error.contains("live writer"), "{lock_error}");
        drop(first);

        let connection = Connection::open(layout.store_path()).unwrap();
        connection
            .execute("CREATE TABLE unexpected_component(value INTEGER)", [])
            .unwrap();
        drop(connection);
        let error = DaemonStore::open(&layout).err().unwrap().to_string();
        assert!(
            error.contains("metadata mismatch") || error.contains("application ID mismatch"),
            "{error}"
        );
    }

    #[test]
    fn old_store_version_is_rejected_without_migration_or_repair() {
        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        drop(DaemonStore::open(&layout).unwrap());
        let connection = Connection::open(layout.store_path()).unwrap();
        connection.pragma_update(None, "user_version", 0).unwrap();
        drop(connection);
        let before = file_sha256(&layout.store_path());

        let error = DaemonStore::open(&layout).err().unwrap().to_string();
        assert!(error.contains("schema version mismatch"), "{error}");
        assert_eq!(file_sha256(&layout.store_path()), before);
    }

    #[test]
    fn illegal_state_and_immutable_session_binding_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = DaemonStore::open(&layout(&temp)).unwrap();
        seed_project_plan_task(&mut store);

        let illegal = StateTransition {
            project_id: "project-1",
            scope_id: "project-1",
            from_version: 0,
            from_state: "draft",
            to_state: "running",
            action: "skip_approval",
            actor_kind: "human",
            actor_identity: "test",
            payload_hash: &digest('a'),
            turn_id: None,
            result_hash: None,
        };
        assert!(
            store
                .transition_project(&illegal)
                .unwrap_err()
                .to_string()
                .contains("illegal project state transition")
        );

        store
            .connection_mut()
            .execute(
                "INSERT INTO worker_sessions (
                     id, project_id, task_id, role, profile_id, adapter,
                     native_session_id, state, created_at, closed_at, updated_at
                 ) VALUES (
                     'session-1', 'project-1', 'task-1', 'developer',
                     'profile-dev', 'fake', NULL, 'creating', 1, NULL, 1
                 )",
                [],
            )
            .unwrap();
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_sessions
                     SET state = 'active', updated_at = 2
                     WHERE id = 'session-1'",
                    [],
                )
                .is_err(),
            "a discovered session must bind its exact native ID before activation"
        );
        assert!(
            store
                .connection_mut()
                .execute(
                    "INSERT INTO worker_turns (
                         id, session_id, sequence, kind, task_version, review_round,
                         request_hash, status, attempt, lease_owner, expires_at,
                         worker_pid, process_birth, progress_phase, last_progress_at,
                         activity_truncated, artifact_dir, result_json, result_hash,
                         error_kind, error_message, created_at, started_at, result_at,
                         applied_at, updated_at
                     ) VALUES (
                         'turn-gap', 'session-trigger', 2, 'resume', 1, 0,
                         ?1, 'queued', 0, NULL, NULL, NULL, NULL, 'queued', NULL,
                         0, 'project-1/task-1/turn-gap', NULL, NULL, NULL, NULL,
                         2, NULL, NULL, NULL, 2
                     )",
                    [digest('4')],
                )
                .is_err()
        );
        store
            .connection_mut()
            .execute(
                "UPDATE worker_sessions
                 SET native_session_id = 'native-1', state = 'active', updated_at = 2
                 WHERE id = 'session-1'",
                [],
            )
            .unwrap();
        let rebound = store.connection_mut().execute(
            "UPDATE worker_sessions
             SET native_session_id = 'native-2', updated_at = 3
             WHERE id = 'session-1'",
            [],
        );
        assert!(rebound.is_err());
    }

    #[test]
    fn typed_schema_triggers_reject_cross_scope_and_skip_state_mutations() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = DaemonStore::open(&layout(&temp)).unwrap();
        seed_project_plan_task(&mut store);

        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_profiles SET adapter = 'other'
                     WHERE id = 'profile-dev'",
                    [],
                )
                .is_err()
        );
        store
            .connection_mut()
            .execute(
                "UPDATE project_plans
                 SET state = 'approved', approved_at = 2
                 WHERE id = 'plan-1'",
                [],
            )
            .unwrap();
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE project_plans
                     SET state = 'draft', approved_at = NULL
                     WHERE id = 'plan-1'",
                    [],
                )
                .is_err()
        );
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE project_tasks
                     SET state = 'completed', version = 1, completed_at = 2,
                         updated_at = 2
                     WHERE id = 'task-1'",
                    [],
                )
                .is_err()
        );

        let queued = StateTransition {
            project_id: "project-1",
            scope_id: "task-1",
            from_version: 0,
            from_state: "draft",
            to_state: "queued",
            action: "task_enqueue",
            actor_kind: "scheduler",
            actor_identity: "daemon:test",
            payload_hash: &digest('3'),
            turn_id: None,
            result_hash: None,
        };
        store.transition_task(&queued).unwrap();
        store
            .connection_mut()
            .execute(
                "INSERT INTO worker_sessions (
                     id, project_id, task_id, role, profile_id, adapter,
                     native_session_id, state, created_at, closed_at, updated_at
                 ) VALUES (
                     'session-trigger', 'project-1', 'task-1', 'developer',
                     'profile-dev', 'fake', NULL, 'creating', 2, NULL, 2
                 )",
                [],
            )
            .unwrap();
        store
            .connection_mut()
            .execute(
                "INSERT INTO worker_turns (
                     id, session_id, sequence, kind, task_version, review_round,
                     request_hash, status, attempt, lease_owner, expires_at,
                     worker_pid, process_birth, progress_phase, last_progress_at,
                     activity_truncated, artifact_dir, result_json, result_hash,
                     error_kind, error_message, created_at, started_at, result_at,
                     applied_at, updated_at
                 ) VALUES (
                     'turn-1', 'session-trigger', 1, 'create', 1, 0,
                     ?1, 'queued', 0, NULL, NULL, NULL, NULL, 'queued', NULL,
                     0, 'project-1/task-1/turn-1', NULL, NULL, NULL, NULL,
                     2, NULL, NULL, NULL, 2
                 )",
                [digest('4')],
            )
            .unwrap();
        store
            .connection_mut()
            .execute(
                "UPDATE worker_sessions
                 SET native_session_id = 'native-trigger', state = 'active', updated_at = 3
                 WHERE id = 'session-trigger'",
                [],
            )
            .unwrap();
        assert!(
            store
                .connection_mut()
                .execute(
                    "INSERT INTO worker_turns (
                         id, session_id, sequence, kind, task_version, review_round,
                         request_hash, status, attempt, lease_owner, expires_at,
                         worker_pid, process_birth, progress_phase, last_progress_at,
                         activity_truncated, artifact_dir, result_json, result_hash,
                         error_kind, error_message, created_at, started_at, result_at,
                         applied_at, updated_at
                     ) VALUES (
                         'turn-overlap', 'session-trigger', 2, 'resume', 1, 0,
                         ?1, 'queued', 0, NULL, NULL, NULL, NULL, 'queued', NULL,
                         0, 'project-1/task-1/turn-overlap', NULL, NULL, NULL, NULL,
                         3, NULL, NULL, NULL, 3
                     )",
                    [digest('5')],
                )
                .is_err(),
            "a resume turn must wait until its predecessor is applied"
        );
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_turns
                     SET status = 'result_ready', progress_phase = 'applying',
                         result_json = '{}', result_hash = ?1, result_at = 3,
                         updated_at = 3
                     WHERE id = 'turn-1'",
                    [digest('6')],
                )
                .is_err()
        );
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE state_transitions SET action = 'rewritten' WHERE scope_id = 'task-1'",
                    [],
                )
                .is_err()
        );

        let old_sha = sha('1');
        let new_sha = sha('2');
        store
            .connection_mut()
            .execute(
                "INSERT INTO project_apply_ops (
                     id, project_id, expected_project_version,
                     expected_target_sha, new_target_sha, state,
                     observed_target_sha, created_at, ref_updated_at, applied_at
                 ) VALUES (
                     'apply-1', 'project-1', 0, ?1, ?2, 'intent',
                     NULL, 2, NULL, NULL
                 )",
                params![old_sha, new_sha],
            )
            .unwrap();
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE project_apply_ops
                     SET state = 'applied', ref_updated_at = 3, applied_at = 3
                     WHERE id = 'apply-1'",
                    [],
                )
                .is_err()
        );

        let actions = r#"["project_get"]"#;
        store
            .insert_pending_architect_binding(&PendingArchitectBinding {
                id: "binding-trigger",
                repo_root: Path::new("/repo"),
                architect_name: "architect-trigger",
                architect_adapter: "codex",
                launch_nonce_hash: &digest('6'),
                control_capability_hash: &digest('7'),
                action_set_json: actions,
                action_set_hash: &sha256_hex(actions.as_bytes()),
            })
            .unwrap();
        let relay_contract_hash = digest('8');
        let relay_scope_hash = digest('9');
        let process = ArchitectProcessBinding {
            architect_pid: 100,
            architect_process_birth: "birth-architect",
            bridge_pid: 101,
            bridge_process_birth: "birth-bridge",
            relay_executable_contract_hash: &relay_contract_hash,
            relay_runtime_scope_hash: &relay_scope_hash,
        };
        store
            .bind_architect_process("binding-trigger", 0, &process)
            .unwrap();
        assert!(
            store
                .bind_architect_process("binding-trigger", 1, &process)
                .is_err()
        );
        store
            .bind_architect_project("binding-trigger", 1, "project-1")
            .unwrap();
        assert!(
            store
                .bind_architect_project("binding-trigger", 2, "project-1")
                .is_err()
        );
        store
            .bind_architect_native_session("binding-trigger", 2, "native-trigger")
            .unwrap();
        assert!(
            store
                .bind_architect_native_session("binding-trigger", 3, "native-rebound")
                .is_err()
        );

        store
            .connection_mut()
            .execute(
                "INSERT INTO project_plans (
                     id, project_id, version, state, base_checkpoint_sha, plan_hash,
                     developer_profile_id, reviewer_profile_id,
                     automatic_through_ordinal, created_by_binding, created_at,
                     approved_at, superseded_at
                 ) VALUES (
                     'plan-2', 'project-1', 2, 'draft', ?1, ?2,
                     'profile-dev', 'profile-review', NULL, NULL, 3, NULL, NULL
                 )",
                params![sha('1'), digest('a')],
            )
            .unwrap();
        store
            .connection_mut()
            .execute(
                "INSERT INTO project_tasks (
                     id, project_id, plan_id, task_key, ordinal, spec_json,
                     spec_hash, state, version, base_revision, head_revision,
                     review_round, max_review_rounds, developer_session_id,
                     reviewer_session_id, result_json, result_hash,
                     created_at, updated_at, completed_at
                 ) VALUES (
                     'task-2', 'project-1', 'plan-2', 'task-two', 0, '{}',
                     ?1, 'draft', 0, NULL, NULL, 0, 5, NULL, NULL, NULL, NULL,
                     3, 3, NULL
                 )",
                [digest('b')],
            )
            .unwrap();
        assert!(
            store
                .connection_mut()
                .execute(
                    "INSERT INTO task_dependencies VALUES ('task-2', 'task-1')",
                    [],
                )
                .is_err()
        );
    }

    #[test]
    fn project_transition_is_cas_and_audit_atomic() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = DaemonStore::open(&layout(&temp)).unwrap();
        seed_project_plan_task(&mut store);
        let transition = StateTransition {
            project_id: "project-1",
            scope_id: "project-1",
            from_version: 0,
            from_state: "draft",
            to_state: "needs_approval",
            action: "plan_replace",
            actor_kind: "architect",
            actor_identity: "binding:test",
            payload_hash: &digest('b'),
            turn_id: None,
            result_hash: None,
        };
        store.transition_project(&transition).unwrap();
        assert!(store.transition_project(&transition).is_err());
        let row: (String, i64) = store
            .connection()
            .query_row(
                "SELECT state, version FROM project_runs WHERE id = 'project-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("needs_approval".into(), 1));
        assert_eq!(
            store
                .connection()
                .query_row("SELECT count(*) FROM state_transitions", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );

        let task_transition = StateTransition {
            project_id: "project-1",
            scope_id: "task-1",
            from_version: 0,
            from_state: "draft",
            to_state: "queued",
            action: "task_enqueue",
            actor_kind: "scheduler",
            actor_identity: "daemon:test",
            payload_hash: &digest('c'),
            turn_id: None,
            result_hash: None,
        };
        store.transition_task(&task_transition).unwrap();
        assert!(store.transition_task(&task_transition).is_err());
        assert_eq!(
            store
                .connection()
                .query_row("SELECT count(*) FROM state_transitions", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
    }

    #[test]
    fn worker_turn_attempt_retry_is_cas_bound_and_rebinds_process() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = DaemonStore::open(&layout(&temp)).unwrap();
        seed_project_plan_task(&mut store);
        let queued = StateTransition {
            project_id: "project-1",
            scope_id: "task-1",
            from_version: 0,
            from_state: "draft",
            to_state: "queued",
            action: "task_enqueue",
            actor_kind: "scheduler",
            actor_identity: "daemon:test",
            payload_hash: &digest('d'),
            turn_id: None,
            result_hash: None,
        };
        store.transition_task(&queued).unwrap();
        store
            .connection_mut()
            .execute(
                "INSERT INTO worker_sessions (
                     id, project_id, task_id, role, profile_id, adapter,
                     native_session_id, state, created_at, closed_at, updated_at
                 ) VALUES (
                     'session-retry', 'project-1', 'task-1', 'developer',
                     'profile-dev', 'fake', NULL, 'creating', 2, NULL, 2
                 )",
                [],
            )
            .unwrap();
        store
            .connection_mut()
            .execute(
                "INSERT INTO worker_turns (
                     id, session_id, sequence, kind, task_version, review_round,
                     request_hash, status, attempt, lease_owner, expires_at,
                     worker_pid, process_birth, progress_phase, last_progress_at,
                     activity_truncated, artifact_dir, result_json, result_hash,
                     error_kind, error_message, created_at, started_at, result_at,
                     applied_at, updated_at
                 ) VALUES (
                     'turn-retry', 'session-retry', 1, 'create', 1, 0,
                     ?1, 'queued', 0, NULL, NULL, NULL, NULL, 'queued', NULL,
                     0, 'project-1/task-1/turn-1', NULL, NULL, NULL, NULL,
                     2, NULL, NULL, NULL, 2
                 )",
                [digest('e')],
            )
            .unwrap();

        assert_eq!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_turns SET attempt = 1, updated_at = 3
                     WHERE id = 'turn-retry' AND attempt = 0 AND status = 'queued'",
                    [],
                )
                .unwrap(),
            1
        );
        store
            .connection_mut()
            .execute(
                "UPDATE worker_turns
                 SET status = 'running', lease_owner = 'lease-1', expires_at = 30,
                     worker_pid = 100, process_birth = 'birth-1',
                     progress_phase = 'spawn', started_at = 3, updated_at = 3
                 WHERE id = 'turn-retry' AND attempt = 1 AND status = 'queued'",
                [],
            )
            .unwrap();
        store
            .connection_mut()
            .execute(
                "UPDATE worker_turns
                 SET status = 'failed', lease_owner = NULL, expires_at = NULL,
                     progress_phase = 'done', error_kind = 'spawn_failed',
                     error_message = 'no model turn started', updated_at = 4
                 WHERE id = 'turn-retry' AND attempt = 1 AND status = 'running'",
                [],
            )
            .unwrap();
        assert_eq!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_turns
                     SET status = 'queued', attempt = 2,
                         worker_pid = NULL, process_birth = NULL,
                         progress_phase = 'queued', last_progress_at = NULL,
                         error_kind = NULL, error_message = NULL,
                         started_at = NULL, result_at = NULL, applied_at = NULL,
                         updated_at = 5
                     WHERE id = 'turn-retry' AND attempt = 1 AND status = 'failed'",
                    [],
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_turns SET status = 'result_ready', updated_at = 6
                     WHERE id = 'turn-retry' AND attempt = 1",
                    [],
                )
                .unwrap(),
            0,
            "a callback from an old attempt must not match the current row"
        );
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_turns SET attempt = 4, updated_at = 6
                     WHERE id = 'turn-retry' AND attempt = 2",
                    [],
                )
                .is_err()
        );
        store
            .connection_mut()
            .execute(
                "UPDATE worker_turns
                 SET status = 'running', lease_owner = 'lease-2', expires_at = 60,
                     worker_pid = 101, process_birth = 'birth-2',
                     progress_phase = 'spawn', started_at = 6, updated_at = 6
                 WHERE id = 'turn-retry' AND attempt = 2 AND status = 'queued'",
                [],
            )
            .unwrap();
        assert!(
            store
                .connection_mut()
                .execute(
                    "UPDATE worker_turns
                     SET worker_pid = 102, process_birth = 'birth-other', updated_at = 7
                     WHERE id = 'turn-retry' AND attempt = 2",
                    [],
                )
                .is_err(),
            "one attempt must not rebind its process identity"
        );
    }

    #[test]
    fn control_request_id_is_idempotent_and_payload_bound() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = DaemonStore::open(&layout(&temp)).unwrap();
        let caller = digest('c');
        let payload = digest('d');
        assert_eq!(
            store
                .begin_control_request(&caller, "req-1", "project_get", &payload)
                .unwrap(),
            RequestReplay::New
        );
        assert_eq!(
            store
                .begin_control_request(&caller, "req-1", "project_get", &payload)
                .unwrap(),
            RequestReplay::InProgress
        );
        let response = r#"{"protocol_version":1,"request_id":"req-1","ok":false}"#;
        store
            .complete_control_request(
                &caller,
                "req-1",
                &payload,
                response,
                &sha256_hex(response.as_bytes()),
            )
            .unwrap();
        assert_eq!(
            store
                .begin_control_request(&caller, "req-1", "project_get", &payload)
                .unwrap(),
            RequestReplay::Completed(response.into())
        );
        assert_eq!(
            store
                .begin_control_request(&caller, "req-1", "project_get", &digest('f'))
                .unwrap(),
            RequestReplay::Conflict
        );
    }

    #[test]
    fn layout_is_independent_of_hcom_dir() {
        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        assert!(!layout.store_path().to_string_lossy().contains(".hcom"));
        assert_ne!(layout.store_path(), temp.path().join("hcom.db"));
        assert!(layout.store_path().ends_with("control-v1/store.sqlite3"));
        assert!(layout.control_socket_path().ends_with("control.sock"));
    }

    fn seed_project_plan_task(store: &mut DaemonStore) {
        let project_sha = sha('1');
        store
            .connection_mut()
            .execute(
                "INSERT INTO project_runs (
                     id, state, version, pause_reason, source_repo_root,
                     source_git_dir_identity, target_ref, target_expected_sha,
                     worktree_root, worktree_branch, checkpoint_sha,
                     applied_target_sha, approved_plan_version, approved_plan_hash,
                     run_requested_at, active_daemon_epoch, created_at, updated_at
                 ) VALUES (
                     'project-1', 'draft', 0, NULL, '/repo', 'git:1',
                     'refs/heads/master', ?1, '/state/worktree',
                     'refs/heads/hcom-project/project-1', ?1,
                     NULL, NULL, NULL, NULL, NULL, 1, 1
                 )",
                [&project_sha],
            )
            .unwrap();
        for (id, role) in [("profile-dev", "developer"), ("profile-review", "reviewer")] {
            store
                .connection_mut()
                .execute(
                    "INSERT INTO worker_profiles (
                         id, project_id, role, adapter, model, reasoning, policy,
                         cli_path, cli_version, adapter_contract_ver,
                         native_session_mode, capability_json, created_at
                     ) VALUES (?1, 'project-1', ?2, 'fake', 'fake-model', 'high',
                               'sandboxed', '/bin/false', '1', 1, 'discovered', '{}', 1)",
                    params![id, role],
                )
                .unwrap();
        }
        store
            .connection_mut()
            .execute(
                "INSERT INTO project_plans (
                     id, project_id, version, state, base_checkpoint_sha, plan_hash,
                     developer_profile_id, reviewer_profile_id,
                     automatic_through_ordinal, created_by_binding, created_at,
                     approved_at, superseded_at
                 ) VALUES (
                     'plan-1', 'project-1', 1, 'draft', ?1, ?2,
                     'profile-dev', 'profile-review', NULL, NULL, 1, NULL, NULL
                 )",
                params![project_sha, digest('1')],
            )
            .unwrap();
        store
            .connection_mut()
            .execute(
                "INSERT INTO project_tasks (
                     id, project_id, plan_id, task_key, ordinal, spec_json,
                     spec_hash, state, version, base_revision, head_revision,
                     review_round, max_review_rounds, developer_session_id,
                     reviewer_session_id, result_json, result_hash,
                     created_at, updated_at, completed_at
                 ) VALUES (
                     'task-1', 'project-1', 'plan-1', 'task-one', 0, '{}',
                     ?1, 'draft', 0, NULL, NULL, 0, 5, NULL, NULL, NULL, NULL,
                     1, 1, NULL
                 )",
                [digest('2')],
            )
            .unwrap();
    }

    fn file_sha256(path: &Path) -> String {
        let mut file = File::open(path).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        sha256_hex(&bytes)
    }

    #[test]
    fn truncated_store_header_is_rejected_without_repair() {
        let temp = tempfile::tempdir().unwrap();
        let layout = layout(&temp);
        ensure_private_dir(&layout.control_root()).unwrap();
        let path = layout.store_path();
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all(b"SQLite").unwrap();
        drop(file);
        set_private_file_mode(&path).unwrap();
        let before = fs::read(&path).unwrap();
        assert!(DaemonStore::open(&layout).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}
