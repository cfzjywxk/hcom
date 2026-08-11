//! GitHub App key, JWT, and installation-token lifetime contracts.
//!
//! Secret material is accepted only through the strict file-descriptor path
//! below or through the sign-only trait. No API in this module implements
//! `Clone`, `Serialize`, or an exposing `Debug` representation for a secret.

use super::git::GitCredential;
use crate::control_api::{GitHubAppBinding, GitHubAppRole, GitHubPermissionLevel};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::DateTime;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::traits::PublicKeyParts;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::{CString, OsString};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::{Zeroize, Zeroizing};

pub(crate) const MAX_PRIVATE_KEY_BYTES: usize = 64 * 1024;
const MAX_CREDENTIAL_BYTES: usize = 64 * 1024;
const JWT_BACKWARD_SKEW_SECONDS: u64 = 60;
const JWT_LIFETIME_SECONDS: u64 = 600;
pub(crate) const TOKEN_REFRESH_MARGIN_SECONDS: i64 = 60;

/// The narrow seam a future broker/HSM can implement without exposing a
/// parsed private key to the HTTP or workflow layers.
pub(crate) trait SignOnlyRs256: Send + Sync {
    fn sign_rs256(&self, message: &[u8]) -> Result<Vec<u8>>;
}

/// An in-process RSA signer whose private components are zeroized by the
/// RustCrypto `RsaPrivateKey`/`SigningKey` backend when dropped.
pub(crate) struct RsaAppSigner {
    key: SigningKey<Sha256>,
}

impl fmt::Debug for RsaAppSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RsaAppSigner([sign-only private key])")
    }
}

impl RsaAppSigner {
    /// Open and parse an unencrypted PKCS#8 or PKCS#1 RSA PEM. Every path
    /// component is opened with `O_NOFOLLOW`; the final file contract is
    /// checked with `fstat` on the descriptor that is actually read.
    pub(crate) fn open_strict(path: &Path) -> Result<Self> {
        let pem = read_private_key_file(path)?;
        let text = std::str::from_utf8(&pem)
            .map_err(|_| anyhow!("GitHub App private-key file is not valid UTF-8 PEM"))?;
        let key = RsaPrivateKey::from_pkcs8_pem(text)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(text))
            .map_err(|_| anyhow!("GitHub App private-key file is not a supported RSA PEM"))?;
        if key.n().bits() < 2048 {
            bail!("GitHub App RSA private key must contain at least 2048 bits");
        }
        // `pem` is zeroized here, immediately after the parsed zeroizing key
        // has taken ownership of the private components.
        drop(pem);
        Ok(Self {
            key: SigningKey::new(key),
        })
    }

    pub(crate) fn mint_jwt(&self, app_id: u64, now: SystemTime) -> Result<AppJwt> {
        mint_app_jwt(self, app_id, now)
    }
}

impl SignOnlyRs256 for RsaAppSigner {
    fn sign_rs256(&self, message: &[u8]) -> Result<Vec<u8>> {
        Ok(self.key.sign(message).to_vec())
    }
}

/// A serialized App JWT. The value is zeroized on drop and cannot be printed
/// through `Debug`.
pub(crate) struct AppJwt(SecretText);

impl AppJwt {
    pub(super) fn expose(&self) -> &str {
        self.0.expose()
    }
}

impl fmt::Debug for AppJwt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AppJwt([redacted])")
    }
}

/// A metadata-only installation token used just long enough to enumerate the
/// installation's repository selection. It cannot authenticate any other REST
/// endpoint and becomes an exact-repository token only after that list proves
/// there is one positive repository ID.
pub(crate) struct BootstrapInstallationToken {
    token: SecretText,
    expires_at_unix: i64,
    role: GitHubAppRole,
}

impl BootstrapInstallationToken {
    fn from_secret_github_response(
        token: SecretText,
        expires_at: &str,
        role: GitHubAppRole,
        now_unix: i64,
    ) -> Result<Self> {
        if !InstallationOperation::RepositoryMetadata.permits_role(role) {
            bail!("GitHub bootstrap token role does not permit repository metadata");
        }
        Ok(Self {
            token,
            expires_at_unix: validated_installation_expiry(expires_at, now_unix)?,
            role,
        })
    }

    pub(super) fn expose(&self) -> &str {
        self.token.expose()
    }

    fn into_repository_token(self, repository_id: u64) -> Result<InstallationToken> {
        if repository_id == 0 {
            bail!("GitHub installation token repository ID must be positive");
        }
        Ok(InstallationToken {
            token: self.token,
            expires_at_unix: self.expires_at_unix,
            repository_id,
            role: self.role,
            operation: InstallationOperation::RepositoryMetadata,
        })
    }
}

impl fmt::Debug for BootstrapInstallationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapInstallationToken")
            .field("token", &"[redacted]")
            .field("expires_at_unix", &self.expires_at_unix)
            .field("role", &self.role)
            .finish()
    }
}

/// One exact-repository installation token. The returned expiry is retained
/// as an authoritative bound and the token bytes are zeroized at drop.
pub(crate) struct InstallationToken {
    token: SecretText,
    expires_at_unix: i64,
    repository_id: u64,
    role: GitHubAppRole,
    operation: InstallationOperation,
}

impl fmt::Debug for InstallationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstallationToken")
            .field("token", &"[redacted]")
            .field("expires_at_unix", &self.expires_at_unix)
            .field("repository_id", &self.repository_id)
            .field("role", &self.role)
            .field("operation", &self.operation)
            .finish()
    }
}

impl InstallationToken {
    pub(crate) fn from_github_response(
        token: String,
        expires_at: &str,
        repository_id: u64,
        role: GitHubAppRole,
        operation: InstallationOperation,
        now_unix: i64,
    ) -> Result<Self> {
        let token = SecretText::new(token)?;
        Self::from_secret_github_response(
            token,
            expires_at,
            repository_id,
            role,
            operation,
            now_unix,
        )
    }

    fn from_secret_github_response(
        token: SecretText,
        expires_at: &str,
        repository_id: u64,
        role: GitHubAppRole,
        operation: InstallationOperation,
        now_unix: i64,
    ) -> Result<Self> {
        if repository_id == 0 {
            bail!("GitHub installation token repository ID must be positive");
        }
        if !operation.permits_role(role) {
            bail!("GitHub installation token role does not permit the operation");
        }
        let expires_at_unix = validated_installation_expiry(expires_at, now_unix)?;
        Ok(Self {
            token,
            expires_at_unix,
            repository_id,
            role,
            operation,
        })
    }

    pub(super) fn expose(&self) -> &str {
        self.token.expose()
    }

    pub(crate) fn is_fresh_at(&self, now_unix: i64) -> bool {
        now_unix.saturating_add(TOKEN_REFRESH_MARGIN_SECONDS) < self.expires_at_unix
    }

    pub(crate) fn repository_id(&self) -> u64 {
        self.repository_id
    }

    pub(crate) fn role(&self) -> GitHubAppRole {
        self.role
    }

    pub(crate) fn operation(&self) -> InstallationOperation {
        self.operation
    }

    /// Git is the only subprocess allowed to consume a token. This creates a
    /// second, independently zeroizing owner for the short askpass operation;
    /// no token is placed in argv or environment.
    pub(crate) fn git_credential(&self) -> Result<GitCredential> {
        GitCredential::new(self.expose().as_bytes().to_vec())
    }
}

fn validated_installation_expiry(expires_at: &str, now_unix: i64) -> Result<i64> {
    if now_unix < 0 {
        bail!("system clock is before the Unix epoch");
    }
    let expires_at_unix = DateTime::parse_from_rfc3339(expires_at)
        .map_err(|_| anyhow!("GitHub installation token expiry is not valid RFC 3339"))?
        .timestamp();
    if expires_at_unix <= now_unix.saturating_add(TOKEN_REFRESH_MARGIN_SECONDS) {
        bail!("GitHub installation token expires inside the refresh safety margin");
    }
    // GitHub currently returns one-hour tokens. A two-hour ceiling avoids
    // accidentally retaining an unbounded credential if a response is
    // malformed or the remote contract changes.
    if expires_at_unix > now_unix.saturating_add(7_200) {
        bail!("GitHub installation token expiry exceeds the bounded lifetime");
    }
    Ok(expires_at_unix)
}

struct SecretText(Zeroizing<String>);

impl SecretText {
    fn new(mut value: String) -> Result<Self> {
        let invalid = value.is_empty()
            || value.len() > MAX_CREDENTIAL_BYTES
            || value.as_bytes().contains(&0)
            || value.contains(['\r', '\n']);
        if invalid {
            value.zeroize();
            bail!("GitHub credential is empty, unbounded, or not one opaque value");
        }
        Ok(Self(Zeroizing::new(value)))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for SecretText {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(|_| serde::de::Error::custom("invalid bounded credential"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstallationOperation {
    RepositoryMetadata,
    RepositoryAndRefRead,
    RulesetAttestation,
    GitFetch,
    GitPush,
    PullRequestCreate,
    PullRequestRead,
    DeveloperComment,
    DeveloperCommentRead,
    TerminalComment,
    ReviewPublish,
    ReviewRead,
    CheckPublish,
    CheckRead,
    Merge,
    RemoteRefCleanup,
}

impl InstallationOperation {
    pub(crate) fn requires_protected_auto_merge(self) -> bool {
        matches!(
            self,
            Self::RulesetAttestation | Self::Merge | Self::RemoteRefCleanup
        )
    }

    pub(crate) fn permits_role(self, role: GitHubAppRole) -> bool {
        match self {
            Self::RepositoryMetadata => true,
            Self::RepositoryAndRefRead => {
                matches!(role, GitHubAppRole::Architect | GitHubAppRole::Developer)
            }
            Self::RulesetAttestation
            | Self::CheckPublish
            | Self::CheckRead
            | Self::Merge
            | Self::RemoteRefCleanup
            | Self::TerminalComment => role == GitHubAppRole::Architect,
            Self::GitFetch
            | Self::GitPush
            | Self::PullRequestCreate
            | Self::DeveloperComment
            | Self::DeveloperCommentRead => role == GitHubAppRole::Developer,
            Self::PullRequestRead => {
                matches!(role, GitHubAppRole::Architect | GitHubAppRole::Developer)
            }
            Self::ReviewPublish | Self::ReviewRead => {
                matches!(role, GitHubAppRole::Reviewer1 | GitHubAppRole::Reviewer2)
            }
        }
    }

    pub(crate) fn minimum_permissions(self) -> &'static [(&'static str, GitHubPermissionLevel)] {
        use GitHubPermissionLevel::{Read, Write};
        match self {
            Self::RepositoryMetadata => &[],
            Self::RepositoryAndRefRead | Self::GitFetch => &[("contents", Read)],
            Self::RulesetAttestation => &[("administration", Read)],
            Self::GitPush | Self::RemoteRefCleanup => &[("contents", Write)],
            Self::PullRequestCreate
            | Self::DeveloperComment
            | Self::TerminalComment
            | Self::ReviewPublish => &[("pull_requests", Write)],
            Self::PullRequestRead | Self::DeveloperCommentRead | Self::ReviewRead => {
                &[("pull_requests", Read)]
            }
            Self::CheckPublish => &[("checks", Write)],
            Self::CheckRead => &[("checks", Read)],
            Self::Merge => &[("contents", Write), ("pull_requests", Write)],
        }
    }
}

/// The exact body sent to GitHub's installation-token endpoint. It contains
/// one repository ID and only the operation-minimum permission map, never the
/// App's frozen registration superset.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct InstallationTokenRequest {
    repository_ids: [u64; 1],
    permissions: BTreeMap<String, GitHubPermissionLevel>,
    #[serde(skip)]
    role: GitHubAppRole,
    #[serde(skip)]
    operation: InstallationOperation,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct BootstrapInstallationTokenRequest {
    permissions: BTreeMap<String, GitHubPermissionLevel>,
}

impl InstallationTokenRequest {
    pub(crate) fn for_operation(
        repository_id: u64,
        role: GitHubAppRole,
        app: &GitHubAppBinding,
        operation: InstallationOperation,
    ) -> Result<Self> {
        if repository_id == 0 || !operation.permits_role(role) {
            bail!("GitHub App role is not authorized for the requested operation");
        }
        let mut permissions = BTreeMap::new();
        for &(name, level) in operation.minimum_permissions() {
            if !app
                .effective_permissions
                .get(name)
                .is_some_and(|actual| actual.satisfies(level))
            {
                bail!(
                    "{} GitHub App lacks the permission required for this operation",
                    role.as_str()
                );
            }
            permissions.insert(name.to_owned(), level);
        }
        Ok(Self {
            repository_ids: [repository_id],
            permissions,
            role,
            operation,
        })
    }

    pub(crate) fn repository_id(&self) -> u64 {
        self.repository_ids[0]
    }

    pub(crate) fn role(&self) -> GitHubAppRole {
        self.role
    }

    pub(crate) fn operation(&self) -> InstallationOperation {
        self.operation
    }

    pub(crate) fn permissions(&self) -> &BTreeMap<String, GitHubPermissionLevel> {
        &self.permissions
    }
}

#[derive(Deserialize)]
struct InstallationTokenResponse {
    token: SecretText,
    expires_at: String,
    repositories: Vec<TokenRepository>,
    permissions: BTreeMap<String, GitHubPermissionLevel>,
}

#[derive(Deserialize)]
struct BootstrapInstallationTokenResponse {
    token: SecretText,
    expires_at: String,
    permissions: BTreeMap<String, GitHubPermissionLevel>,
}

#[derive(Deserialize)]
struct InstallationRepositoriesResponse {
    total_count: u64,
    repositories: Vec<TokenRepository>,
}

#[derive(Deserialize)]
struct TokenRepository {
    id: u64,
}

/// Mint one operation token with an App JWT, then prove GitHub returned only
/// the requested repository and exact minimum permission map. The response
/// body's owned buffer is zeroizing in the REST client, and the moved token
/// string immediately enters its long-lived zeroizing owner here.
pub(crate) fn mint_installation_token(
    client: &super::client::GitHubRestClient,
    jwt: &AppJwt,
    app: &GitHubAppBinding,
    request: &InstallationTokenRequest,
    now_unix: i64,
) -> Result<InstallationToken> {
    let response: InstallationTokenResponse = client
        .send_json(
            super::client::RestEndpoint::CreateInstallationToken {
                installation_id: app.installation_id,
            },
            super::client::GitHubAuthentication::App(jwt),
            request,
        )
        .map_err(anyhow::Error::new)
        .context("bounded GitHub installation-token request failed")?;
    if response.repositories.len() != 1 || response.repositories[0].id != request.repository_id() {
        bail!("GitHub installation token is not scoped to the exact repository");
    }
    if !matches_requested_permissions(&response.permissions, request.permissions()) {
        bail!("GitHub installation token permissions differ from the operation minimum");
    }
    InstallationToken::from_secret_github_response(
        response.token,
        &response.expires_at,
        request.repository_id(),
        request.role(),
        request.operation(),
        now_unix,
    )
}

/// Bootstrap a metadata-only token before the numeric repository ID is known.
/// GitHub does not include `repositories` in an unscoped token response, so the
/// token may authenticate only the bounded installation-repositories endpoint.
/// That listing must contain exactly one repository. The configured owner/name
/// is then read with the resulting exact-repository token and must resolve to
/// that same ID. Every later token is scoped by the frozen numeric ID.
pub(crate) fn mint_bootstrap_repository_token(
    client: &super::client::GitHubRestClient,
    jwt: &AppJwt,
    installation_id: u64,
    repository: &str,
    role: GitHubAppRole,
    now_unix: i64,
) -> Result<(InstallationToken, u64)> {
    if installation_id == 0 {
        bail!("GitHub App installation ID must be positive");
    }
    super::validate_slug("GitHub bootstrap repository", repository)?;
    let request = BootstrapInstallationTokenRequest {
        permissions: BTreeMap::from([("metadata".into(), GitHubPermissionLevel::Read)]),
    };
    let response: BootstrapInstallationTokenResponse = client
        .send_json(
            super::client::RestEndpoint::CreateInstallationToken { installation_id },
            super::client::GitHubAuthentication::App(jwt),
            &request,
        )
        .map_err(anyhow::Error::new)
        .context("bounded GitHub bootstrap-token request failed")?;
    if response.permissions != request.permissions {
        bail!("GitHub bootstrap token permissions differ from metadata-only access");
    }
    let bootstrap = BootstrapInstallationToken::from_secret_github_response(
        response.token,
        &response.expires_at,
        role,
        now_unix,
    )?;
    let repositories: InstallationRepositoriesResponse = client
        .get(
            super::client::RestEndpoint::InstallationRepositories,
            super::client::GitHubAuthentication::BootstrapInstallation(&bootstrap),
        )
        .map_err(anyhow::Error::new)
        .context("bounded GitHub installation-repositories request failed")?;
    let repository_id = exact_bootstrap_repository_id(&repositories)?;
    let token = bootstrap.into_repository_token(repository_id)?;
    Ok((token, repository_id))
}

fn exact_bootstrap_repository_id(response: &InstallationRepositoriesResponse) -> Result<u64> {
    if response.total_count != 1
        || response.repositories.len() != 1
        || response.repositories[0].id == 0
    {
        bail!("GitHub App installation must select only one exact repository");
    }
    Ok(response.repositories[0].id)
}

fn matches_requested_permissions(
    observed: &BTreeMap<String, GitHubPermissionLevel>,
    requested: &BTreeMap<String, GitHubPermissionLevel>,
) -> bool {
    let mut normalized = observed.clone();
    if normalized
        .remove("metadata")
        .is_some_and(|level| level != GitHubPermissionLevel::Read)
    {
        return false;
    }
    normalized == *requested
}

#[derive(Serialize)]
struct AppClaims {
    iat: u64,
    exp: u64,
    iss: u64,
}

fn mint_app_jwt(signer: &dyn SignOnlyRs256, app_id: u64, now: SystemTime) -> Result<AppJwt> {
    if app_id == 0 {
        bail!("GitHub App ID must be positive before JWT issuance");
    }
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("system clock is before the Unix epoch"))?
        .as_secs();
    let iat = now
        .checked_sub(JWT_BACKWARD_SKEW_SECONDS)
        .ok_or_else(|| anyhow!("system clock cannot accommodate GitHub JWT skew"))?;
    let exp = iat
        .checked_add(JWT_LIFETIME_SECONDS)
        .ok_or_else(|| anyhow!("system clock overflows the GitHub JWT lifetime"))?;
    if exp <= iat || exp.saturating_sub(iat) > JWT_LIFETIME_SECONDS {
        bail!("GitHub JWT lifetime is invalid");
    }

    let header = Zeroizing::new(URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#));
    let claims_json = Zeroizing::new(
        serde_json::to_vec(&AppClaims {
            iat,
            exp,
            iss: app_id,
        })
        .context("failed to serialize bounded GitHub JWT claims")?,
    );
    let claims = Zeroizing::new(URL_SAFE_NO_PAD.encode(claims_json.as_slice()));
    let signing_input = Zeroizing::new(format!("{}.{}", header.as_str(), claims.as_str()));
    let mut signature = Zeroizing::new(
        signer
            .sign_rs256(signing_input.as_bytes())
            .map_err(|_| anyhow!("GitHub App RS256 signing failed"))?,
    );
    let encoded_signature = Zeroizing::new(URL_SAFE_NO_PAD.encode(signature.as_slice()));
    // Erase the raw signature before constructing the serialized credential.
    signature.fill(0);
    let serialized = format!("{}.{}", signing_input.as_str(), encoded_signature.as_str());
    Ok(AppJwt(SecretText::new(serialized)?))
}

fn read_private_key_file(path: &Path) -> Result<Zeroizing<Vec<u8>>> {
    if !path.is_absolute() || path.as_os_str().as_bytes().len() > 4_096 {
        bail!("GitHub App private-key path must be absolute");
    }
    let components = path
        .components()
        .map(|component| match component {
            Component::RootDir => Ok(None),
            Component::Normal(value) => Ok(Some(value.to_os_string())),
            _ => Err(anyhow!("GitHub App private-key path must be normalized")),
        })
        .collect::<Result<Vec<Option<OsString>>>>()?;
    let names = components.into_iter().flatten().collect::<Vec<_>>();
    if names.is_empty() {
        bail!("GitHub App private-key path must name a file");
    }

    // SAFETY: the static C string is NUL terminated and `open` returns a new
    // owned descriptor on success.
    let root_fd = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root_fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to open the root directory for private-key validation");
    }
    // SAFETY: `root_fd` was just returned by `open` and has not been adopted.
    let mut directory = unsafe { OwnedFd::from_raw_fd(root_fd) };
    let euid = unsafe { libc::geteuid() };

    for name in &names[..names.len() - 1] {
        let name = CString::new(name.as_bytes())
            .map_err(|_| anyhow!("GitHub App private-key path contains NUL"))?;
        // SAFETY: both descriptors/name pointers are valid for this call.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error())
                .context("GitHub App private-key path component is unavailable or unsafe");
        }
        // SAFETY: `fd` is fresh and is adopted exactly once.
        let next = unsafe { OwnedFd::from_raw_fd(fd) };
        directory = next;
    }

    let parent_metadata = File::from(directory.try_clone()?).metadata()?;
    if parent_metadata.uid() != euid || parent_metadata.mode() & 0o077 != 0 {
        bail!(
            "GitHub App private-key parent must be current-user-owned and inaccessible to group/world"
        );
    }

    let file_name = CString::new(names.last().expect("non-empty path").as_bytes())
        .map_err(|_| anyhow!("GitHub App private-key path contains NUL"))?;
    // SAFETY: the parent descriptor and filename are valid; `O_NOFOLLOW`
    // binds validation to the actual final descriptor rather than a prior stat.
    let file_fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if file_fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to open GitHub App private-key file safely");
    }
    // SAFETY: `file_fd` is fresh and is adopted exactly once.
    let mut file = unsafe { File::from_raw_fd(file_fd) };
    let before = file.metadata()?;
    if !before.is_file()
        || before.uid() != euid
        || before.mode() & 0o7777 != 0o600
        || before.len() == 0
        || before.len() > MAX_PRIVATE_KEY_BYTES as u64
    {
        bail!(
            "GitHub App private-key file must be a current-user-owned regular mode-0600 file of 1..=65536 bytes"
        );
    }
    let mut pem = Zeroizing::new(Vec::with_capacity(before.len() as usize));
    (&mut file)
        .take(MAX_PRIVATE_KEY_BYTES as u64 + 1)
        .read_to_end(&mut pem)
        .context("failed to read GitHub App private-key file")?;
    let after = file.metadata()?;
    if (
        before.dev(),
        before.ino(),
        before.uid(),
        before.mode(),
        before.len(),
    ) != (
        after.dev(),
        after.ino(),
        after.uid(),
        after.mode(),
        after.len(),
    ) || pem.len() != before.len() as usize
    {
        bail!("GitHub App private-key file changed while it was opened");
    }
    Ok(pem)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use std::fs;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::thread;

    fn app(permissions: &[(&str, GitHubPermissionLevel)]) -> GitHubAppBinding {
        GitHubAppBinding {
            app_id: 1,
            installation_id: 2,
            slug: "fixture-app".into(),
            bot_user_id: 3,
            effective_permissions: permissions
                .iter()
                .map(|(name, level)| ((*name).into(), *level))
                .collect(),
        }
    }

    fn fixture_key() -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let secret = temp.path().join("secret");
        fs::create_dir(&secret).unwrap();
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o700)).unwrap();
        let key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let path = secret.join("fixture.pem");
        fs::write(&path, pem.as_bytes()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        (temp, path)
    }

    #[test]
    fn strict_pem_open_and_rs256_jwt_are_bounded_and_redacted() {
        fn assert_zeroizing_backend<T: zeroize::ZeroizeOnDrop>() {}
        assert_zeroizing_backend::<SigningKey<Sha256>>();
        let (_temp, path) = fixture_key();
        let signer = RsaAppSigner::open_strict(&path).unwrap();
        let jwt = signer
            .mint_jwt(42, UNIX_EPOCH + std::time::Duration::from_secs(1_000))
            .unwrap();
        assert_eq!(jwt.expose().split('.').count(), 3);
        assert_eq!(
            String::from_utf8(
                URL_SAFE_NO_PAD
                    .decode(jwt.expose().split('.').next().unwrap())
                    .unwrap()
            )
            .unwrap(),
            r#"{"alg":"RS256","typ":"JWT"}"#
        );
        let claims: serde_json::Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(jwt.expose().split('.').nth(1).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claims["iss"], 42);
        assert_eq!(claims["iat"], 940);
        assert_eq!(claims["exp"], 1540);
        assert_eq!(format!("{jwt:?}"), "AppJwt([redacted])");
        assert!(!format!("{signer:?}").contains("PRIVATE"));
    }

    #[test]
    fn strict_pem_open_rejects_modes_and_symlinks_without_secret_echo() {
        let (_temp, path) = fixture_key();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        let error = RsaAppSigner::open_strict(&path).unwrap_err();
        assert!(error.to_string().contains("mode-0600"));

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let link = path.with_file_name("link.pem");
        symlink(&path, &link).unwrap();
        let error = RsaAppSigner::open_strict(&link).unwrap_err();
        assert!(!format!("{error:#}").contains("BEGIN PRIVATE KEY"));

        let linked_parent = path.parent().unwrap().with_file_name("linked-secret");
        symlink(path.parent().unwrap(), &linked_parent).unwrap();
        assert!(RsaAppSigner::open_strict(&linked_parent.join("fixture.pem")).is_err());

        let fifo = path.with_file_name("fixture.fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: the path is a valid temporary C string and the fixture is
        // removed with its owning temporary directory.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        assert!(RsaAppSigner::open_strict(&fifo).is_err());

        fs::set_permissions(path.parent().unwrap(), fs::Permissions::from_mode(0o750)).unwrap();
        assert!(RsaAppSigner::open_strict(&path).is_err());
    }

    #[test]
    fn operation_tokens_request_only_minimum_permissions_from_stable_superset() {
        let binding = app(&[
            ("administration", GitHubPermissionLevel::Read),
            ("checks", GitHubPermissionLevel::Write),
            ("contents", GitHubPermissionLevel::Write),
            ("issues", GitHubPermissionLevel::Write),
            ("pull_requests", GitHubPermissionLevel::Write),
            ("workflows", GitHubPermissionLevel::Write),
        ]);
        let request = InstallationTokenRequest::for_operation(
            99,
            GitHubAppRole::Architect,
            &binding,
            InstallationOperation::Merge,
        )
        .unwrap();
        assert_eq!(
            request.permissions(),
            &BTreeMap::from([
                ("contents".into(), GitHubPermissionLevel::Write),
                ("pull_requests".into(), GitHubPermissionLevel::Write),
            ])
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["repository_ids"], serde_json::json!([99]));
        assert!(json["permissions"].get("issues").is_none());
        assert!(
            InstallationTokenRequest::for_operation(
                99,
                GitHubAppRole::Reviewer1,
                &binding,
                InstallationOperation::Merge,
            )
            .is_err()
        );
        assert!(matches_requested_permissions(
            &BTreeMap::from([
                ("contents".into(), GitHubPermissionLevel::Write),
                ("metadata".into(), GitHubPermissionLevel::Read),
                ("pull_requests".into(), GitHubPermissionLevel::Write),
            ]),
            request.permissions(),
        ));
        assert!(!matches_requested_permissions(
            &BTreeMap::from([
                ("contents".into(), GitHubPermissionLevel::Write),
                ("issues".into(), GitHubPermissionLevel::Read),
                ("pull_requests".into(), GitHubPermissionLevel::Write),
            ]),
            request.permissions(),
        ));
    }

    #[test]
    fn only_protected_delivery_operations_require_merge_authority() {
        for operation in [
            InstallationOperation::RulesetAttestation,
            InstallationOperation::Merge,
            InstallationOperation::RemoteRefCleanup,
        ] {
            assert!(operation.requires_protected_auto_merge());
        }
        for operation in [
            InstallationOperation::RepositoryAndRefRead,
            InstallationOperation::GitFetch,
            InstallationOperation::GitPush,
            InstallationOperation::PullRequestCreate,
            InstallationOperation::DeveloperComment,
            InstallationOperation::ReviewPublish,
            InstallationOperation::CheckPublish,
            InstallationOperation::TerminalComment,
        ] {
            assert!(!operation.requires_protected_auto_merge());
        }
    }

    #[test]
    fn installation_token_has_no_prefix_assumption_and_honors_expiry_margin() {
        let token = InstallationToken::from_github_response(
            "opaque fixture value !".into(),
            "1970-01-01T00:20:00Z",
            99,
            GitHubAppRole::Developer,
            InstallationOperation::GitPush,
            1_000,
        )
        .unwrap();
        assert!(token.is_fresh_at(1_100));
        assert!(!token.is_fresh_at(1_140));
        assert!(!format!("{token:?}").contains("opaque fixture"));
        assert!(
            InstallationToken::from_github_response(
                "another opaque fixture".into(),
                "1970-01-01T00:20:00Z",
                99,
                GitHubAppRole::Developer,
                InstallationOperation::GitPush,
                -1,
            )
            .is_err()
        );
    }

    #[test]
    fn fake_http_token_mint_proves_exact_repository_and_minimum_permissions() {
        struct FakeSigner;
        impl SignOnlyRs256 for FakeSigner {
            fn sign_rs256(&self, _message: &[u8]) -> Result<Vec<u8>> {
                Ok(vec![7; 256])
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut headers = String::new();
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                    content_length = value.trim().parse().unwrap();
                }
                headers.push_str(&line);
            }
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            let response_body = br#"{"token":"opaque fake response value","expires_at":"1970-01-01T01:16:40Z","repositories":[{"id":99}],"permissions":{"contents":"write"}}"#;
            write!(
                reader.get_mut(),
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            )
            .unwrap();
            reader.get_mut().write_all(response_body).unwrap();
            (headers, body)
        });
        let client = super::super::client::GitHubRestClient::new_for_test(
            reqwest::Url::parse(&format!("http://{address}/")).unwrap(),
        )
        .unwrap();
        let binding = app(&[("contents", GitHubPermissionLevel::Write)]);
        let request = InstallationTokenRequest::for_operation(
            99,
            GitHubAppRole::Developer,
            &binding,
            InstallationOperation::GitPush,
        )
        .unwrap();
        let jwt = mint_app_jwt(
            &FakeSigner,
            binding.app_id,
            UNIX_EPOCH + std::time::Duration::from_secs(1_000),
        )
        .unwrap();
        let token = mint_installation_token(&client, &jwt, &binding, &request, 1_000).unwrap();
        assert_eq!(token.repository_id(), 99);
        assert_eq!(token.role(), GitHubAppRole::Developer);
        assert_eq!(token.operation(), InstallationOperation::GitPush);
        assert!(!format!("{token:?}").contains("opaque fake"));
        let (headers, body) = server.join().unwrap();
        assert!(headers.starts_with("POST /app/installations/2/access_tokens HTTP/1.1\r\n"));
        assert!(headers.contains("authorization: Bearer "));
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["repository_ids"], serde_json::json!([99]));
        assert_eq!(body["permissions"], serde_json::json!({"contents":"write"}));
    }

    #[test]
    fn bootstrap_token_requests_metadata_and_lists_the_exact_installation_repository() {
        struct FakeSigner;
        impl SignOnlyRs256 for FakeSigner {
            fn sign_rs256(&self, _message: &[u8]) -> Result<Vec<u8>> {
                Ok(vec![7; 256])
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let response_bodies: [&[u8]; 2] = [
                br#"{"token":"opaque bootstrap response value","expires_at":"1970-01-01T01:16:40Z","repository_selection":"selected","permissions":{"metadata":"read"}}"#,
                br#"{"total_count":1,"repositories":[{"id":99}]}"#,
            ];
            let mut requests = Vec::new();
            for response_body in response_bodies {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut headers = String::new();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ")
                    {
                        content_length = value.trim().parse().unwrap();
                    }
                    headers.push_str(&line);
                }
                let mut body = vec![0; content_length];
                reader.read_exact(&mut body).unwrap();
                write!(
                    reader.get_mut(),
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    if requests.is_empty() {
                        "201 Created"
                    } else {
                        "200 OK"
                    },
                    response_body.len()
                )
                .unwrap();
                reader.get_mut().write_all(response_body).unwrap();
                requests.push((headers, body));
            }
            requests
        });
        let client = super::super::client::GitHubRestClient::new_for_test(
            reqwest::Url::parse(&format!("http://{address}/")).unwrap(),
        )
        .unwrap();
        let jwt = mint_app_jwt(
            &FakeSigner,
            1,
            UNIX_EPOCH + std::time::Duration::from_secs(1_000),
        )
        .unwrap();
        let (token, repository_id) = mint_bootstrap_repository_token(
            &client,
            &jwt,
            2,
            "repo",
            GitHubAppRole::Architect,
            1_000,
        )
        .unwrap();
        assert_eq!(repository_id, 99);
        assert_eq!(token.repository_id(), 99);
        assert_eq!(token.role(), GitHubAppRole::Architect);
        assert_eq!(token.operation(), InstallationOperation::RepositoryMetadata);
        assert!(!format!("{token:?}").contains("opaque bootstrap"));

        let requests = server.join().unwrap();
        assert!(
            requests[0]
                .0
                .starts_with("POST /app/installations/2/access_tokens HTTP/1.1\r\n")
        );
        let request_body: serde_json::Value = serde_json::from_slice(&requests[0].1).unwrap();
        assert_eq!(
            request_body,
            serde_json::json!({"permissions":{"metadata":"read"}})
        );
        assert!(
            requests[1]
                .0
                .starts_with("GET /installation/repositories?per_page=2&page=1 HTTP/1.1\r\n")
        );
        assert!(
            requests[1]
                .0
                .contains("authorization: Bearer opaque bootstrap response value")
        );
        assert!(requests[1].1.is_empty());
    }

    #[test]
    fn bootstrap_repository_listing_rejects_ambiguous_or_invalid_selections() {
        let request = BootstrapInstallationTokenRequest {
            permissions: BTreeMap::from([("metadata".into(), GitHubPermissionLevel::Read)]),
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({"permissions": {"metadata":"read"}})
        );
        let response = InstallationRepositoriesResponse {
            total_count: 2,
            repositories: vec![TokenRepository { id: 99 }, TokenRepository { id: 100 }],
        };
        assert!(exact_bootstrap_repository_id(&response).is_err());
        let response = InstallationRepositoriesResponse {
            total_count: 1,
            repositories: Vec::new(),
        };
        assert!(exact_bootstrap_repository_id(&response).is_err());
        let response = InstallationRepositoriesResponse {
            total_count: 1,
            repositories: vec![TokenRepository { id: 0 }],
        };
        assert!(exact_bootstrap_repository_id(&response).is_err());
    }

    #[test]
    fn bootstrap_token_cannot_authenticate_repository_routes() {
        let client = super::super::client::GitHubRestClient::new_for_test(
            reqwest::Url::parse("http://127.0.0.1:9/").unwrap(),
        )
        .unwrap();
        let token = BootstrapInstallationToken::from_secret_github_response(
            SecretText::new("opaque bootstrap fixture".into()).unwrap(),
            "1970-01-01T01:16:40Z",
            GitHubAppRole::Architect,
            1_000,
        )
        .unwrap();
        let error = client
            .get::<serde_json::Value>(
                super::super::client::RestEndpoint::Repository {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                super::super::client::GitHubAuthentication::BootstrapInstallation(&token),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            super::super::client::GitHubApiError::CredentialScope { .. }
        ));
        assert!(!format!("{error}").contains("opaque bootstrap"));
    }
}
