//! Closed, blocking GitHub REST client.
//!
//! Production construction fixes the origin to `api.github.com`, uses rustls
//! with native roots, disables redirects, and applies per-request body/time
//! bounds. Tests may replace only the origin while exercising this same code.

use super::auth::{AppJwt, BootstrapInstallationToken, InstallationOperation, InstallationToken};
use super::{validate_branch, validate_slug};
use anyhow::{Result, anyhow, bail};
use reqwest::Url;
use reqwest::blocking::{Client, Response};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue,
    RETRY_AFTER, USER_AGENT,
};
use reqwest::redirect::Policy;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error as StdError;
use std::fmt;
use std::io::{self, Read};
use std::time::{Duration, Instant};
use thiserror::Error;
use zeroize::Zeroizing;

pub(crate) const GITHUB_API_ORIGIN: &str = "https://api.github.com/";
pub(crate) const GITHUB_API_VERSION: &str = "2022-11-28";
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_LIST_PAGES: u16 = 64;
pub(crate) const MAX_LIST_ITEMS: usize = 4_096;
pub(crate) const MAX_RECONCILIATION_BYTES: usize = 64 * 1024 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_REQUEST_ID_BYTES: usize = 128;
const PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestMethod {
    Get,
    Post,
    Patch,
    Put,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportStage {
    RequestSend,
    ResponseBody,
}

impl fmt::Display for TransportStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RequestSend => "request_send",
            Self::ResponseBody => "response_body",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportKind {
    Timeout,
    ConnectTimeout,
    Tls,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    BrokenPipe,
    UnexpectedEof,
    Connect,
    Request,
    Body,
    Decode,
    Io,
    Other,
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Timeout => "timeout",
            Self::ConnectTimeout => "connect_timeout",
            Self::Tls => "tls",
            Self::ConnectionRefused => "connection_refused",
            Self::ConnectionReset => "connection_reset",
            Self::ConnectionAborted => "connection_aborted",
            Self::NotConnected => "not_connected",
            Self::BrokenPipe => "broken_pipe",
            Self::UnexpectedEof => "unexpected_eof",
            Self::Connect => "connect",
            Self::Request => "request",
            Self::Body => "body",
            Self::Decode => "decode",
            Self::Io => "io",
            Self::Other => "other",
        })
    }
}

impl RestMethod {
    fn reqwest(self) -> reqwest::Method {
        match self {
            Self::Get => reqwest::Method::GET,
            Self::Post => reqwest::Method::POST,
            Self::Patch => reqwest::Method::PATCH,
            Self::Put => reqwest::Method::PUT,
            Self::Delete => reqwest::Method::DELETE,
        }
    }

    fn is_mutating(self) -> bool {
        self != Self::Get
    }
}

/// Every REST route the v1 lane can issue. There is deliberately no raw URL,
/// path, or method variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestEndpoint {
    AppIdentity,
    RepositoryInstallation {
        owner: String,
        repository: String,
    },
    CreateInstallationToken {
        installation_id: u64,
    },
    InstallationRepositories,
    Repository {
        owner: String,
        repository: String,
    },
    BotUser {
        login: String,
    },
    Reference {
        owner: String,
        repository: String,
        qualified_ref: String,
    },
    RulesForBranch {
        owner: String,
        repository: String,
        branch: String,
    },
    RepositoryRuleset {
        owner: String,
        repository: String,
        ruleset_id: u64,
    },
    ListPullRequests {
        owner: String,
        repository: String,
        head: String,
        base: String,
        page: u16,
    },
    CreatePullRequest {
        owner: String,
        repository: String,
    },
    PullRequest {
        owner: String,
        repository: String,
        number: u64,
    },
    ListIssueComments {
        owner: String,
        repository: String,
        number: u64,
        page: u16,
    },
    CreateIssueComment {
        owner: String,
        repository: String,
        number: u64,
    },
    ListReviews {
        owner: String,
        repository: String,
        number: u64,
        page: u16,
    },
    CreateReview {
        owner: String,
        repository: String,
        number: u64,
    },
    ListCheckRuns {
        owner: String,
        repository: String,
        head_sha: String,
        page: u16,
    },
    CreateCheckRun {
        owner: String,
        repository: String,
    },
    CheckRun {
        owner: String,
        repository: String,
        check_run_id: u64,
        update: bool,
    },
    MergePullRequest {
        owner: String,
        repository: String,
        number: u64,
    },
    DeleteReference {
        owner: String,
        repository: String,
        qualified_ref: String,
    },
}

impl RestEndpoint {
    pub(crate) fn method(&self) -> RestMethod {
        match self {
            Self::AppIdentity
            | Self::RepositoryInstallation { .. }
            | Self::InstallationRepositories
            | Self::Repository { .. }
            | Self::BotUser { .. }
            | Self::Reference { .. }
            | Self::RulesForBranch { .. }
            | Self::RepositoryRuleset { .. }
            | Self::ListPullRequests { .. }
            | Self::PullRequest { .. }
            | Self::ListIssueComments { .. }
            | Self::ListReviews { .. }
            | Self::ListCheckRuns { .. }
            | Self::CheckRun { update: false, .. } => RestMethod::Get,
            Self::CreateInstallationToken { .. }
            | Self::CreatePullRequest { .. }
            | Self::CreateIssueComment { .. }
            | Self::CreateReview { .. }
            | Self::CreateCheckRun { .. } => RestMethod::Post,
            Self::CheckRun { update: true, .. } => RestMethod::Patch,
            Self::MergePullRequest { .. } => RestMethod::Put,
            Self::DeleteReference { .. } => RestMethod::Delete,
        }
    }

    pub(crate) fn template(&self) -> &'static str {
        match self {
            Self::AppIdentity => "GET /app",
            Self::RepositoryInstallation { .. } => "GET /repos/{owner}/{repo}/installation",
            Self::CreateInstallationToken { .. } => {
                "POST /app/installations/{installation_id}/access_tokens"
            }
            Self::InstallationRepositories => "GET /installation/repositories",
            Self::Repository { .. } => "GET /repos/{owner}/{repo}",
            Self::BotUser { .. } => "GET /users/{bot}",
            Self::Reference { .. } => "GET /repos/{owner}/{repo}/git/ref/{ref}",
            Self::RulesForBranch { .. } => "GET /repos/{owner}/{repo}/rules/branches/{branch}",
            Self::RepositoryRuleset { .. } => "GET /repos/{owner}/{repo}/rulesets/{ruleset_id}",
            Self::ListPullRequests { .. } => "GET /repos/{owner}/{repo}/pulls",
            Self::CreatePullRequest { .. } => "POST /repos/{owner}/{repo}/pulls",
            Self::PullRequest { .. } => "GET /repos/{owner}/{repo}/pulls/{number}",
            Self::ListIssueComments { .. } => "GET /repos/{owner}/{repo}/issues/{number}/comments",
            Self::CreateIssueComment { .. } => {
                "POST /repos/{owner}/{repo}/issues/{number}/comments"
            }
            Self::ListReviews { .. } => "GET /repos/{owner}/{repo}/pulls/{number}/reviews",
            Self::CreateReview { .. } => "POST /repos/{owner}/{repo}/pulls/{number}/reviews",
            Self::ListCheckRuns { .. } => "GET /repos/{owner}/{repo}/commits/{head}/check-runs",
            Self::CreateCheckRun { .. } => "POST /repos/{owner}/{repo}/check-runs",
            Self::CheckRun { update: false, .. } => {
                "GET /repos/{owner}/{repo}/check-runs/{check_id}"
            }
            Self::CheckRun { update: true, .. } => {
                "PATCH /repos/{owner}/{repo}/check-runs/{check_id}"
            }
            Self::MergePullRequest { .. } => "PUT /repos/{owner}/{repo}/pulls/{number}/merge",
            Self::DeleteReference { .. } => "DELETE /repos/{owner}/{repo}/git/refs/{ref}",
        }
    }

    fn expected_status(&self) -> u16 {
        match self {
            Self::CreateInstallationToken { .. }
            | Self::CreatePullRequest { .. }
            | Self::CreateIssueComment { .. }
            | Self::CreateCheckRun { .. } => 201,
            Self::DeleteReference { .. } => 204,
            Self::AppIdentity
            | Self::RepositoryInstallation { .. }
            | Self::InstallationRepositories
            | Self::Repository { .. }
            | Self::BotUser { .. }
            | Self::Reference { .. }
            | Self::RulesForBranch { .. }
            | Self::RepositoryRuleset { .. }
            | Self::ListPullRequests { .. }
            | Self::PullRequest { .. }
            | Self::ListIssueComments { .. }
            | Self::ListReviews { .. }
            | Self::CreateReview { .. }
            | Self::ListCheckRuns { .. }
            | Self::CheckRun { .. }
            | Self::MergePullRequest { .. } => 200,
        }
    }

    fn validate(&self) -> Result<()> {
        let validate_repo = |owner: &str, repository: &str| {
            validate_slug("GitHub owner", owner)?;
            validate_slug("GitHub repository", repository)
        };
        let validate_number = |value: u64| {
            if value == 0 {
                bail!("GitHub object number must be positive");
            }
            Ok(())
        };
        let validate_page = |page: u16| {
            if !(1..=MAX_LIST_PAGES).contains(&page) {
                bail!("GitHub pagination page is outside the bounded range");
            }
            Ok(())
        };
        match self {
            Self::AppIdentity => Ok(()),
            Self::RepositoryInstallation { owner, repository }
            | Self::Repository { owner, repository }
            | Self::CreatePullRequest { owner, repository }
            | Self::CreateCheckRun { owner, repository } => validate_repo(owner, repository),
            Self::CreateInstallationToken { installation_id } => validate_number(*installation_id),
            Self::InstallationRepositories => Ok(()),
            Self::BotUser { login } => {
                let slug = login
                    .strip_suffix("[bot]")
                    .ok_or_else(|| anyhow!("GitHub bot login must end in [bot]"))?;
                validate_slug("GitHub bot login slug", slug)
            }
            Self::Reference {
                owner,
                repository,
                qualified_ref,
            }
            | Self::DeleteReference {
                owner,
                repository,
                qualified_ref,
            } => {
                validate_repo(owner, repository)?;
                validate_qualified_ref(qualified_ref)
            }
            Self::RulesForBranch {
                owner,
                repository,
                branch,
            } => {
                validate_repo(owner, repository)?;
                validate_branch(branch)
            }
            Self::RepositoryRuleset {
                owner,
                repository,
                ruleset_id,
            } => {
                validate_repo(owner, repository)?;
                validate_number(*ruleset_id)
            }
            Self::ListPullRequests {
                owner,
                repository,
                head,
                base,
                page,
            } => {
                validate_repo(owner, repository)?;
                validate_branch(base)?;
                validate_page(*page)?;
                let (head_owner, head_branch) = head
                    .split_once(':')
                    .ok_or_else(|| anyhow!("GitHub PR head selector must be owner:branch"))?;
                validate_slug("GitHub PR head owner", head_owner)?;
                validate_branch(head_branch)
            }
            Self::PullRequest {
                owner,
                repository,
                number,
            }
            | Self::CreateIssueComment {
                owner,
                repository,
                number,
            }
            | Self::CreateReview {
                owner,
                repository,
                number,
            }
            | Self::MergePullRequest {
                owner,
                repository,
                number,
            } => {
                validate_repo(owner, repository)?;
                validate_number(*number)
            }
            Self::ListIssueComments {
                owner,
                repository,
                number,
                page,
            }
            | Self::ListReviews {
                owner,
                repository,
                number,
                page,
            } => {
                validate_repo(owner, repository)?;
                validate_number(*number)?;
                validate_page(*page)
            }
            Self::ListCheckRuns {
                owner,
                repository,
                head_sha,
                page,
            } => {
                validate_repo(owner, repository)?;
                super::validate_git_sha("GitHub Check head", head_sha)?;
                validate_page(*page)
            }
            Self::CheckRun {
                owner,
                repository,
                check_run_id,
                ..
            } => {
                validate_repo(owner, repository)?;
                validate_number(*check_run_id)
            }
        }
    }

    fn url(&self, base: &Url) -> Result<Url> {
        self.validate()?;
        let mut url = base.clone();
        url.set_query(None);
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow!("fixed GitHub API origin cannot accept path segments"))?;
            segments.clear();
            let mut push_repo = |owner: &str, repository: &str| {
                segments.push("repos").push(owner).push(repository);
            };
            match self {
                Self::AppIdentity => {
                    segments.push("app");
                }
                Self::RepositoryInstallation { owner, repository } => {
                    push_repo(owner, repository);
                    segments.push("installation");
                }
                Self::CreateInstallationToken { installation_id } => {
                    segments
                        .push("app")
                        .push("installations")
                        .push(&installation_id.to_string())
                        .push("access_tokens");
                }
                Self::InstallationRepositories => {
                    segments.push("installation").push("repositories");
                }
                Self::Repository { owner, repository } => push_repo(owner, repository),
                Self::BotUser { login } => {
                    segments.push("users").push(login);
                }
                Self::Reference {
                    owner,
                    repository,
                    qualified_ref,
                } => {
                    push_repo(owner, repository);
                    segments.push("git").push("ref");
                    for part in qualified_ref.split('/') {
                        segments.push(part);
                    }
                }
                Self::RulesForBranch {
                    owner,
                    repository,
                    branch,
                } => {
                    push_repo(owner, repository);
                    segments.push("rules").push("branches");
                    for part in branch.split('/') {
                        segments.push(part);
                    }
                }
                Self::RepositoryRuleset {
                    owner,
                    repository,
                    ruleset_id,
                } => {
                    push_repo(owner, repository);
                    segments.push("rulesets").push(&ruleset_id.to_string());
                }
                Self::ListPullRequests {
                    owner, repository, ..
                }
                | Self::CreatePullRequest { owner, repository } => {
                    push_repo(owner, repository);
                    segments.push("pulls");
                }
                Self::PullRequest {
                    owner,
                    repository,
                    number,
                } => {
                    push_repo(owner, repository);
                    segments.push("pulls").push(&number.to_string());
                }
                Self::ListIssueComments {
                    owner,
                    repository,
                    number,
                    ..
                }
                | Self::CreateIssueComment {
                    owner,
                    repository,
                    number,
                } => {
                    push_repo(owner, repository);
                    segments
                        .push("issues")
                        .push(&number.to_string())
                        .push("comments");
                }
                Self::ListReviews {
                    owner,
                    repository,
                    number,
                    ..
                }
                | Self::CreateReview {
                    owner,
                    repository,
                    number,
                } => {
                    push_repo(owner, repository);
                    segments
                        .push("pulls")
                        .push(&number.to_string())
                        .push("reviews");
                }
                Self::ListCheckRuns {
                    owner,
                    repository,
                    head_sha,
                    ..
                } => {
                    push_repo(owner, repository);
                    segments.push("commits").push(head_sha).push("check-runs");
                }
                Self::CreateCheckRun { owner, repository } => {
                    push_repo(owner, repository);
                    segments.push("check-runs");
                }
                Self::CheckRun {
                    owner,
                    repository,
                    check_run_id,
                    ..
                } => {
                    push_repo(owner, repository);
                    segments.push("check-runs").push(&check_run_id.to_string());
                }
                Self::MergePullRequest {
                    owner,
                    repository,
                    number,
                } => {
                    push_repo(owner, repository);
                    segments
                        .push("pulls")
                        .push(&number.to_string())
                        .push("merge");
                }
                Self::DeleteReference {
                    owner,
                    repository,
                    qualified_ref,
                } => {
                    push_repo(owner, repository);
                    segments.push("git").push("refs");
                    for part in qualified_ref.split('/') {
                        segments.push(part);
                    }
                }
            }
        }
        match self {
            Self::InstallationRepositories => {
                url.query_pairs_mut()
                    .append_pair("per_page", "2")
                    .append_pair("page", "1");
            }
            Self::ListPullRequests {
                head, base, page, ..
            } => {
                url.query_pairs_mut()
                    .append_pair("state", "all")
                    .append_pair("head", head)
                    .append_pair("base", base)
                    .append_pair("per_page", &PAGE_SIZE.to_string())
                    .append_pair("page", &page.to_string());
            }
            Self::ListIssueComments { page, .. } | Self::ListReviews { page, .. } => {
                url.query_pairs_mut()
                    .append_pair("per_page", &PAGE_SIZE.to_string())
                    .append_pair("page", &page.to_string());
            }
            Self::ListCheckRuns { page, .. } => {
                url.query_pairs_mut()
                    .append_pair("filter", "all")
                    .append_pair("per_page", &PAGE_SIZE.to_string())
                    .append_pair("page", &page.to_string());
            }
            _ => {}
        }
        Ok(url)
    }

    fn accepts_installation_operation(&self, operation: InstallationOperation) -> bool {
        use InstallationOperation as O;
        match self {
            Self::Repository { .. } | Self::BotUser { .. } => true,
            Self::Reference { .. } => matches!(
                operation,
                O::RepositoryAndRefRead | O::GitFetch | O::GitPush | O::RemoteRefCleanup | O::Merge
            ),
            Self::RulesForBranch { .. } => operation == O::RulesetAttestation,
            Self::RepositoryRuleset { .. } => operation == O::RulesetAttestation,
            Self::ListPullRequests { .. } | Self::PullRequest { .. } => matches!(
                operation,
                O::PullRequestCreate | O::PullRequestRead | O::DeveloperComment | O::Merge
            ),
            Self::CreatePullRequest { .. } => operation == O::PullRequestCreate,
            Self::ListIssueComments { .. } | Self::CreateIssueComment { .. } => {
                matches!(
                    operation,
                    O::DeveloperComment | O::DeveloperCommentRead | O::TerminalComment
                )
            }
            Self::ListReviews { .. } | Self::CreateReview { .. } => {
                matches!(operation, O::ReviewPublish | O::ReviewRead | O::Merge)
            }
            Self::ListCheckRuns { .. } | Self::CheckRun { update: false, .. } => {
                matches!(operation, O::CheckPublish | O::CheckRead | O::Merge)
            }
            Self::CreateCheckRun { .. } | Self::CheckRun { update: true, .. } => {
                operation == O::CheckPublish
            }
            Self::MergePullRequest { .. } => operation == O::Merge,
            Self::DeleteReference { .. } => operation == O::RemoteRefCleanup,
            Self::AppIdentity
            | Self::RepositoryInstallation { .. }
            | Self::CreateInstallationToken { .. }
            | Self::InstallationRepositories => false,
        }
    }
}

fn validate_qualified_ref(value: &str) -> Result<()> {
    let branch = value
        .strip_prefix("heads/")
        .ok_or_else(|| anyhow!("GitHub REST ref must be under heads/"))?;
    validate_branch(branch)
}

pub(crate) enum GitHubAuthentication<'a> {
    App(&'a AppJwt),
    BootstrapInstallation(&'a BootstrapInstallationToken),
    Installation(&'a InstallationToken),
}

impl fmt::Debug for GitHubAuthentication<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::App(_) => formatter.write_str("GitHubAuthentication::App([redacted])"),
            Self::BootstrapInstallation(token) => formatter
                .debug_tuple("GitHubAuthentication::BootstrapInstallation")
                .field(token)
                .finish(),
            Self::Installation(token) => formatter
                .debug_tuple("GitHubAuthentication::Installation")
                .field(token)
                .finish(),
        }
    }
}

pub(crate) struct ApiResponse {
    pub(crate) status: u16,
    pub(crate) request_id: Option<String>,
    method: RestMethod,
    endpoint: &'static str,
    body: Zeroizing<Vec<u8>>,
}

impl fmt::Debug for ApiResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiResponse")
            .field("status", &self.status)
            .field("request_id", &self.request_id)
            .field("method", &self.method)
            .field("endpoint", &self.endpoint)
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

impl ApiResponse {
    pub(crate) fn json<T: DeserializeOwned>(&self) -> std::result::Result<T, GitHubApiError> {
        serde_json::from_slice(&self.body).map_err(|_| GitHubApiError::InvalidResponse {
            endpoint: self.endpoint,
            reason: "response body does not match the required typed JSON shape".into(),
            ambiguous: self.method.is_mutating(),
        })
    }

    pub(crate) fn body_len(&self) -> usize {
        self.body.len()
    }
}

#[derive(Debug, Error)]
pub(crate) enum GitHubApiError {
    #[error(
        "GitHub {endpoint} transport failed (stage={stage}, kind={kind}, elapsed_ms={elapsed_millis}, stage_elapsed_ms={stage_elapsed_millis}, timeout_ms={timeout_millis}, http_status={status:?}, request_id={request_id:?}, retry_after={retry_after_seconds:?}, rate_limit_reset={rate_limit_reset_unix:?}, ambiguous={ambiguous})"
    )]
    Transport {
        method: RestMethod,
        endpoint: &'static str,
        stage: TransportStage,
        kind: TransportKind,
        elapsed_millis: u64,
        stage_elapsed_millis: u64,
        timeout_millis: u64,
        status: Option<u16>,
        request_id: Option<String>,
        retry_after_seconds: Option<u64>,
        rate_limit_reset_unix: Option<u64>,
        ambiguous: bool,
    },
    #[error(
        "GitHub {endpoint} returned HTTP {status} (request_id={request_id:?}, retry_after={retry_after_seconds:?}, rate_limit_reset={rate_limit_reset_unix:?}): {reason}"
    )]
    Http {
        method: RestMethod,
        endpoint: &'static str,
        status: u16,
        request_id: Option<String>,
        retry_after_seconds: Option<u64>,
        rate_limit_reset_unix: Option<u64>,
        reason: String,
    },
    #[error("GitHub {endpoint} response is invalid (ambiguous={ambiguous}): {reason}")]
    InvalidResponse {
        endpoint: &'static str,
        reason: String,
        ambiguous: bool,
    },
    #[error("GitHub {endpoint} exceeded the {bound} bound (ambiguous={ambiguous})")]
    BoundExceeded {
        endpoint: &'static str,
        bound: &'static str,
        ambiguous: bool,
    },
    #[error("GitHub credential scope does not permit {endpoint}")]
    CredentialScope { endpoint: &'static str },
}

impl GitHubApiError {
    pub(crate) fn requires_mutation_reconciliation(&self) -> bool {
        match self {
            Self::Transport {
                ambiguous: true, ..
            }
            | Self::InvalidResponse {
                ambiguous: true, ..
            }
            | Self::BoundExceeded {
                ambiguous: true, ..
            } => true,
            Self::Http { method, status, .. } if method.is_mutating() => {
                matches!(*status, 405 | 408 | 409 | 422 | 429 | 500..=599)
            }
            _ => false,
        }
    }

    pub(crate) fn http_status(&self) -> Option<u16> {
        match self {
            Self::Transport { status, .. } => *status,
            Self::Http { status, .. } => Some(*status),
            _ => None,
        }
    }

    pub(crate) fn is_bound_exceeded(&self) -> bool {
        matches!(self, Self::BoundExceeded { .. })
    }

    pub(crate) fn rate_limit_signals(&self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Transport {
                retry_after_seconds,
                rate_limit_reset_unix,
                ..
            }
            | Self::Http {
                retry_after_seconds,
                rate_limit_reset_unix,
                ..
            } => (*retry_after_seconds, *rate_limit_reset_unix),
            _ => (None, None),
        }
    }

    pub(crate) fn is_retryable_read(&self) -> bool {
        match self {
            Self::Transport {
                method: RestMethod::Get,
                ambiguous: false,
                ..
            } => true,
            Self::Http {
                method: RestMethod::Get,
                status,
                retry_after_seconds,
                rate_limit_reset_unix,
                ..
            } => {
                matches!(*status, 408 | 429 | 500..=599)
                    || (*status == 403
                        && (retry_after_seconds.is_some() || rate_limit_reset_unix.is_some()))
            }
            _ => false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct GitHubRestClient {
    client: Client,
    api_base: Url,
    request_timeout: Duration,
}

impl fmt::Debug for GitHubRestClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitHubRestClient")
            .field("origin", &self.api_base.origin().ascii_serialization())
            .field("redirects", &"disabled")
            .field("request_timeout", &self.request_timeout)
            .field("response_cap", &MAX_RESPONSE_BODY_BYTES)
            .finish()
    }
}

impl GitHubRestClient {
    pub(crate) fn new() -> Result<Self> {
        ensure_rustls_crypto_provider()?;
        let api_base = Url::parse(GITHUB_API_ORIGIN).expect("fixed GitHub API URL is valid");
        let client = client_builder(true, REQUEST_TIMEOUT)
            .build()
            .map_err(|_| anyhow!("failed to build the bounded rustls GitHub HTTP client"))?;
        Ok(Self {
            client,
            api_base,
            request_timeout: REQUEST_TIMEOUT,
        })
    }

    #[cfg(test)]
    pub(super) fn new_for_test(api_base: Url) -> Result<Self> {
        Self::new_for_test_with_timeout(api_base, REQUEST_TIMEOUT)
    }

    #[cfg(test)]
    fn new_for_test_with_timeout(api_base: Url, request_timeout: Duration) -> Result<Self> {
        if api_base.scheme() != "http" || api_base.cannot_be_a_base() {
            bail!("fake GitHub origin must be a hierarchical HTTP URL");
        }
        if request_timeout.is_zero() {
            bail!("fake GitHub request timeout must be positive");
        }
        ensure_rustls_crypto_provider()?;
        let client = client_builder(false, request_timeout)
            .no_proxy()
            .build()
            .map_err(|_| anyhow!("failed to build fake GitHub HTTP client"))?;
        Ok(Self {
            client,
            api_base,
            request_timeout,
        })
    }

    pub(crate) fn get<T: DeserializeOwned>(
        &self,
        endpoint: RestEndpoint,
        auth: GitHubAuthentication<'_>,
    ) -> std::result::Result<T, GitHubApiError> {
        let response = self.execute(endpoint, auth, None)?;
        response.json()
    }

    pub(crate) fn send_json<B: Serialize, T: DeserializeOwned>(
        &self,
        endpoint: RestEndpoint,
        auth: GitHubAuthentication<'_>,
        body: &B,
    ) -> std::result::Result<T, GitHubApiError> {
        let bytes = serde_json::to_vec(body).map_err(|_| GitHubApiError::InvalidResponse {
            endpoint: endpoint.template(),
            reason: "request body could not be serialized".into(),
            ambiguous: false,
        })?;
        if bytes.len() > MAX_REQUEST_BODY_BYTES {
            return Err(GitHubApiError::BoundExceeded {
                endpoint: endpoint.template(),
                bound: "2-MiB request-body",
                ambiguous: false,
            });
        }
        let response = self.execute(endpoint, auth, Some(&bytes))?;
        response.json()
    }

    pub(crate) fn send_json_allow_empty<B: Serialize>(
        &self,
        endpoint: RestEndpoint,
        auth: GitHubAuthentication<'_>,
        body: &B,
    ) -> std::result::Result<ApiResponse, GitHubApiError> {
        let bytes = serde_json::to_vec(body).map_err(|_| GitHubApiError::InvalidResponse {
            endpoint: endpoint.template(),
            reason: "request body could not be serialized".into(),
            ambiguous: false,
        })?;
        if bytes.len() > MAX_REQUEST_BODY_BYTES {
            return Err(GitHubApiError::BoundExceeded {
                endpoint: endpoint.template(),
                bound: "2-MiB request-body",
                ambiguous: false,
            });
        }
        self.execute(endpoint, auth, Some(&bytes))
    }

    pub(crate) fn delete(
        &self,
        endpoint: RestEndpoint,
        auth: GitHubAuthentication<'_>,
    ) -> std::result::Result<ApiResponse, GitHubApiError> {
        self.execute(endpoint, auth, None)
    }

    pub(crate) fn paginated_values<F>(
        &self,
        mut endpoint_for_page: F,
        auth: GitHubAuthentication<'_>,
        array_field: Option<&str>,
    ) -> std::result::Result<Vec<serde_json::Value>, GitHubApiError>
    where
        F: FnMut(u16) -> RestEndpoint,
    {
        let mut items = Vec::new();
        let mut aggregate_bytes = 0usize;
        for page in 1..=MAX_LIST_PAGES {
            let endpoint = endpoint_for_page(page);
            let response = self.execute(endpoint, auth_ref(&auth), None)?;
            aggregate_bytes = aggregate_bytes.checked_add(response.body_len()).ok_or(
                GitHubApiError::BoundExceeded {
                    endpoint: "paginated reconciliation list",
                    bound: "64-MiB aggregate-byte",
                    ambiguous: false,
                },
            )?;
            if aggregate_bytes > MAX_RECONCILIATION_BYTES {
                return Err(GitHubApiError::BoundExceeded {
                    endpoint: "paginated reconciliation list",
                    bound: "64-MiB aggregate-byte",
                    ambiguous: false,
                });
            }
            let value: serde_json::Value = response.json()?;
            let page_items = if let Some(field) = array_field {
                value
                    .get(field)
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| GitHubApiError::InvalidResponse {
                        endpoint: "paginated reconciliation list",
                        reason: "response omitted its bounded item array".into(),
                        ambiguous: false,
                    })?
            } else {
                value
                    .as_array()
                    .ok_or_else(|| GitHubApiError::InvalidResponse {
                        endpoint: "paginated reconciliation list",
                        reason: "response is not a bounded item array".into(),
                        ambiguous: false,
                    })?
            };
            if items.len().saturating_add(page_items.len()) > MAX_LIST_ITEMS {
                return Err(GitHubApiError::BoundExceeded {
                    endpoint: "paginated reconciliation list",
                    bound: "4096-item",
                    ambiguous: false,
                });
            }
            items.extend(page_items.iter().cloned());
            if page_items.len() < PAGE_SIZE {
                return Ok(items);
            }
        }
        Err(GitHubApiError::BoundExceeded {
            endpoint: "paginated reconciliation list",
            bound: "64-page",
            ambiguous: false,
        })
    }

    fn execute(
        &self,
        endpoint: RestEndpoint,
        auth: GitHubAuthentication<'_>,
        body: Option<&[u8]>,
    ) -> std::result::Result<ApiResponse, GitHubApiError> {
        let method = endpoint.method();
        let template = endpoint.template();
        let expected_status = endpoint.expected_status();
        if method == RestMethod::Get && body.is_some()
            || method != RestMethod::Get && body.is_none() && !matches!(method, RestMethod::Delete)
        {
            return Err(GitHubApiError::InvalidResponse {
                endpoint: template,
                reason: "request method/body shape violates the closed endpoint contract".into(),
                ambiguous: false,
            });
        }
        match auth {
            GitHubAuthentication::App(_) => {
                if !matches!(
                    endpoint,
                    RestEndpoint::AppIdentity
                        | RestEndpoint::RepositoryInstallation { .. }
                        | RestEndpoint::CreateInstallationToken { .. }
                ) {
                    return Err(GitHubApiError::CredentialScope { endpoint: template });
                }
            }
            GitHubAuthentication::BootstrapInstallation(_) => {
                if !matches!(endpoint, RestEndpoint::InstallationRepositories) {
                    return Err(GitHubApiError::CredentialScope { endpoint: template });
                }
            }
            GitHubAuthentication::Installation(token) => {
                if !endpoint.accepts_installation_operation(token.operation()) {
                    return Err(GitHubApiError::CredentialScope { endpoint: template });
                }
            }
        }
        let url = endpoint
            .url(&self.api_base)
            .map_err(|_| GitHubApiError::InvalidResponse {
                endpoint: template,
                reason: "validated endpoint parameters could not form a fixed-origin URL".into(),
                ambiguous: false,
            })?;
        let mut request = self.client.request(method.reqwest(), url).header(
            AUTHORIZATION,
            authorization_header(&auth)
                .map_err(|_| GitHubApiError::CredentialScope { endpoint: template })?,
        );
        if let Some(body) = body {
            request = request
                .header(CONTENT_TYPE, "application/json")
                .body(body.to_vec());
        }
        let send_started = Instant::now();
        let response = request.send().map_err(|error| {
            let elapsed_millis = duration_millis(send_started.elapsed());
            GitHubApiError::Transport {
                method,
                endpoint: template,
                stage: TransportStage::RequestSend,
                kind: classify_reqwest_transport(&error),
                elapsed_millis,
                stage_elapsed_millis: elapsed_millis,
                timeout_millis: duration_millis(self.request_timeout),
                status: error.status().map(|status| status.as_u16()),
                request_id: None,
                retry_after_seconds: None,
                rate_limit_reset_unix: None,
                // Even connect-looking failures can be reported by a proxy after
                // it accepted request bytes. Keep mutations ambiguous until the
                // endpoint-specific remote reconciliation proves zero effect.
                ambiguous: method.is_mutating(),
            }
        })?;
        normalize_response(
            method,
            template,
            expected_status,
            response,
            self.request_timeout,
            send_started,
        )
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn classify_reqwest_transport(error: &reqwest::Error) -> TransportKind {
    if error.is_timeout() {
        return if error.is_connect() {
            TransportKind::ConnectTimeout
        } else {
            TransportKind::Timeout
        };
    }
    if error_chain_contains_tls(error) {
        return TransportKind::Tls;
    }
    if let Some(kind) = error_chain_io_kind(error) {
        return kind;
    }
    if error.is_connect() {
        return TransportKind::Connect;
    }
    if error.is_body() {
        return TransportKind::Body;
    }
    if error.is_decode() {
        return TransportKind::Decode;
    }
    if error.is_request() {
        return TransportKind::Request;
    }
    TransportKind::Other
}

fn classify_io_transport(error: &io::Error) -> TransportKind {
    if let Some(kind) = io_transport_kind(error.kind()) {
        return kind;
    }
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<reqwest::Error>() {
            return classify_reqwest_transport(error);
        }
        if cause.downcast_ref::<rustls::Error>().is_some() {
            return TransportKind::Tls;
        }
        if let Some(error) = cause.downcast_ref::<io::Error>()
            && let Some(kind) = io_transport_kind(error.kind())
        {
            return kind;
        }
        source = cause.source();
    }
    TransportKind::Io
}

fn classify_response_body_transport(
    error: &io::Error,
    elapsed: Duration,
    request_timeout: Duration,
) -> TransportKind {
    let kind = classify_io_transport(error);
    if kind == TransportKind::Io && elapsed >= request_timeout {
        // reqwest's blocking adapter can surface its deadline as an opaque
        // std::io::Error without preserving the inner reqwest::Error in the
        // public source chain. The configured deadline plus measured stage
        // duration still distinguishes it deterministically from an immediate
        // protocol/body I/O failure.
        TransportKind::Timeout
    } else {
        kind
    }
}

fn error_chain_contains_tls(error: &(dyn StdError + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(current) = cause {
        if current.downcast_ref::<rustls::Error>().is_some() {
            return true;
        }
        cause = current.source();
    }
    false
}

fn error_chain_io_kind(error: &(dyn StdError + 'static)) -> Option<TransportKind> {
    let mut cause = Some(error);
    while let Some(current) = cause {
        if let Some(error) = current.downcast_ref::<io::Error>()
            && let Some(kind) = io_transport_kind(error.kind())
        {
            return Some(kind);
        }
        cause = current.source();
    }
    None
}

fn io_transport_kind(kind: io::ErrorKind) -> Option<TransportKind> {
    match kind {
        io::ErrorKind::TimedOut => Some(TransportKind::Timeout),
        io::ErrorKind::ConnectionRefused => Some(TransportKind::ConnectionRefused),
        io::ErrorKind::ConnectionReset => Some(TransportKind::ConnectionReset),
        io::ErrorKind::ConnectionAborted => Some(TransportKind::ConnectionAborted),
        io::ErrorKind::NotConnected => Some(TransportKind::NotConnected),
        io::ErrorKind::BrokenPipe => Some(TransportKind::BrokenPipe),
        io::ErrorKind::UnexpectedEof => Some(TransportKind::UnexpectedEof),
        _ => None,
    }
}

fn ensure_rustls_crypto_provider() -> Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // reqwest's no-provider feature lets this crate retain the AWS-LC
        // provider already selected by the relay stack. Installation races are
        // harmless: a losing caller observes the winner below.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        bail!("failed to install the process rustls crypto provider");
    }
    Ok(())
}

fn client_builder(https_only: bool, request_timeout: Duration) -> reqwest::blocking::ClientBuilder {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    headers.insert(USER_AGENT, HeaderValue::from_static("hcom-github-pr-v1"));
    headers.insert(
        "X-GitHub-Api-Version",
        HeaderValue::from_static(GITHUB_API_VERSION),
    );
    let builder = Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(request_timeout)
        .redirect(Policy::none())
        .default_headers(headers);
    if https_only {
        builder.https_only(true)
    } else {
        builder
    }
}

fn auth_ref<'a>(auth: &'a GitHubAuthentication<'a>) -> GitHubAuthentication<'a> {
    match auth {
        GitHubAuthentication::App(jwt) => GitHubAuthentication::App(jwt),
        GitHubAuthentication::BootstrapInstallation(token) => {
            GitHubAuthentication::BootstrapInstallation(token)
        }
        GitHubAuthentication::Installation(token) => GitHubAuthentication::Installation(token),
    }
}

fn authorization_header(auth: &GitHubAuthentication<'_>) -> Result<HeaderValue> {
    let value = match auth {
        GitHubAuthentication::App(jwt) => jwt.expose(),
        GitHubAuthentication::BootstrapInstallation(token) => token.expose(),
        GitHubAuthentication::Installation(token) => token.expose(),
    };
    let bearer = Zeroizing::new(format!("Bearer {value}"));
    HeaderValue::from_bytes(bearer.as_bytes())
        .map_err(|_| anyhow!("GitHub credential cannot form an Authorization header"))
}

fn normalize_response(
    method: RestMethod,
    endpoint: &'static str,
    expected_status: u16,
    mut response: Response,
    request_timeout: Duration,
    request_started: Instant,
) -> std::result::Result<ApiResponse, GitHubApiError> {
    let status = response.status();
    let request_id = bounded_header(response.headers(), "x-github-request-id");
    let retry_after_seconds = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds <= 86_400);
    let rate_limit_reset_unix = (response
        .headers()
        .get("x-ratelimit-remaining")
        .and_then(|value| value.to_str().ok())
        == Some("0"))
    .then(|| {
        response
            .headers()
            .get("x-ratelimit-reset")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value <= i64::MAX as u64)
    })
    .flatten();
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > MAX_RESPONSE_BODY_BYTES as u64
    {
        return Err(GitHubApiError::BoundExceeded {
            endpoint,
            bound: "2-MiB response-body",
            ambiguous: method.is_mutating(),
        });
    }
    if let Some(encoding) = response.headers().get("content-encoding")
        && encoding
            .to_str()
            .map_or(true, |value| !value.eq_ignore_ascii_case("identity"))
    {
        return Err(GitHubApiError::InvalidResponse {
            endpoint,
            reason: "compressed response was returned despite identity-only negotiation".into(),
            ambiguous: method.is_mutating(),
        });
    }
    // Installation-token responses carry a credential in JSON. Zeroize every
    // owned response body, not merely bodies the typed layer already knows to
    // contain a token.
    let mut body = Zeroizing::new(Vec::new());
    let body_started = Instant::now();
    response
        .by_ref()
        .take(MAX_RESPONSE_BODY_BYTES as u64 + 1)
        .read_to_end(&mut body)
        .map_err(|error| {
            let stage_elapsed = body_started.elapsed();
            let elapsed = request_started.elapsed();
            GitHubApiError::Transport {
                method,
                endpoint,
                stage: TransportStage::ResponseBody,
                kind: classify_response_body_transport(&error, elapsed, request_timeout),
                elapsed_millis: duration_millis(elapsed),
                stage_elapsed_millis: duration_millis(stage_elapsed),
                timeout_millis: duration_millis(request_timeout),
                status: Some(status.as_u16()),
                request_id: request_id.clone(),
                retry_after_seconds,
                rate_limit_reset_unix,
                // The request has already returned headers; callers of a mutating
                // endpoint reconcile any body-read failure conservatively.
                ambiguous: method.is_mutating(),
            }
        })?;
    if body.len() > MAX_RESPONSE_BODY_BYTES {
        return Err(GitHubApiError::BoundExceeded {
            endpoint,
            bound: "2-MiB response-body",
            ambiguous: method.is_mutating(),
        });
    }
    if !body.is_empty() && !is_json_content_type(response.headers()) {
        return Err(GitHubApiError::InvalidResponse {
            endpoint,
            reason: "non-empty response is not a GitHub JSON media type".into(),
            ambiguous: method.is_mutating(),
        });
    }
    if !status.is_success() {
        return Err(GitHubApiError::Http {
            method,
            endpoint,
            status: status.as_u16(),
            request_id,
            retry_after_seconds,
            rate_limit_reset_unix,
            reason: sanitized_remote_reason(&body),
        });
    }
    if status.as_u16() != expected_status {
        return Err(GitHubApiError::InvalidResponse {
            endpoint,
            reason: format!(
                "response status {} differs from expected {expected_status}",
                status.as_u16()
            ),
            ambiguous: method.is_mutating(),
        });
    }
    Ok(ApiResponse {
        status: status.as_u16(),
        request_id,
        method,
        endpoint,
        body,
    })
}

fn bounded_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= MAX_REQUEST_ID_BYTES
                && value.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
                })
        })
        .map(str::to_owned)
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|value| value == "application/json" || value.ends_with("+json"))
}

fn sanitized_remote_reason(body: &[u8]) -> String {
    let fallback = "remote API rejected the bounded request".to_owned();
    let Some(message) = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("message")?.as_str().map(str::to_owned))
    else {
        return fallback;
    };
    if message.is_empty()
        || message.len() > 256
        || !message
            .bytes()
            .all(|byte| byte == b' ' || byte.is_ascii_graphic())
    {
        return fallback;
    }
    let lower = message.to_ascii_lowercase();
    if [
        "token",
        "authorization",
        "bearer",
        "private key",
        "jwt",
        "secret",
        "password",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return fallback;
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::GitHubAppRole;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::thread;

    fn token(operation: InstallationOperation) -> InstallationToken {
        InstallationToken::from_github_response(
            "fixture-token-no-real-prefix".into(),
            "2099-01-01T00:00:00Z",
            99,
            match operation {
                InstallationOperation::ReviewPublish | InstallationOperation::ReviewRead => {
                    GitHubAppRole::Reviewer1
                }
                InstallationOperation::RulesetAttestation
                | InstallationOperation::CheckPublish
                | InstallationOperation::CheckRead
                | InstallationOperation::Merge
                | InstallationOperation::RemoteRefCleanup
                | InstallationOperation::TerminalComment => GitHubAppRole::Architect,
                _ => GitHubAppRole::Developer,
            },
            operation,
            4_070_905_200,
        )
        .unwrap()
    }

    fn serve_once(response: Vec<u8>) -> (Url, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                request.push_str(&line);
            }
            reader.get_mut().write_all(&response).unwrap();
            request
        });
        (Url::parse(&format!("http://{address}/")).unwrap(), handle)
    }

    fn serve_transport_case(
        response: Option<Vec<u8>>,
        delay_before_response: Duration,
        hold_after_response: Duration,
    ) -> (Url, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0u8; content_length];
            reader.read_exact(&mut body).unwrap();
            thread::sleep(delay_before_response);
            if let Some(response) = response {
                reader.get_mut().write_all(&response).unwrap();
                reader.get_mut().flush().unwrap();
            }
            thread::sleep(hold_after_response);
        });
        (Url::parse(&format!("http://{address}/")).unwrap(), handle)
    }

    #[test]
    fn typed_client_uses_closed_url_headers_and_bounded_json_path() {
        let body = br#"{"id":99,"private":true}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-GitHub-Request-Id: fixture-1\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect();
        let (origin, server) = serve_once(response);
        let client = GitHubRestClient::new_for_test(origin).unwrap();
        let token = token(InstallationOperation::RepositoryMetadata);
        let value: serde_json::Value = client
            .get(
                RestEndpoint::Repository {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&token),
            )
            .unwrap();
        assert_eq!(value["id"], 99);
        let request = server.join().unwrap();
        assert!(request.starts_with("GET /repos/owner/repo HTTP/1.1\r\n"));
        assert!(request.contains("x-github-api-version: 2022-11-28\r\n"));
        assert!(request.contains("accept-encoding: identity\r\n"));
        assert!(request.contains("authorization: Bearer fixture-token-no-real-prefix\r\n"));
    }

    #[test]
    fn credential_operation_scope_is_enforced_before_network() {
        let client =
            GitHubRestClient::new_for_test(Url::parse("http://127.0.0.1:9/").unwrap()).unwrap();
        let token = token(InstallationOperation::ReviewPublish);
        let error = client
            .get::<serde_json::Value>(
                RestEndpoint::RulesForBranch {
                    owner: "owner".into(),
                    repository: "repo".into(),
                    branch: "master".into(),
                },
                GitHubAuthentication::Installation(&token),
            )
            .unwrap_err();
        assert!(matches!(error, GitHubApiError::CredentialScope { .. }));
        assert!(!format!("{error}").contains("fixture-token"));
    }

    #[test]
    fn redirects_compression_and_oversized_bodies_fail_closed() {
        let responses = [
            b"HTTP/1.1 302 Found\r\nLocation: https://example.com/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".to_vec(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_RESPONSE_BODY_BYTES + 1
            )
            .into_bytes(),
        ];
        for response in responses {
            let (origin, server) = serve_once(response);
            let client = GitHubRestClient::new_for_test(origin).unwrap();
            let token = token(InstallationOperation::RepositoryMetadata);
            let result = client.get::<serde_json::Value>(
                RestEndpoint::Repository {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&token),
            );
            assert!(result.is_err());
            server.join().unwrap();
        }
    }

    #[test]
    fn url_parameters_cannot_replace_the_origin_or_escape_the_allowlist() {
        assert!(
            RestEndpoint::Repository {
                owner: "https://evil.invalid".into(),
                repository: "repo".into(),
            }
            .url(&Url::parse(GITHUB_API_ORIGIN).unwrap())
            .is_err()
        );
        let url = RestEndpoint::Reference {
            owner: "owner".into(),
            repository: "repo".into(),
            qualified_ref: "heads/feature/topic".into(),
        }
        .url(&Url::parse(GITHUB_API_ORIGIN).unwrap())
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.github.com/repos/owner/repo/git/ref/heads/feature/topic"
        );
        let bot_url = RestEndpoint::BotUser {
            login: "hcom-dev[bot]".into(),
        }
        .url(&Url::parse(GITHUB_API_ORIGIN).unwrap())
        .unwrap();
        assert_eq!(
            bot_url.as_str(),
            "https://api.github.com/users/hcom-dev[bot]"
        );
        assert!(
            RestEndpoint::BotUser {
                login: "hcom-dev".into(),
            }
            .url(&Url::parse(GITHUB_API_ORIGIN).unwrap())
            .is_err()
        );
        assert!(
            RestEndpoint::BotUser {
                login: "hcom/dev[bot]".into(),
            }
            .url(&Url::parse(GITHUB_API_ORIGIN).unwrap())
            .is_err()
        );
    }

    #[test]
    fn sensitive_remote_reasons_are_never_reflected() {
        let body = br#"{"message":"bearer fixture-token-no-real-prefix was rejected"}"#;
        assert_eq!(
            sanitized_remote_reason(body),
            "remote API rejected the bounded request"
        );
        assert_eq!(
            sanitized_remote_reason(br#"{"message":"Merge conflict"}"#),
            "Merge conflict"
        );
        super::super::validate_id("fixture", "request-id").unwrap();
    }

    #[test]
    fn endpoint_statuses_and_rate_limit_signals_are_exact_and_sanitized() {
        let body = br#"{"id":1}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect();
        let (origin, server) = serve_once(response);
        let client = GitHubRestClient::new_for_test(origin).unwrap();
        let comment_token = token(InstallationOperation::DeveloperComment);
        let error = client
            .send_json::<_, serde_json::Value>(
                RestEndpoint::CreateIssueComment {
                    owner: "owner".into(),
                    repository: "repo".into(),
                    number: 1,
                },
                GitHubAuthentication::Installation(&comment_token),
                &serde_json::json!({"body":"fixture"}),
            )
            .unwrap_err();
        assert!(matches!(&error, GitHubApiError::InvalidResponse { .. }));
        assert!(format!("{error}").contains("expected 201"));
        assert!(error.requires_mutation_reconciliation());
        server.join().unwrap();

        let body = br#"{"message":"secondary rate limit"}"#;
        let response = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 7\r\nX-RateLimit-Remaining: 0\r\nX-RateLimit-Reset: 4070908800\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect();
        let (origin, server) = serve_once(response);
        let client = GitHubRestClient::new_for_test(origin).unwrap();
        let repository_token = token(InstallationOperation::RepositoryMetadata);
        let error = client
            .get::<serde_json::Value>(
                RestEndpoint::Repository {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&repository_token),
            )
            .unwrap_err();
        assert!(matches!(
            &error,
            GitHubApiError::Http {
                method: RestMethod::Get,
                status: 403,
                retry_after_seconds: Some(7),
                rate_limit_reset_unix: Some(4_070_908_800),
                ..
            }
        ));
        assert!(!error.requires_mutation_reconciliation());
        server.join().unwrap();
    }

    #[test]
    fn mutation_http_failures_that_can_hide_an_effect_require_reconciliation() {
        let error = GitHubApiError::Http {
            method: RestMethod::Post,
            endpoint: "POST /repos/{owner}/{repo}/pulls",
            status: 422,
            request_id: Some("fixture".into()),
            retry_after_seconds: None,
            rate_limit_reset_unix: None,
            reason: "remote API rejected the bounded request".into(),
        };
        assert!(error.requires_mutation_reconciliation());
        assert_eq!(error.http_status(), Some(422));
    }

    #[test]
    fn only_transient_get_failures_are_retryable_reads() {
        let transport = GitHubApiError::Transport {
            method: RestMethod::Get,
            endpoint: "GET /repos/{owner}/{repo}",
            stage: TransportStage::RequestSend,
            kind: TransportKind::Timeout,
            elapsed_millis: 30_000,
            stage_elapsed_millis: 30_000,
            timeout_millis: 30_000,
            status: None,
            request_id: None,
            retry_after_seconds: None,
            rate_limit_reset_unix: None,
            ambiguous: false,
        };
        assert!(transport.is_retryable_read());
        let rate_limited = GitHubApiError::Http {
            method: RestMethod::Get,
            endpoint: "GET /repos/{owner}/{repo}",
            status: 403,
            request_id: None,
            retry_after_seconds: Some(1),
            rate_limit_reset_unix: None,
            reason: "rate limited".into(),
        };
        assert!(rate_limited.is_retryable_read());
        let invalid = GitHubApiError::InvalidResponse {
            endpoint: "GET /repos/{owner}/{repo}",
            reason: "invalid typed response".into(),
            ambiguous: false,
        };
        assert!(!invalid.is_retryable_read());
    }

    #[test]
    fn request_send_timeout_is_classified_before_any_response_headers() {
        let timeout = Duration::from_millis(75);
        let (origin, server) =
            serve_transport_case(None, Duration::from_millis(250), Duration::ZERO);
        let client = GitHubRestClient::new_for_test_with_timeout(origin, timeout).unwrap();
        let token = token(InstallationOperation::PullRequestCreate);
        let error = client
            .send_json::<_, serde_json::Value>(
                RestEndpoint::CreatePullRequest {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&token),
                &serde_json::json!({"title":"fixture"}),
            )
            .unwrap_err();

        assert!(matches!(
            &error,
            GitHubApiError::Transport {
                method: RestMethod::Post,
                stage: TransportStage::RequestSend,
                kind: TransportKind::Timeout,
                timeout_millis: 75,
                status: None,
                request_id: None,
                ambiguous: true,
                ..
            }
        ));
        let detail = error.to_string();
        assert!(detail.contains("stage=request_send"));
        assert!(detail.contains("kind=timeout"));
        server.join().unwrap();
    }

    #[test]
    fn connection_refusal_is_distinct_from_a_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let origin = Url::parse(&format!("http://{address}/")).unwrap();
        let client =
            GitHubRestClient::new_for_test_with_timeout(origin, Duration::from_millis(500))
                .unwrap();
        let token = token(InstallationOperation::RepositoryMetadata);
        let error = client
            .get::<serde_json::Value>(
                RestEndpoint::Repository {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&token),
            )
            .unwrap_err();

        assert!(
            matches!(
                &error,
                GitHubApiError::Transport {
                    method: RestMethod::Get,
                    stage: TransportStage::RequestSend,
                    kind: TransportKind::ConnectionRefused,
                    status: None,
                    ambiguous: false,
                    ..
                }
            ),
            "unexpected connection diagnostic: {error:?} / {error}"
        );
    }

    #[test]
    fn truncated_response_body_retains_headers_and_body_stage() {
        let response = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 64\r\nX-GitHub-Request-Id: fixture-truncated\r\nConnection: close\r\n\r\n{\"id\":".to_vec();
        let (origin, server) = serve_transport_case(Some(response), Duration::ZERO, Duration::ZERO);
        let client = GitHubRestClient::new_for_test(origin).unwrap();
        let token = token(InstallationOperation::PullRequestCreate);
        let error = client
            .send_json::<_, serde_json::Value>(
                RestEndpoint::CreatePullRequest {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&token),
                &serde_json::json!({"title":"fixture"}),
            )
            .unwrap_err();

        assert!(matches!(
            &error,
            GitHubApiError::Transport {
                method: RestMethod::Post,
                stage: TransportStage::ResponseBody,
                kind: TransportKind::Body | TransportKind::UnexpectedEof,
                status: Some(201),
                request_id: Some(request_id),
                ambiguous: true,
                ..
            } if request_id == "fixture-truncated"
        ));
        let detail = error.to_string();
        assert!(detail.contains("stage=response_body"));
        assert!(detail.contains("http_status=Some(201)"));
        assert!(detail.contains("fixture-truncated"));
        server.join().unwrap();
    }

    #[test]
    fn response_body_timeout_is_distinct_from_request_send_timeout() {
        let timeout = Duration::from_millis(75);
        let response = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 64\r\nX-GitHub-Request-Id: fixture-body-timeout\r\n\r\n{\"id\":".to_vec();
        let (origin, server) =
            serve_transport_case(Some(response), Duration::ZERO, Duration::from_millis(250));
        let client = GitHubRestClient::new_for_test_with_timeout(origin, timeout).unwrap();
        let token = token(InstallationOperation::PullRequestCreate);
        let error = client
            .send_json::<_, serde_json::Value>(
                RestEndpoint::CreatePullRequest {
                    owner: "owner".into(),
                    repository: "repo".into(),
                },
                GitHubAuthentication::Installation(&token),
                &serde_json::json!({"title":"fixture"}),
            )
            .unwrap_err();

        assert!(
            matches!(
                &error,
                GitHubApiError::Transport {
                    method: RestMethod::Post,
                    stage: TransportStage::ResponseBody,
                    kind: TransportKind::Timeout,
                    timeout_millis: 75,
                    status: Some(201),
                    request_id: Some(request_id),
                    ambiguous: true,
                    ..
                } if request_id == "fixture-body-timeout"
            ),
            "unexpected body timeout diagnostic: {error:?} / {error}"
        );
        server.join().unwrap();
    }

    #[test]
    #[ignore = "stateless live GitHub transport probe; run only with explicit network authorization"]
    fn live_exact_pr_post_transport_probe_reaches_an_http_response() {
        let client = GitHubRestClient::new().unwrap();
        let token = token(InstallationOperation::PullRequestCreate);
        let error = client
            .send_json::<_, serde_json::Value>(
                RestEndpoint::CreatePullRequest {
                    owner: "octocat".into(),
                    repository: "Hello-World".into(),
                },
                GitHubAuthentication::Installation(&token),
                &serde_json::json!({
                    "title":"hcom stateless transport probe",
                    "head":"hcom-stateless-transport-probe",
                    "base":"master",
                    "body":"x".repeat(7 * 1024),
                    "draft":false
                }),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            GitHubApiError::Http {
                method: RestMethod::Post,
                status: 401,
                ..
            }
        ));
    }
}
