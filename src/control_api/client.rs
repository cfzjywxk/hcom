use super::codec::{read_response_frame, write_request_frame};
use super::protocol::{ControlRequest, ControlResponse};
use anyhow::{Context, Result, bail};
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ControlClient {
    socket_path: PathBuf,
}

pub(crate) struct PendingControlResponse {
    stream: UnixStream,
    request_id: String,
}

impl ControlClient {
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
        }
    }

    pub fn request(&self, request: &ControlRequest) -> Result<ControlResponse> {
        self.begin_request(request, Some(SOCKET_IO_TIMEOUT))?.wait()
    }

    pub(crate) fn begin_wait(&self, request: &ControlRequest) -> Result<PendingControlResponse> {
        self.begin_request(request, None)
    }

    fn begin_request(
        &self,
        request: &ControlRequest,
        read_timeout: Option<Duration>,
    ) -> Result<PendingControlResponse> {
        request
            .validate()
            .context("refusing to send invalid control request")?;
        self.validate_socket_path()?;
        let payload = serde_json::to_vec(request).context("failed to encode control request")?;
        let mut stream = UnixStream::connect(&self.socket_path).with_context(|| {
            format!(
                "failed to connect to control socket {}",
                self.socket_path.display()
            )
        })?;
        stream.set_read_timeout(read_timeout)?;
        stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT))?;
        write_request_frame(&mut stream, &payload)?;
        Ok(PendingControlResponse {
            stream,
            request_id: request.request_id.clone(),
        })
    }

    fn validate_socket_path(&self) -> Result<()> {
        if !self.socket_path.is_absolute() {
            bail!("control socket path must be absolute");
        }
        let metadata = fs::symlink_metadata(&self.socket_path).with_context(|| {
            format!(
                "failed to inspect control socket {}",
                self.socket_path.display()
            )
        })?;
        // SAFETY: geteuid has no preconditions.
        let expected_uid = unsafe { libc::geteuid() };
        if !metadata.file_type().is_socket()
            || metadata.uid() != expected_uid
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            bail!("control socket is not a private socket owned by the current uid");
        }
        let parent = self
            .socket_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("control socket has no parent directory"))?;
        let parent_metadata = fs::symlink_metadata(parent)?;
        if parent_metadata.file_type().is_symlink()
            || !parent_metadata.is_dir()
            || parent_metadata.uid() != expected_uid
            || parent_metadata.permissions().mode() & 0o777 != 0o700
        {
            bail!("control socket parent is not private");
        }
        Ok(())
    }
}

impl PendingControlResponse {
    pub(crate) fn cancellation_stream(&self) -> Result<UnixStream> {
        self.stream
            .try_clone()
            .context("failed to clone pending control stream")
    }

    pub(crate) fn wait(mut self) -> Result<ControlResponse> {
        let response_frame = read_response_frame(&mut self.stream)?;
        let response: ControlResponse = serde_json::from_slice(&response_frame)
            .context("session supervisor returned malformed control JSON")?;
        response
            .validate()
            .context("session supervisor returned an invalid control response")?;
        if response.request_id != self.request_id {
            bail!("session supervisor returned an invalid control response envelope");
        }
        Ok(response)
    }
}
