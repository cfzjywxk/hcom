//! Bounded JSONL transport for the pinned Codex App Server protocol.
//!
//! The reader threads continuously drain both child pipes. Only correlated
//! responses, terminal turn notifications, and sanitized server-request
//! methods cross into the runtime. Raw command output and protocol transcripts
//! are never retained as artifacts or diagnostics.

use super::runtime::{
    MAX_RUNTIME_DIAGNOSTIC_BYTES, RuntimeError, RuntimeFailureClass, RuntimeTelemetry,
    SanitizedRuntimeFailure,
};
use serde_json::{Value, json};
use std::collections::{BTreeSet, VecDeque};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub(crate) const MAX_RPC_LINE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_TURN_PROTOCOL_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_STDERR_TAIL_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_UNKNOWN_NOTIFICATIONS: u32 = 4096;

const MAX_QUEUED_PROTOCOL_BYTES: usize = 64 * 1024 * 1024;
const MAX_QUEUED_EVENTS: usize = 64;
const MAX_OUTGOING_RPC_BYTES: usize = 1024 * 1024;
const MAX_CLIENT_REQUEST_IDS: usize = 256;
const READ_CHUNK_BYTES: usize = 32 * 1024;

pub(crate) const OPT_OUT_NOTIFICATION_METHODS: &[&str] = &[
    "item/started",
    "item/completed",
    "item/agentMessage/delta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta",
    "item/commandExecution/outputDelta",
    "turn/diff/updated",
    "turn/plan/updated",
    "thread/tokenUsage/updated",
];

pub(crate) enum RpcInbound {
    Response { id: u64, result: Value },
    ResponseError { id: u64 },
    TurnCompleted { params: Value },
    ServerRequest { method: String },
    Eof,
}

enum ParsedInbound {
    Response { id: u64, result: Value },
    ResponseError { id: u64 },
    TurnCompleted { params: Value },
    ServerRequest { method: String },
    IgnoredNotification,
    UnknownNotification,
}

struct QueuedInbound {
    event: RpcInbound,
    wire_bytes: usize,
}

struct SharedState {
    queue: VecDeque<QueuedInbound>,
    queued_bytes: usize,
    fatal: Option<SanitizedRuntimeFailure>,
    eof: bool,
    turn_window_active: bool,
    protocol_bytes: u64,
    stderr_bytes: u64,
    stderr_window_base: u64,
    stderr_tail: VecDeque<u8>,
    notification_count: u32,
    unknown_notification_count: u32,
}

impl SharedState {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            queued_bytes: 0,
            fatal: None,
            eof: false,
            turn_window_active: false,
            protocol_bytes: 0,
            stderr_bytes: 0,
            stderr_window_base: 0,
            stderr_tail: VecDeque::with_capacity(MAX_STDERR_TAIL_BYTES),
            notification_count: 0,
            unknown_notification_count: 0,
        }
    }

    fn set_fatal(
        &mut self,
        class: RuntimeFailureClass,
        detail: impl Into<String>,
        retryable: bool,
    ) {
        if self.fatal.is_none() {
            let failure =
                SanitizedRuntimeFailure::new(class, detail, retryable).unwrap_or_else(|_| {
                    SanitizedRuntimeFailure {
                        class: RuntimeFailureClass::Protocol,
                        detail: "Codex App Server transport violated its bounded contract".into(),
                        retryable: false,
                    }
                });
            self.queue.clear();
            self.queued_bytes = 0;
            self.fatal = Some(failure);
        }
    }

    fn account_protocol_bytes(&mut self, count: usize) {
        if !self.turn_window_active || self.fatal.is_some() {
            return;
        }
        self.protocol_bytes = self.protocol_bytes.saturating_add(count as u64);
        if self.protocol_bytes > MAX_TURN_PROTOCOL_BYTES {
            self.set_fatal(
                RuntimeFailureClass::Protocol,
                "Codex App Server turn protocol exceeded 64 MiB",
                false,
            );
        }
    }

    fn account_notification(&mut self, unknown: bool) {
        self.notification_count = self.notification_count.saturating_add(1);
        if unknown {
            self.unknown_notification_count = self.unknown_notification_count.saturating_add(1);
            if self.unknown_notification_count > MAX_UNKNOWN_NOTIFICATIONS {
                self.set_fatal(
                    RuntimeFailureClass::Protocol,
                    "Codex App Server emitted more than 4096 unknown notifications",
                    false,
                );
            }
        }
    }

    fn enqueue(&mut self, event: RpcInbound, wire_bytes: usize) {
        if self.fatal.is_some() {
            return;
        }
        let next_bytes = self.queued_bytes.saturating_add(wire_bytes);
        if self.queue.len() >= MAX_QUEUED_EVENTS || next_bytes > MAX_QUEUED_PROTOCOL_BYTES {
            self.set_fatal(
                RuntimeFailureClass::Protocol,
                "Codex App Server inbound queue exceeded its bound",
                false,
            );
            return;
        }
        self.queue.push_back(QueuedInbound { event, wire_bytes });
        self.queued_bytes = next_bytes;
    }

    fn pop(&mut self) -> Option<RpcInbound> {
        let queued = self.queue.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(queued.wire_bytes);
        Some(queued.event)
    }

    fn begin_turn_window(&mut self) -> Result<(), RuntimeError> {
        if self.fatal.is_some() || self.eof {
            return Err(RuntimeError::invalid_transition(
                "Codex App Server transport is terminal",
            ));
        }
        if !self.queue.is_empty() {
            return Err(RuntimeError::invalid_transition(
                "Codex App Server has an unexpected pending protocol event",
            ));
        }
        self.turn_window_active = true;
        self.protocol_bytes = 0;
        self.notification_count = 0;
        self.unknown_notification_count = 0;
        self.stderr_window_base = self.stderr_bytes;
        Ok(())
    }

    fn end_turn_window(&mut self) {
        self.turn_window_active = false;
    }

    fn telemetry(&self) -> RuntimeTelemetry {
        RuntimeTelemetry {
            protocol_bytes: self.protocol_bytes,
            stderr_bytes: self.stderr_bytes.saturating_sub(self.stderr_window_base),
            notification_count: self.notification_count,
        }
    }

    fn append_stderr(&mut self, bytes: &[u8]) {
        self.stderr_bytes = self.stderr_bytes.saturating_add(bytes.len() as u64);
        if bytes.len() >= MAX_STDERR_TAIL_BYTES {
            self.stderr_tail.clear();
            self.stderr_tail
                .extend(bytes[bytes.len() - MAX_STDERR_TAIL_BYTES..].iter().copied());
            return;
        }
        let overflow = self
            .stderr_tail
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(MAX_STDERR_TAIL_BYTES);
        if overflow > 0 {
            self.stderr_tail.drain(..overflow);
        }
        self.stderr_tail.extend(bytes.iter().copied());
    }
}

struct Shared {
    state: Mutex<SharedState>,
    wake: Condvar,
}

impl Shared {
    fn new() -> Self {
        Self {
            state: Mutex::new(SharedState::new()),
            wake: Condvar::new(),
        }
    }

    fn fail(&self, class: RuntimeFailureClass, detail: &'static str) {
        if let Ok(mut state) = self.state.lock() {
            state.set_fatal(class, detail, false);
            self.wake.notify_all();
        }
    }
}

pub(crate) struct CodexRpc {
    writer: Option<BufWriter<Box<dyn Write + Send>>>,
    shared: Arc<Shared>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
    next_request_id: u64,
    outstanding_request: Option<u64>,
    completed_request_ids: BTreeSet<u64>,
}

impl CodexRpc {
    pub(crate) fn new<W, O, E>(writer: W, stdout: O, stderr: E) -> Self
    where
        W: Write + Send + 'static,
        O: Read + Send + 'static,
        E: Read + Send + 'static,
    {
        let shared = Arc::new(Shared::new());
        let stdout_shared = shared.clone();
        let stderr_shared = shared.clone();
        Self {
            writer: Some(BufWriter::new(Box::new(writer))),
            shared,
            stdout_thread: Some(thread::spawn(move || {
                drain_stdout(stdout, &stdout_shared);
            })),
            stderr_thread: Some(thread::spawn(move || {
                drain_stderr(stderr, &stderr_shared);
            })),
            next_request_id: 1,
            outstanding_request: None,
            completed_request_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn send_request(
        &mut self,
        method: &'static str,
        params: Value,
    ) -> Result<u64, RuntimeError> {
        if self.outstanding_request.is_some() {
            return Err(RuntimeError::invalid_transition(
                "Codex App Server already has an outstanding client request",
            ));
        }
        if self.completed_request_ids.len() >= MAX_CLIENT_REQUEST_IDS {
            return Err(RuntimeError::invalid_transition(
                "Codex App Server request-id inventory exceeded 256 entries",
            ));
        }
        let id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| RuntimeError::internal("Codex App Server request id overflow"))?;
        self.write_message(&json!({"method": method, "id": id, "params": params}))?;
        self.outstanding_request = Some(id);
        Ok(id)
    }

    pub(crate) fn send_notification(
        &mut self,
        method: &'static str,
        params: Value,
    ) -> Result<(), RuntimeError> {
        self.write_message(&json!({"method": method, "params": params}))
    }

    fn write_message(&mut self, message: &Value) -> Result<(), RuntimeError> {
        let mut encoded = serde_json::to_vec(message)
            .map_err(|_| RuntimeError::internal("failed to encode Codex App Server request"))?;
        if encoded.len() >= MAX_OUTGOING_RPC_BYTES {
            return Err(RuntimeError::invalid_contract(
                "Codex App Server request exceeded 1 MiB",
            ));
        }
        encoded.push(b'\n');
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| RuntimeError::invalid_transition("Codex App Server stdin is closed"))?;
        writer
            .write_all(&encoded)
            .and_then(|()| writer.flush())
            .map_err(|_| RuntimeError::internal("failed to write Codex App Server request"))
    }

    pub(crate) fn begin_turn_window(&self) -> Result<(), RuntimeError> {
        self.shared
            .state
            .lock()
            .map_err(|_| RuntimeError::internal("Codex App Server transport lock was poisoned"))?
            .begin_turn_window()
    }

    pub(crate) fn end_turn_window(&self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.end_turn_window();
        }
    }

    pub(crate) fn telemetry(&self) -> RuntimeTelemetry {
        self.shared
            .state
            .lock()
            .map(|state| state.telemetry())
            .unwrap_or_default()
    }

    pub(crate) fn receive(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<RpcInbound>, SanitizedRuntimeFailure> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut state = self.shared.state.lock().map_err(|_| {
                sanitized_failure(
                    RuntimeFailureClass::Protocol,
                    "Codex App Server transport lock was poisoned",
                )
            })?;
            if let Some(failure) = state.fatal.clone() {
                return Err(failure);
            }
            if let Some(event) = state.pop() {
                drop(state);
                return self.correlate(event).map(Some);
            }
            if state.eof {
                return Ok(Some(RpcInbound::Eof));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let wait = deadline.saturating_duration_since(now);
            let (next, _) = self.shared.wake.wait_timeout(state, wait).map_err(|_| {
                sanitized_failure(
                    RuntimeFailureClass::Protocol,
                    "Codex App Server transport wait was poisoned",
                )
            })?;
            drop(next);
        }
    }

    pub(crate) fn try_receive(&mut self) -> Result<Option<RpcInbound>, SanitizedRuntimeFailure> {
        self.receive(Duration::ZERO)
    }

    fn correlate(&mut self, event: RpcInbound) -> Result<RpcInbound, SanitizedRuntimeFailure> {
        let id = match &event {
            RpcInbound::Response { id, .. } | RpcInbound::ResponseError { id } => Some(*id),
            _ => None,
        };
        let Some(id) = id else {
            return Ok(event);
        };
        let failure = if self.completed_request_ids.contains(&id) {
            Some("Codex App Server returned a duplicate response id")
        } else if self.outstanding_request != Some(id) {
            Some("Codex App Server returned a stale or mismatched response id")
        } else {
            None
        };
        if let Some(detail) = failure {
            if let Ok(mut state) = self.shared.state.lock() {
                state.set_fatal(RuntimeFailureClass::Protocol, detail, false);
                self.shared.wake.notify_all();
            }
            return Err(sanitized_failure(RuntimeFailureClass::Protocol, detail));
        }
        self.outstanding_request = None;
        self.completed_request_ids.insert(id);
        Ok(event)
    }

    pub(crate) fn close_writer(&mut self) {
        self.writer.take();
    }

    pub(crate) fn join_readers(&mut self, timeout: Duration) -> Result<(), RuntimeError> {
        self.close_writer();
        let deadline = Instant::now() + timeout;
        let mut failed = false;
        for (label, handle) in [
            ("stdout", self.stdout_thread.take()),
            ("stderr", self.stderr_thread.take()),
        ] {
            let Some(handle) = handle else {
                continue;
            };
            while !handle.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            if !handle.is_finished() {
                failed = true;
                continue;
            }
            if handle.join().is_err() {
                return Err(RuntimeError::internal(format!(
                    "Codex App Server {label} reader panicked"
                )));
            }
        }
        if failed {
            Err(RuntimeError::internal(
                "Codex App Server readers exceeded their shutdown deadline",
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn stderr_tail_len(&self) -> usize {
        self.shared
            .state
            .lock()
            .map(|state| state.stderr_tail.len())
            .unwrap_or_default()
    }
}

fn drain_stdout(stdout: impl Read, shared: &Shared) {
    let mut reader = BufReader::with_capacity(READ_CHUNK_BYTES, stdout);
    let mut line = Vec::with_capacity(READ_CHUNK_BYTES);
    let mut line_bytes = 0usize;
    let mut discard_line = false;
    loop {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(_) => {
                shared.fail(
                    RuntimeFailureClass::Process,
                    "failed to read Codex App Server stdout",
                );
                return;
            }
        };
        if available.is_empty() {
            if line_bytes > 0 && !discard_line {
                process_line(&line, line_bytes, shared);
            }
            if let Ok(mut state) = shared.state.lock() {
                state.eof = true;
                shared.wake.notify_all();
            }
            return;
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let payload = newline.map_or(consumed, |position| position);
        if let Ok(mut state) = shared.state.lock() {
            state.account_protocol_bytes(consumed);
        }
        line_bytes = line_bytes.saturating_add(payload);
        if !discard_line && line_bytes > MAX_RPC_LINE_BYTES {
            shared.fail(
                RuntimeFailureClass::Protocol,
                "Codex App Server emitted a JSON-RPC line larger than 16 MiB",
            );
            discard_line = true;
            line.clear();
        } else if !discard_line {
            line.extend_from_slice(&available[..payload]);
        }
        reader.consume(consumed);

        if newline.is_some() {
            if !discard_line {
                process_line(&line, line_bytes.saturating_add(1), shared);
            }
            line.clear();
            line_bytes = 0;
            discard_line = false;
        }
    }
}

fn process_line(line: &[u8], wire_bytes: usize, shared: &Shared) {
    if line.is_empty() {
        shared.fail(
            RuntimeFailureClass::Protocol,
            "Codex App Server emitted an empty JSON-RPC line",
        );
        return;
    }
    let value: Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(_) => {
            shared.fail(
                RuntimeFailureClass::Protocol,
                "Codex App Server emitted malformed JSON",
            );
            return;
        }
    };
    let parsed = match classify_message(value) {
        Ok(parsed) => parsed,
        Err(detail) => {
            shared.fail(RuntimeFailureClass::Protocol, detail);
            return;
        }
    };
    let Ok(mut state) = shared.state.lock() else {
        return;
    };
    match parsed {
        ParsedInbound::Response { id, result } => {
            state.enqueue(RpcInbound::Response { id, result }, wire_bytes);
        }
        ParsedInbound::ResponseError { id } => {
            state.enqueue(RpcInbound::ResponseError { id }, wire_bytes);
        }
        ParsedInbound::TurnCompleted { params } => {
            state.account_notification(false);
            state.enqueue(RpcInbound::TurnCompleted { params }, wire_bytes);
        }
        ParsedInbound::ServerRequest { method } => {
            state.enqueue(RpcInbound::ServerRequest { method }, wire_bytes);
        }
        ParsedInbound::IgnoredNotification => state.account_notification(false),
        ParsedInbound::UnknownNotification => state.account_notification(true),
    }
    shared.wake.notify_all();
}

fn classify_message(value: Value) -> Result<ParsedInbound, &'static str> {
    let Value::Object(mut object) = value else {
        return Err("Codex App Server JSON-RPC message was not an object");
    };
    let method = object.remove("method");
    let id = object.remove("id");
    match (method, id) {
        (Some(Value::String(method)), Some(_request_id)) => Ok(ParsedInbound::ServerRequest {
            method: sanitize_method(&method),
        }),
        (Some(Value::String(method)), None) => {
            let params = object.remove("params").unwrap_or(Value::Null);
            if method == "turn/completed" {
                Ok(ParsedInbound::TurnCompleted { params })
            } else if OPT_OUT_NOTIFICATION_METHODS.contains(&method.as_str()) {
                Ok(ParsedInbound::IgnoredNotification)
            } else {
                Ok(ParsedInbound::UnknownNotification)
            }
        }
        (Some(_), _) => Err("Codex App Server JSON-RPC method was not a string"),
        (None, Some(id)) => {
            let id = parse_response_id(id)?;
            let result = object.remove("result");
            let error = object.remove("error");
            match (result, error) {
                (Some(result), None) => Ok(ParsedInbound::Response { id, result }),
                (None, Some(_)) => Ok(ParsedInbound::ResponseError { id }),
                _ => Err("Codex App Server response must contain exactly one result or error"),
            }
        }
        (None, None) => Err("Codex App Server JSON-RPC envelope omitted method and id"),
    }
}

fn parse_response_id(value: Value) -> Result<u64, &'static str> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .ok_or("Codex App Server response id was not a non-negative integer"),
        _ => Err("Codex App Server response id did not match hcom numeric ids"),
    }
}

fn sanitize_method(method: &str) -> String {
    if !method.is_empty()
        && method.len() <= 128
        && method
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        method.to_owned()
    } else {
        "invalid-method".into()
    }
}

fn drain_stderr(mut stderr: impl Read, shared: &Shared) {
    let mut buffer = [0u8; READ_CHUNK_BYTES];
    loop {
        let count = match stderr.read(&mut buffer) {
            Ok(count) => count,
            Err(_) => {
                shared.fail(
                    RuntimeFailureClass::Process,
                    "failed to read Codex App Server stderr",
                );
                return;
            }
        };
        if count == 0 {
            return;
        }
        if let Ok(mut state) = shared.state.lock() {
            state.append_stderr(&buffer[..count]);
        }
    }
}

fn sanitized_failure(
    class: RuntimeFailureClass,
    detail: impl Into<String>,
) -> SanitizedRuntimeFailure {
    let detail = detail.into();
    debug_assert!(detail.len() <= MAX_RUNTIME_DIAGNOSTIC_BYTES);
    SanitizedRuntimeFailure::new(class, detail, false).unwrap_or(SanitizedRuntimeFailure {
        class,
        detail: "Codex App Server runtime failure".into(),
        retryable: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::mpsc;

    #[test]
    fn exact_line_and_turn_protocol_bounds_are_closed() {
        for (bytes, accepted) in [
            (MAX_TURN_PROTOCOL_BYTES - 1, true),
            (MAX_TURN_PROTOCOL_BYTES, true),
            (MAX_TURN_PROTOCOL_BYTES + 1, false),
        ] {
            let mut state = SharedState::new();
            state.turn_window_active = true;
            state.account_protocol_bytes(bytes as usize);
            assert_eq!(state.fatal.is_none(), accepted);
        }
        assert_eq!(MAX_RPC_LINE_BYTES, 16 * 1024 * 1024);
    }

    #[test]
    fn queue_outgoing_message_and_request_id_bounds_are_exact() {
        for (events, accepted) in [
            (MAX_QUEUED_EVENTS - 1, true),
            (MAX_QUEUED_EVENTS, true),
            (MAX_QUEUED_EVENTS + 1, false),
        ] {
            let mut state = SharedState::new();
            for _ in 0..events {
                state.enqueue(RpcInbound::Eof, 0);
            }
            assert_eq!(state.fatal.is_none(), accepted);
        }
        for (bytes, accepted) in [
            (MAX_QUEUED_PROTOCOL_BYTES - 1, true),
            (MAX_QUEUED_PROTOCOL_BYTES, true),
            (MAX_QUEUED_PROTOCOL_BYTES + 1, false),
        ] {
            let mut state = SharedState::new();
            state.enqueue(RpcInbound::Eof, bytes);
            assert_eq!(state.fatal.is_none(), accepted);
        }

        let empty = json!({"payload": ""});
        let overhead = serde_json::to_vec(&empty).unwrap().len();
        for (bytes, accepted) in [
            (MAX_OUTGOING_RPC_BYTES - 1, true),
            (MAX_OUTGOING_RPC_BYTES, false),
            (MAX_OUTGOING_RPC_BYTES + 1, false),
        ] {
            let message = json!({"payload": "x".repeat(bytes - overhead)});
            assert_eq!(serde_json::to_vec(&message).unwrap().len(), bytes);
            let (writer, _written) = mpsc_writer();
            let mut rpc = CodexRpc::new(
                writer,
                Cursor::new(Vec::<u8>::new()),
                Cursor::new(Vec::new()),
            );
            assert_eq!(rpc.write_message(&message).is_ok(), accepted);
        }

        for (requests, accepted) in [
            (MAX_CLIENT_REQUEST_IDS - 1, true),
            (MAX_CLIENT_REQUEST_IDS, true),
            (MAX_CLIENT_REQUEST_IDS + 1, false),
        ] {
            let (writer, _written) = mpsc_writer();
            let mut rpc = CodexRpc::new(
                writer,
                Cursor::new(Vec::<u8>::new()),
                Cursor::new(Vec::new()),
            );
            let mut result = Ok(());
            for _ in 0..requests {
                let id = match rpc.send_request("bounded/request", json!({})) {
                    Ok(id) => id,
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                };
                rpc.correlate(RpcInbound::Response {
                    id,
                    result: json!({}),
                })
                .unwrap();
            }
            assert_eq!(result.is_ok(), accepted);
        }
    }

    #[test]
    fn unknown_notification_bound_is_exact_and_opted_out_events_do_not_consume_it() {
        for (notifications, accepted) in [
            (MAX_UNKNOWN_NOTIFICATIONS - 1, true),
            (MAX_UNKNOWN_NOTIFICATIONS, true),
            (MAX_UNKNOWN_NOTIFICATIONS + 1, false),
        ] {
            let mut state = SharedState::new();
            for _ in 0..notifications {
                state.account_notification(true);
            }
            assert_eq!(state.fatal.is_none(), accepted);
        }
        let mut state = SharedState::new();
        for _ in 0..MAX_UNKNOWN_NOTIFICATIONS {
            state.account_notification(true);
        }
        assert!(state.fatal.is_none());
        state.account_notification(false);
        assert!(state.fatal.is_none());
        state.account_notification(true);
        assert!(state.fatal.is_some());
    }

    #[test]
    fn response_correlation_rejects_mismatch_duplicate_and_stale_ids() {
        let (writer, _written) = mpsc_writer();
        let mut rpc = CodexRpc::new(
            writer,
            Cursor::new(Vec::<u8>::new()),
            Cursor::new(Vec::new()),
        );
        let id = rpc.send_request("initialize", json!({})).unwrap();
        assert_eq!(id, 1);
        assert!(
            rpc.send_request("thread/start", json!({}))
                .unwrap_err()
                .detail
                .contains("outstanding")
        );
        let mismatch = rpc.correlate(RpcInbound::Response {
            id: 2,
            result: json!({}),
        });
        assert!(mismatch.is_err());

        let (writer, _written) = mpsc_writer();
        let mut rpc = CodexRpc::new(
            writer,
            Cursor::new(Vec::<u8>::new()),
            Cursor::new(Vec::new()),
        );
        let id = rpc.send_request("initialize", json!({})).unwrap();
        rpc.correlate(RpcInbound::Response {
            id,
            result: json!({}),
        })
        .unwrap();
        assert!(
            rpc.correlate(RpcInbound::Response {
                id,
                result: json!({})
            })
            .is_err()
        );
    }

    #[test]
    fn stderr_is_continuously_drained_into_a_one_mib_tail() {
        for bytes in [
            MAX_STDERR_TAIL_BYTES - 1,
            MAX_STDERR_TAIL_BYTES,
            MAX_STDERR_TAIL_BYTES + 1,
        ] {
            let (writer, _written) = mpsc_writer();
            let stderr = Cursor::new(vec![b'x'; bytes]);
            let mut rpc = CodexRpc::new(writer, Cursor::new(Vec::<u8>::new()), stderr);
            rpc.join_readers(Duration::from_secs(1)).unwrap();
            assert_eq!(rpc.stderr_tail_len(), bytes.min(MAX_STDERR_TAIL_BYTES));
            assert_eq!(rpc.telemetry().stderr_bytes, bytes as u64);
        }
    }

    #[test]
    fn decoder_drops_opted_out_item_payload_and_sanitizes_server_requests() {
        assert!(matches!(
            classify_message(json!({
                "method": "item/completed",
                "params": {"raw": "secret-command-output"}
            }))
            .unwrap(),
            ParsedInbound::IgnoredNotification
        ));
        match classify_message(json!({
            "id": "server-id",
            "method": "item/tool/requestUserInput",
            "params": {"secret": "must-not-cross"}
        }))
        .unwrap()
        {
            ParsedInbound::ServerRequest { method } => {
                assert_eq!(method, "item/tool/requestUserInput");
            }
            _ => panic!("server request was not classified"),
        }
        match classify_message(json!({"id": 9, "method": "bad method\nsecret", "params": {}}))
            .unwrap()
        {
            ParsedInbound::ServerRequest { method } => assert_eq!(method, "invalid-method"),
            _ => panic!("server request was not classified"),
        }
    }

    struct ChannelWriter(mpsc::Sender<Vec<u8>>);

    impl Write for ChannelWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0
                .send(buffer.to_vec())
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn mpsc_writer() -> (ChannelWriter, mpsc::Receiver<Vec<u8>>) {
        let (sender, receiver) = mpsc::channel();
        (ChannelWriter(sender), receiver)
    }
}
