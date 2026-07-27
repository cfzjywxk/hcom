//! Narrow library surface for the same-terminal foreground supervisor.
//!
//! Phase 2 intentionally exports only the tool-independent supervisor core.
//! The hcom database adapter remains an internal binary module and no public
//! command starts a handoff chain before the real Codex adapter exists.

#[cfg(unix)]
pub mod chain_pty;
#[cfg(unix)]
pub mod chain_supervisor;
