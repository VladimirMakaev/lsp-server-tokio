//! Lifecycle state management for LSP connections.
//!
//! This module provides the [`LifecycleState`] enum for tracking connection state through
//! all LSP phases, [`ProtocolError`] for lifecycle violations, and [`ExitCode`] for
//! distinguishing clean from dirty exits.
//!
//! # LSP Lifecycle
//!
//! The LSP protocol defines a strict lifecycle:
//!
//! 1. **Uninitialized** - Connection established, only `initialize` request allowed
//! 2. **Initializing** - `initialize` received, only `initialized` notification allowed
//! 3. **Running** - Normal operation, all messages except `initialize` allowed
//! 4. **`ShuttingDown`** - `shutdown` received, only `exit` notification allowed
//! 5. **Exited** - `exit` received, connection should close
//!
//! Messages received in invalid states should be rejected (requests) or dropped (notifications).
//!
//! # Example
//!
//! ```
//! use lsp_server_tokio::LifecycleState;
//!
//! let state = LifecycleState::Uninitialized;
//!
//! // Only initialize allowed in Uninitialized state
//! assert!(state.is_request_allowed("initialize"));
//! assert!(!state.is_request_allowed("shutdown"));
//!
//! // Exit always allowed as notification
//! assert!(state.is_notification_allowed("exit"));
//! ```

use thiserror::Error;

/// Represents the lifecycle state of an LSP connection.
///
/// The LSP specification mandates a strict state machine where messages are only
/// valid in certain states. This enum tracks the current state and provides
/// validation methods.
///
/// # State Transitions
///
/// ```text
/// Uninitialized ──(initialize req)──> Initializing
///       │                                  │
///       │ (exit notif)                     │ (initialized notif)
///       v                                  v
///    Exited                            Running
///                                          │
///                                          │ (shutdown req)
///                                          v
///                                    ShuttingDown
///                                          │
///                                          │ (exit notif)
///                                          v
///                                       Exited
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LifecycleState {
    /// Connection established, awaiting `initialize` request.
    ///
    /// Only the `initialize` request is allowed. All other requests should
    /// receive a `ServerNotInitialized` error response. The `exit` notification
    /// is always allowed (to handle early disconnection).
    #[default]
    Uninitialized,

    /// `initialize` request received, awaiting `initialized` notification.
    ///
    /// No requests are allowed in this state. Only the `initialized` notification
    /// can transition the connection to `Running`.
    Initializing,

    /// Normal operation - all messages allowed except `initialize`.
    ///
    /// The server is fully initialized and can process any request or notification
    /// except another `initialize` request (cannot re-initialize).
    Running,

    /// `shutdown` request received, awaiting `exit` notification.
    ///
    /// No requests are allowed. Only the `exit` notification is valid.
    /// Any other messages should be rejected.
    ShuttingDown,

    /// `exit` notification received, connection should close.
    ///
    /// No messages are allowed. The connection should be closed immediately.
    Exited,
}

impl LifecycleState {
    /// Returns `true` if the given request method is valid in this state.
    ///
    /// # Request Validity by State
    ///
    /// | State | Allowed Requests |
    /// |-------|-----------------|
    /// | Uninitialized | `initialize` only |
    /// | Initializing | None |
    /// | Running | All except `initialize` |
    /// | `ShuttingDown` | None |
    /// | Exited | None |
    ///
    /// # Examples
    ///
    /// ```
    /// use lsp_server_tokio::LifecycleState;
    ///
    /// let state = LifecycleState::Running;
    /// assert!(state.is_request_allowed("textDocument/hover"));
    /// assert!(state.is_request_allowed("shutdown"));
    /// assert!(!state.is_request_allowed("initialize")); // Can't re-initialize
    /// ```
    #[must_use]
    pub fn is_request_allowed(&self, method: &str) -> bool {
        match self {
            Self::Uninitialized => method == "initialize",
            Self::Initializing | Self::ShuttingDown | Self::Exited => false,
            Self::Running => method != "initialize",
        }
    }

    /// Returns `true` if the given notification method is valid in this state.
    ///
    /// # Notification Validity by State
    ///
    /// | State | Allowed Notifications |
    /// |-------|----------------------|
    /// | Uninitialized | `exit` only |
    /// | Initializing | `initialized` only |
    /// | Running | All |
    /// | `ShuttingDown` | `exit` only |
    /// | Exited | None |
    ///
    /// # Examples
    ///
    /// ```
    /// use lsp_server_tokio::LifecycleState;
    ///
    /// let state = LifecycleState::Initializing;
    /// assert!(state.is_notification_allowed("initialized"));
    /// assert!(!state.is_notification_allowed("textDocument/didOpen"));
    /// ```
    #[must_use]
    pub fn is_notification_allowed(&self, method: &str) -> bool {
        match self {
            Self::Uninitialized | Self::ShuttingDown => method == "exit",
            Self::Initializing => method == "initialized",
            Self::Running => true,
            Self::Exited => false,
        }
    }
}

/// Exit code for the LSP server process.
///
/// Per the LSP specification:
/// - Exit code 0 if proper `shutdown` -> `exit` sequence was followed
/// - Exit code 1 if `exit` was received without prior `shutdown`
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::ExitCode;
///
/// let code = ExitCode::Success;
/// assert_eq!(code as i32, 0);
///
/// let code = ExitCode::Error;
/// assert_eq!(code as i32, 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    /// Proper shutdown sequence was followed (`shutdown` then `exit`).
    Success = 0,
    /// Exit without prior shutdown request.
    Error = 1,
}

/// Errors that occur during LSP protocol lifecycle management.
///
/// These errors represent violations of the LSP protocol's initialization
/// and shutdown sequences.
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::ProtocolError;
///
/// // Creating protocol errors
/// let err = ProtocolError::ExpectedInitialize("textDocument/hover".to_string());
/// assert!(err.to_string().contains("textDocument/hover"));
///
/// let err = ProtocolError::Disconnected;
/// assert!(err.to_string().contains("disconnected"));
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Expected `initialize` request, but received a different message.
    ///
    /// This occurs when the server is in `Uninitialized` state and receives
    /// a request other than `initialize`.
    #[error("expected initialize request, got: {0}")]
    ExpectedInitialize(String),

    /// Expected `initialized` notification, but received a different message.
    ///
    /// This occurs when the server is in `Initializing` state and receives
    /// a notification other than `initialized`.
    #[error("expected initialized notification, got: {0}")]
    ExpectedInitialized(String),

    /// The connection was disconnected unexpectedly.
    ///
    /// This typically occurs when the client closes the connection without
    /// sending an `exit` notification.
    #[error("connection disconnected unexpectedly")]
    Disconnected,

    /// Received a request after the `shutdown` request was processed.
    ///
    /// After receiving `shutdown`, only the `exit` notification is valid.
    #[error("received request after shutdown: {0}")]
    AfterShutdown(String),

    /// Timed out waiting for the `initialized` notification.
    #[error("timed out waiting for initialized notification (60s)")]
    InitializeTimeout,

    /// Timed out waiting for a response to a server-initiated request.
    #[error("timed out waiting for response to server request")]
    RequestTimeout,

    /// An I/O error occurred during protocol communication.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // LifecycleState Tests
    // =========================================================================

    #[test]
    fn lifecycle_state_default_is_uninitialized() {
        assert_eq!(LifecycleState::default(), LifecycleState::Uninitialized);
    }

    #[test]
    fn lifecycle_state_is_copy() {
        let state = LifecycleState::Running;
        let copy = state;
        assert_eq!(state, copy);
    }

    // -------------------------------------------------------------------------
    // is_request_allowed tests
    // -------------------------------------------------------------------------

    #[test]
    fn uninitialized_allows_only_initialize_request() {
        let state = LifecycleState::Uninitialized;

        assert!(state.is_request_allowed("initialize"));
        assert!(!state.is_request_allowed("shutdown"));
        assert!(!state.is_request_allowed("textDocument/hover"));
        assert!(!state.is_request_allowed("workspace/symbol"));
    }

    #[test]
    fn initializing_allows_no_requests() {
        let state = LifecycleState::Initializing;

        assert!(!state.is_request_allowed("initialize"));
        assert!(!state.is_request_allowed("shutdown"));
        assert!(!state.is_request_allowed("textDocument/hover"));
    }

    #[test]
    fn running_allows_all_requests_except_initialize() {
        let state = LifecycleState::Running;

        assert!(!state.is_request_allowed("initialize"));
        assert!(state.is_request_allowed("shutdown"));
        assert!(state.is_request_allowed("textDocument/hover"));
        assert!(state.is_request_allowed("textDocument/completion"));
        assert!(state.is_request_allowed("workspace/symbol"));
    }

    #[test]
    fn shutting_down_allows_no_requests() {
        let state = LifecycleState::ShuttingDown;

        assert!(!state.is_request_allowed("initialize"));
        assert!(!state.is_request_allowed("shutdown"));
        assert!(!state.is_request_allowed("textDocument/hover"));
    }

    #[test]
    fn exited_allows_no_requests() {
        let state = LifecycleState::Exited;

        assert!(!state.is_request_allowed("initialize"));
        assert!(!state.is_request_allowed("shutdown"));
        assert!(!state.is_request_allowed("textDocument/hover"));
    }

    // -------------------------------------------------------------------------
    // is_notification_allowed tests
    // -------------------------------------------------------------------------

    #[test]
    fn uninitialized_allows_only_exit_notification() {
        let state = LifecycleState::Uninitialized;

        assert!(state.is_notification_allowed("exit"));
        assert!(!state.is_notification_allowed("initialized"));
        assert!(!state.is_notification_allowed("textDocument/didOpen"));
    }

    #[test]
    fn initializing_allows_only_initialized_notification() {
        let state = LifecycleState::Initializing;

        assert!(state.is_notification_allowed("initialized"));
        assert!(!state.is_notification_allowed("exit"));
        assert!(!state.is_notification_allowed("textDocument/didOpen"));
    }

    #[test]
    fn running_allows_all_notifications() {
        let state = LifecycleState::Running;

        assert!(state.is_notification_allowed("exit"));
        assert!(state.is_notification_allowed("initialized"));
        assert!(state.is_notification_allowed("textDocument/didOpen"));
        assert!(state.is_notification_allowed("textDocument/didChange"));
        assert!(state.is_notification_allowed("$/cancelRequest"));
    }

    #[test]
    fn shutting_down_allows_only_exit_notification() {
        let state = LifecycleState::ShuttingDown;

        assert!(state.is_notification_allowed("exit"));
        assert!(!state.is_notification_allowed("initialized"));
        assert!(!state.is_notification_allowed("textDocument/didOpen"));
    }

    #[test]
    fn exited_allows_no_notifications() {
        let state = LifecycleState::Exited;

        assert!(!state.is_notification_allowed("exit"));
        assert!(!state.is_notification_allowed("initialized"));
        assert!(!state.is_notification_allowed("textDocument/didOpen"));
    }

    // =========================================================================
    // ExitCode Tests
    // =========================================================================

    #[test]
    fn exit_code_success_is_zero() {
        assert_eq!(ExitCode::Success as i32, 0);
    }

    #[test]
    fn exit_code_error_is_one() {
        assert_eq!(ExitCode::Error as i32, 1);
    }

    #[test]
    fn exit_code_is_copy() {
        let code = ExitCode::Success;
        let copy = code;
        assert_eq!(code, copy);
    }

    // =========================================================================
    // ProtocolError Tests
    // =========================================================================

    #[test]
    fn protocol_error_expected_initialize_message() {
        let err = ProtocolError::ExpectedInitialize("textDocument/hover".to_string());
        let msg = err.to_string();
        assert!(msg.contains("expected initialize request"));
        assert!(msg.contains("textDocument/hover"));
    }

    #[test]
    fn protocol_error_expected_initialized_message() {
        let err = ProtocolError::ExpectedInitialized("textDocument/didOpen".to_string());
        let msg = err.to_string();
        assert!(msg.contains("expected initialized notification"));
        assert!(msg.contains("textDocument/didOpen"));
    }

    #[test]
    fn protocol_error_disconnected_message() {
        let err = ProtocolError::Disconnected;
        let msg = err.to_string();
        assert!(msg.contains("disconnected unexpectedly"));
    }

    #[test]
    fn protocol_error_after_shutdown_message() {
        let err = ProtocolError::AfterShutdown("textDocument/hover".to_string());
        let msg = err.to_string();
        assert!(msg.contains("after shutdown"));
        assert!(msg.contains("textDocument/hover"));
    }

    #[test]
    fn protocol_error_request_timeout_message() {
        let err = ProtocolError::RequestTimeout;
        let msg = err.to_string();
        assert!(msg.contains("timed out waiting for response to server request"));
    }

    #[test]
    fn protocol_error_initialize_timeout_message() {
        let err = ProtocolError::InitializeTimeout;
        let msg = err.to_string();
        assert!(msg.contains("timed out waiting for initialized notification"));
        assert!(msg.contains("60s"));
    }

    #[test]
    fn protocol_error_io_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "connection lost");
        let err: ProtocolError = io_err.into();
        let msg = err.to_string();
        assert!(msg.contains("I/O error"));
        assert!(msg.contains("connection lost"));
    }

    #[test]
    fn protocol_error_is_debug() {
        let err = ProtocolError::Disconnected;
        let debug = format!("{err:?}");
        assert!(debug.contains("Disconnected"));
    }
}
