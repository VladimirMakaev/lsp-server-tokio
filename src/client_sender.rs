//! Cloneable client communication handle with response routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{Message, Notification, ProtocolError, Request, RequestId, Response};

/// Shared map for routing responses to waiting callers.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResponseMap {
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<Response>>>>,
}

impl ResponseMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn lock_pending(
        &self,
        operation: &str,
    ) -> std::sync::MutexGuard<'_, HashMap<RequestId, oneshot::Sender<Response>>> {
        match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => panic!(
                "response map lock poisoned during {operation}; expected healthy routing state, received poisoned mutex: {poisoned}"
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn register(&self, id: RequestId) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        self.lock_pending("register").insert(id, tx);
        rx
    }

    pub(crate) fn try_register(&self, id: RequestId) -> Option<oneshot::Receiver<Response>> {
        let (tx, rx) = oneshot::channel();
        let mut pending = self.lock_pending("register");
        if pending.contains_key(&id) {
            return None;
        }

        pending.insert(id, tx);
        Some(rx)
    }

    pub(crate) fn deliver(&self, id: &RequestId, response: Response) -> bool {
        if let Some(tx) = self.lock_pending("deliver").remove(id) {
            let _ = tx.send(response);
            true
        } else {
            false
        }
    }

    pub(crate) fn cancel(&self, id: &RequestId) -> bool {
        self.lock_pending("cancel").remove(id).is_some()
    }

    pub(crate) fn cancel_all(&self) {
        self.lock_pending("cancel_all").clear();
    }
}

#[derive(Debug)]
struct PendingResponseGuard {
    response_map: ResponseMap,
    id: Option<RequestId>,
}

impl PendingResponseGuard {
    fn new(response_map: ResponseMap, id: RequestId) -> Self {
        Self {
            response_map,
            id: Some(id),
        }
    }

    fn disarm(&mut self) {
        self.id = None;
    }
}

impl Drop for PendingResponseGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.response_map.cancel(&id);
        }
    }
}

/// Error returned when the connection is closed and messages can no longer be queued.
#[derive(Debug, Clone, Error)]
#[error("connection closed")]
pub struct SendError;

/// A cloneable, non-blocking handle for server→client communication.
#[derive(Debug, Clone)]
pub struct ClientSender {
    tx: mpsc::UnboundedSender<Message>,
    response_map: ResponseMap,
    id_counter: Arc<AtomicI64>,
}

impl ClientSender {
    pub(crate) fn new(tx: mpsc::UnboundedSender<Message>, response_map: ResponseMap) -> Self {
        Self {
            tx,
            response_map,
            id_counter: Arc::new(AtomicI64::new(1)),
        }
    }

    /// Queues a notification for delivery to the client.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] if the connection has been closed.
    pub fn notify(&self, method: &str, params: Option<serde_json::Value>) -> Result<(), SendError> {
        let notification = Notification::new(method, params);
        self.tx
            .send(Message::Notification(notification))
            .map_err(|_| SendError)
    }

    /// Queues a response for delivery to the client.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] if the connection has been closed.
    pub fn respond(&self, response: Response) -> Result<(), SendError> {
        self.tx
            .send(Message::Response(response))
            .map_err(|_| SendError)
    }

    /// Sends a request with an auto-generated ID and waits for the response.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Disconnected`] if the connection is closed before
    /// a response arrives.
    pub async fn request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<Response, ProtocolError> {
        let (id, rx) = self.reserve_request_slot();
        self.send_registered_request(id, method, params, rx).await
    }

    /// Sends a request and returns a timeout error if no response arrives in time.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::RequestTimeout`] if the timeout elapses, or
    /// [`ProtocolError::Disconnected`] if the connection is closed.
    pub async fn request_timeout(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
        timeout: Duration,
    ) -> Result<Response, ProtocolError> {
        tokio::time::timeout(timeout, self.request(method, params))
            .await
            .map_err(|_| ProtocolError::RequestTimeout)?
    }

    fn next_id(&self) -> RequestId {
        let id = match self
            .id_counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(if current >= i64::from(i32::MAX) {
                    1
                } else {
                    current + 1
                })
            }) {
            Ok(id) => id,
            Err(current) => panic!(
                "request id counter update failed; expected atomic update to succeed, received current value {current}"
            ),
        };

        match i32::try_from(id) {
            Ok(id) => RequestId::Integer(id),
            Err(out_of_range) => panic!(
                "generated request id out of range; expected i32-compatible counter, received {out_of_range}"
            ),
        }
    }

    fn reserve_request_slot(&self) -> (RequestId, oneshot::Receiver<Response>) {
        loop {
            let id = self.next_id();
            if let Some(rx) = self.response_map.try_register(id.clone()) {
                return (id, rx);
            }
        }
    }

    #[cfg(test)]
    async fn send_request_with_id(
        &self,
        id: RequestId,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<Response, ProtocolError> {
        let rx = self.response_map.register(id.clone());
        self.send_registered_request(id, method, params, rx).await
    }

    async fn send_registered_request(
        &self,
        id: RequestId,
        method: &str,
        params: Option<serde_json::Value>,
        rx: oneshot::Receiver<Response>,
    ) -> Result<Response, ProtocolError> {
        let mut cleanup = PendingResponseGuard::new(self.response_map.clone(), id.clone());
        let request = Request::new(id.clone(), method, params);

        if self.tx.send(Message::Request(request)).is_err() {
            return Err(ProtocolError::Disconnected);
        }

        match rx.await {
            Ok(response) => {
                cleanup.disarm();
                Ok(response)
            }
            Err(_) => Err(ProtocolError::Disconnected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::future::join_all;
    use futures::StreamExt;
    use serde_json::json;

    use crate::{Connection, IncomingMessage};

    fn assert_clone_send_sync<T: Clone + Send + Sync>() {}

    #[test]
    fn client_sender_is_clone_send_sync() {
        assert_clone_send_sync::<ClientSender>();
    }

    #[tokio::test]
    async fn client_sender_notify_sends_notification() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        sender
            .notify("window/logMessage", Some(json!({"type": 4})))
            .unwrap();

        match client.receiver.next().await.unwrap().unwrap() {
            Message::Notification(notification) => {
                assert_eq!(notification.method, "window/logMessage");
                assert_eq!(notification.params, Some(json!({"type": 4})));
            }
            other => panic!("expected notification, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_sender_respond_sends_response() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        sender.respond(Response::ok(7, json!("ok"))).unwrap();

        match client.receiver.next().await.unwrap().unwrap() {
            Message::Response(response) => {
                assert_eq!(response.id, Some(7.into()));
                assert_eq!(response.result, Some(json!("ok")));
            }
            other => panic!("expected response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_sender_request_auto_id() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let first_sender = sender.clone();
        let first_task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move {
                first_sender
                    .request("first", None::<serde_json::Value>)
                    .await
            });

        let first_id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => {
                assert_eq!(request.method, "first");
                request.id
            }
            other => panic!("expected request, got {other:?}"),
        };
        assert_eq!(first_id, RequestId::Integer(1));

        let second_sender = sender.clone();
        let second_task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move {
                second_sender
                    .request("second", None::<serde_json::Value>)
                    .await
            });

        let second_id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => {
                assert_eq!(request.method, "second");
                request.id
            }
            other => panic!("expected request, got {other:?}"),
        };
        assert_eq!(second_id, RequestId::Integer(2));

        assert!(matches!(
            server.route(Message::Response(Response::ok(
                first_id.clone(),
                json!("first")
            ))),
            IncomingMessage::ResponseRouted
        ));
        assert!(matches!(
            server.route(Message::Response(Response::ok(
                second_id.clone(),
                json!("second")
            ))),
            IncomingMessage::ResponseRouted
        ));

        assert_eq!(first_task.await.unwrap().unwrap().id, Some(first_id));
        assert_eq!(second_task.await.unwrap().unwrap().id, Some(second_id));
    }

    #[tokio::test]
    async fn client_sender_request_gets_response() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move {
                sender
                    .request("workspace/configuration", Some(json!({"items": []})))
                    .await
            });

        let id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => {
                assert_eq!(request.method, "workspace/configuration");
                request.id
            }
            other => panic!("expected request, got {other:?}"),
        };

        let routed = server.route(Message::Response(Response::ok(
            id.clone(),
            json!({"settings": []}),
        )));
        assert!(matches!(routed, IncomingMessage::ResponseRouted));

        let response = task.await.unwrap().unwrap();
        assert_eq!(response.id, Some(id));
        assert_eq!(response.result, Some(json!({"settings": []})));
    }

    #[tokio::test]
    async fn client_sender_concurrent_requests() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let methods = ["first", "second", "third"];
        let tasks = methods.into_iter().map(|method| {
            let sender = sender.clone();
            let method = method.to_string();
            tokio::spawn(async move {
                let response = sender
                    .request(&method, None::<serde_json::Value>)
                    .await
                    .unwrap();
                (method, response)
            })
        });
        let tasks = join_all(tasks);

        let mut requests = Vec::new();
        for _ in 0..3 {
            match client.receiver.next().await.unwrap().unwrap() {
                Message::Request(request) => requests.push(request),
                other => panic!("expected request, got {other:?}"),
            }
        }

        for request in [
            requests[2].clone(),
            requests[0].clone(),
            requests[1].clone(),
        ] {
            assert!(matches!(
                server.route(Message::Response(Response::ok(
                    request.id.clone(),
                    json!(request.method.clone()),
                ))),
                IncomingMessage::ResponseRouted
            ));
        }

        let results = tasks.await;
        for result in results {
            let (method, response) = result.unwrap();
            assert_eq!(response.result, Some(json!(method)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn client_sender_request_timeout() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let _client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move {
                sender
                    .request_timeout(
                        "workspace/configuration",
                        None::<serde_json::Value>,
                        Duration::from_secs(5),
                    )
                    .await
            });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;

        let result = task.await.unwrap();
        assert!(matches!(result, Err(ProtocolError::RequestTimeout)));
    }

    #[tokio::test]
    async fn client_sender_request_disconnected() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        drop(client);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            sender.request("test", None::<serde_json::Value>),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ProtocolError::Disconnected)));
    }

    #[tokio::test]
    async fn client_sender_notify_after_close() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        drop(client);
        let _ = sender.notify("window/logMessage", None);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sender.notify("window/logMessage", None).is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn client_sender_multiple_clones_share_state() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();
        let sender_clone = sender.clone();

        let first_task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move { sender.request("first", None::<serde_json::Value>).await });
        let second_task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move {
                sender_clone
                    .request("second", None::<serde_json::Value>)
                    .await
            });

        let first_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };
        let second_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };

        assert_eq!(first_request.id, RequestId::Integer(1));
        assert_eq!(second_request.id, RequestId::Integer(2));

        assert!(matches!(
            server.route(Message::Response(Response::ok(
                first_request.id.clone(),
                json!(first_request.method.clone()),
            ))),
            IncomingMessage::ResponseRouted
        ));
        assert!(matches!(
            server.route(Message::Response(Response::ok(
                second_request.id.clone(),
                json!(second_request.method.clone()),
            ))),
            IncomingMessage::ResponseRouted
        ));

        assert_eq!(
            first_task.await.unwrap().unwrap().result,
            Some(json!("first"))
        );
        assert_eq!(
            second_task.await.unwrap().unwrap().result,
            Some(json!("second"))
        );
    }

    #[tokio::test]
    async fn route_delivers_to_response_map() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move { sender.request("test", None::<serde_json::Value>).await });

        let request_id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request.id,
            other => panic!("expected request, got {other:?}"),
        };

        let routed = server.route(Message::Response(Response::ok(
            request_id.clone(),
            json!(true),
        )));
        assert!(matches!(routed, IncomingMessage::ResponseRouted));

        let response = task.await.unwrap().unwrap();
        assert_eq!(response.id, Some(request_id));
    }

    #[tokio::test]
    async fn route_response_map_takes_priority() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let task: tokio::task::JoinHandle<Result<Response, ProtocolError>> =
            tokio::spawn(async move { sender.request("test", None::<serde_json::Value>).await });

        let request_id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request.id,
            other => panic!("expected request, got {other:?}"),
        };
        let _outgoing_rx = server.request_queue.outgoing.register(request_id.clone());

        let routed = server.route(Message::Response(Response::ok(
            request_id.clone(),
            json!("response-map"),
        )));
        assert!(matches!(routed, IncomingMessage::ResponseRouted));
        assert!(server.request_queue.outgoing.is_pending(&request_id));

        let response = task.await.unwrap().unwrap();
        assert_eq!(response.result, Some(json!("response-map")));
    }

    #[tokio::test]
    async fn client_sender_request_timeout_does_not_swallow_late_response() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let task = tokio::spawn(async move {
            sender
                .request_timeout(
                    "late-response",
                    None::<serde_json::Value>,
                    Duration::from_millis(10),
                )
                .await
        });

        let request_id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request.id,
            other => panic!("expected request, got {other:?}"),
        };

        let result = task.await.unwrap();
        assert!(matches!(result, Err(ProtocolError::RequestTimeout)));

        let routed = server.route(Message::Response(Response::ok(
            request_id.clone(),
            json!(true),
        )));
        match routed {
            IncomingMessage::ResponseUnknown(response) => {
                assert_eq!(response.id, Some(request_id));
            }
            other => panic!("late response should not be swallowed after timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_sender_aborted_request_does_not_swallow_late_response() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let task =
            tokio::spawn(async move { sender.request("aborted", None::<serde_json::Value>).await });

        let request_id = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request.id,
            other => panic!("expected request, got {other:?}"),
        };

        task.abort();
        let _ = task.await;

        let routed = server.route(Message::Response(Response::ok(
            request_id.clone(),
            json!(true),
        )));
        match routed {
            IncomingMessage::ResponseUnknown(response) => {
                assert_eq!(response.id, Some(request_id));
            }
            other => panic!("late response should not be swallowed after abort, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_sender_wraparound_does_not_reuse_pending_id() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let first_sender = sender.clone();
        let first_task = tokio::spawn(async move {
            first_sender
                .send_request_with_id(RequestId::Integer(1), "first", None::<serde_json::Value>)
                .await
        });

        let first_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };
        assert_eq!(first_request.id, RequestId::Integer(1));

        sender
            .id_counter
            .store(i64::from(i32::MAX), Ordering::Relaxed);

        let max_sender = sender.clone();
        let max_task =
            tokio::spawn(async move { max_sender.request("max", None::<serde_json::Value>).await });
        let wrapped_sender = sender.clone();
        let wrapped_task = tokio::spawn(async move {
            wrapped_sender
                .request("wrapped", None::<serde_json::Value>)
                .await
        });

        let second_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };
        let third_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };

        assert_ne!(second_request.id, first_request.id);
        assert_ne!(
            third_request.id, first_request.id,
            "wrapped request reused an active pending id"
        );
        assert_ne!(second_request.id, third_request.id);

        assert!(matches!(
            server.route(Message::Response(Response::ok(
                first_request.id.clone(),
                json!("first")
            ))),
            IncomingMessage::ResponseRouted
        ));
        assert!(matches!(
            server.route(Message::Response(Response::ok(
                second_request.id.clone(),
                json!("second")
            ))),
            IncomingMessage::ResponseRouted
        ));
        assert!(matches!(
            server.route(Message::Response(Response::ok(
                third_request.id.clone(),
                json!("third")
            ))),
            IncomingMessage::ResponseRouted
        ));

        assert_eq!(
            first_task.await.unwrap().unwrap().result,
            Some(json!("first"))
        );
        assert_eq!(
            max_task.await.unwrap().unwrap().result,
            Some(json!("second"))
        );
        assert_eq!(
            wrapped_task.await.unwrap().unwrap().result,
            Some(json!("third"))
        );
    }

    #[tokio::test]
    async fn client_sender_timeout_preserves_other_pending_requests() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut server: Connection<_, (), Response> = Connection::new(server_stream);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let sender = server.client_sender();

        let timeout_sender = sender.clone();
        let timeout_task = tokio::spawn(async move {
            timeout_sender
                .request_timeout(
                    "timeout",
                    None::<serde_json::Value>,
                    Duration::from_millis(10),
                )
                .await
        });
        let ok_sender = sender.clone();
        let ok_task =
            tokio::spawn(async move { ok_sender.request("ok", None::<serde_json::Value>).await });

        let first_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };
        let second_request = match client.receiver.next().await.unwrap().unwrap() {
            Message::Request(request) => request,
            other => panic!("expected request, got {other:?}"),
        };

        let (timed_out, live_request) = if first_request.method == "timeout" {
            (first_request, second_request)
        } else {
            (second_request, first_request)
        };

        let result = timeout_task.await.unwrap();
        assert!(matches!(result, Err(ProtocolError::RequestTimeout)));

        assert!(matches!(
            server.route(Message::Response(Response::ok(
                live_request.id.clone(),
                json!("ok")
            ))),
            IncomingMessage::ResponseRouted
        ));
        assert_eq!(ok_task.await.unwrap().unwrap().result, Some(json!("ok")));

        let routed = server.route(Message::Response(Response::ok(
            timed_out.id.clone(),
            json!("late"),
        )));
        assert!(matches!(routed, IncomingMessage::ResponseUnknown(_)));
    }

    #[tokio::test]
    async fn response_map_cancel_ignores_unknown_id() {
        let response_map = ResponseMap::new();

        assert!(!response_map.cancel(&RequestId::Integer(404)));
        assert!(!response_map.deliver(&RequestId::Integer(404), Response::ok(404, json!(null))));
    }
}
