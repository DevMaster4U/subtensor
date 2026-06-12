//! Persistent WS / HTTP JSON-RPC clients for fast announce forwarding.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::announcing_peers::ForwardedAnnouncePayload;
use crate::peers::is_rpc_announcing_endpoint;

const RPC_METHOD: &str = "node_receiveForwardedAnnounce";

pub struct AnnouncingRpcPool {
    inner: RwLock<PoolInner>,
    http: reqwest::Client,
}

struct PoolInner {
    endpoints: HashMap<String, EndpointWorker>,
}

struct EndpointWorker {
    tx: mpsc::UnboundedSender<String>,
    _task: tokio::task::JoinHandle<()>,
}

impl AnnouncingRpcPool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(PoolInner {
                endpoints: HashMap::new(),
            }),
            http: reqwest::Client::new(),
        })
    }

    pub fn connect(&self, url: &str) -> Result<(), String> {
        let url = url.trim();
        if !is_rpc_announcing_endpoint(url) {
            return Err(format!("invalid rpc endpoint: {url}"));
        }

        let mut inner = self.inner.write().expect("poisoned");
        if inner.endpoints.contains_key(url) {
            return Ok(());
        }

        if url.starts_with("http://") || url.starts_with("https://") {
            inner.endpoints.insert(
                url.to_string(),
                EndpointWorker {
                    tx: mpsc::unbounded_channel().0,
                    _task: tokio::spawn(async {}),
                },
            );
            log::info!(target: "bot::announce", "announcing rpc endpoint registered (http): {url}");
            return Ok(());
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let endpoint = url.to_string();
        let task = tokio::spawn(ws_writer_loop(endpoint.clone(), rx));
        inner.endpoints.insert(
            url.to_string(),
            EndpointWorker { tx, _task: task },
        );
        log::info!(target: "bot::announce", "announcing rpc endpoint registered (ws): {url}");
        Ok(())
    }

    pub fn disconnect(&self, url: &str) {
        self.inner
            .write()
            .expect("poisoned")
            .endpoints
            .remove(url.trim());
    }

    pub fn clear(&self) {
        self.inner.write().expect("poisoned").endpoints.clear();
    }

    pub fn broadcast(&self, payload: &ForwardedAnnouncePayload) {
        let body = match serde_json::to_string(payload) {
            Ok(value) => value,
            Err(e) => {
                log::warn!(target: "bot::announce", "announce rpc encode failed: {e}");
                return;
            }
        };
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": RPC_METHOD,
            "params": [serde_json::from_str::<serde_json::Value>(&body).unwrap_or_default()],
            "id": 1
        });
        let frame = request.to_string();

        let endpoints: Vec<(String, Option<mpsc::UnboundedSender<String>>)> = self
            .inner
            .read()
            .expect("poisoned")
            .endpoints
            .iter()
            .map(|(url, worker)| (url.clone(), Some(worker.tx.clone())))
            .collect();

        for (url, ws_tx) in endpoints {
            if url.starts_with("http://") || url.starts_with("https://") {
                let http = self.http.clone();
                let url = url.clone();
                let frame = frame.clone();
                tokio::spawn(async move {
                    let endpoint = url.clone();
                    if let Err(e) = http
                        .post(endpoint)
                        .header("content-type", "application/json")
                        .body(frame)
                        .send()
                        .await
                    {
                        log::warn!(target: "bot::announce", "announce rpc http send to {url} failed: {e}");
                    }
                });
            } else if let Some(tx) = ws_tx {
                if tx.send(frame.clone()).is_err() {
                    log::warn!(target: "bot::announce", "announce rpc ws channel closed for {url}");
                }
            }
        }
    }
}

async fn ws_writer_loop(url: String, mut rx: mpsc::UnboundedReceiver<String>) {
    loop {
        match connect_async(&url).await {
            Ok((ws, _)) => {
                log::info!(target: "bot::announce", "announcing rpc ws connected: {url}");
                let (mut write, mut read) = ws.split();
                loop {
                    tokio::select! {
                        msg = rx.recv() => {
                            match msg {
                                Some(frame) => {
                                    if write.send(Message::Text(frame.into())).await.is_err() {
                                        break;
                                    }
                                }
                                None => return,
                            }
                        }
                        incoming = read.next() => {
                            match incoming {
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    target: "bot::announce",
                    "announcing rpc ws connect to {url} failed: {e}; retrying in 2s",
                );
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}
