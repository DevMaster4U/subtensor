//! After a local IPC submit, forward the transaction to configured remote nodes.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::remote_nodes::{RemoteNodeEntry, RemoteNodesControl};
use crate::submit::PreparedSubmitRequest;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmitType {
    CRpc,
    Rpc,
}

impl SubmitType {
    fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "c_rpc" | "crpc" | "3" => Some(Self::CRpc),
            "rpc" | "2" => Some(Self::Rpc),
            _ => None,
        }
    }
}

/// Runtime toggle for forwarding IPC transactions to `remote_nodes`.
pub struct RemoteSubmitControl {
    enabled: AtomicBool,
    remote_nodes: Arc<RemoteNodesControl>,
    http: reqwest::Client,
    ws_pool: Arc<WsSubmitPool>,
}

impl RemoteSubmitControl {
    pub fn new(remote_nodes: Arc<RemoteNodesControl>) -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(true),
            remote_nodes,
            http: reqwest::Client::new(),
            ws_pool: WsSubmitPool::new(),
        })
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
        log::info!(target: "bot::submit", "remote IPC submit forwarding enabled");
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        self.shutdown_ws_connections();
        log::info!(target: "bot::submit", "remote IPC submit forwarding disabled");
    }

    /// Drop all persistent WS submit clients (stops reconnect retry loops).
    pub fn shutdown_ws_connections(&self) {
        self.ws_pool.clear();
    }

    /// Keep WS clients only for `c_rpc` URLs still listed on enabled remote nodes.
    pub fn sync_ws_connections(&self) {
        if !self.is_enabled() || self.remote_nodes.list().is_empty() {
            self.shutdown_ws_connections();
            return;
        }
        self.ws_pool.retain(&c_rpc_urls_from_nodes(&self.remote_nodes.list()));
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    pub fn forward_after_ipc(&self, request: PreparedSubmitRequest) {
        if !self.is_enabled() {
            return;
        }
        let nodes = self.remote_nodes.list();
        if nodes.is_empty() {
            return;
        }
        let control = Arc::new(RemoteSubmitRunner {
            http: self.http.clone(),
            ws_pool: self.ws_pool.clone(),
        });
        tokio::spawn(async move {
            control.forward_to_remotes(&nodes, &request).await;
        });
    }
}

struct RemoteSubmitRunner {
    http: reqwest::Client,
    ws_pool: Arc<WsSubmitPool>,
}

fn node_submit_types(node: &RemoteNodeEntry) -> Vec<SubmitType> {
    match &node.submit_types {
        Some(raw) => raw.iter().filter_map(|s| SubmitType::parse(s)).collect(),
        None => vec![SubmitType::CRpc],
    }
}

fn dedupe_urls(urls: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for url in urls {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !out.iter().any(|u| u == trimmed) {
            out.push(trimmed.to_string());
        }
    }
    out
}

fn wire_hex_from_request(request: &PreparedSubmitRequest) -> Option<String> {
    if let Some(inner) = request.extrinsic.as_ref().filter(|s| !s.is_empty()) {
        let inner_bytes = subtensor_ipc::decode_hex(inner).ok()?;
        if inner_bytes.is_empty() {
            return None;
        }
        let wire = subtensor_ipc::encode_opaque_wire(&inner_bytes);
        return Some(format!("0x{}", hex::encode(wire)));
    }
    let hash = request.hash.trim();
    if hash.is_empty() {
        None
    } else {
        Some(hash.to_string())
    }
}

impl RemoteSubmitRunner {
    async fn forward_to_remotes(&self, nodes: &[RemoteNodeEntry], request: &PreparedSubmitRequest) {
        let inner_hex = request.extrinsic.as_deref().filter(|s| !s.is_empty());
        let wire_hex = wire_hex_from_request(request);
        let hash_ref = wire_hex.as_deref();

        for node in nodes {
            for submit_type in node_submit_types(node) {
                match submit_type {
                    SubmitType::CRpc => {
                        let ws_urls = dedupe_urls(&node.c_rpc);
                        if ws_urls.is_empty() {
                            log::warn!(
                                target: "bot::submit",
                                "remote forward {} c_rpc: missing c_rpc URL(s)",
                                node.name,
                            );
                            continue;
                        }
                        let http_fallbacks = dedupe_urls(&node.rpc);
                        let Some(inner_hex) = inner_hex else {
                            log::warn!(
                                target: "bot::submit",
                                "remote forward {} c_rpc: missing inner extrinsic",
                                node.name,
                            );
                            continue;
                        };
                        for ws_url in ws_urls {
                            match self
                                .submit_prepared_ws(&ws_url, inner_hex, hash_ref)
                                .await
                            {
                                Ok(hash) => log::info!(
                                    target: "bot::submit",
                                    "remote forward c_rpc → {} {} ok hash={hash}",
                                    node.name,
                                    ws_url,
                                ),
                                Err(ws_err) => {
                                    let mut last_err = ws_err;
                                    let mut ok = false;
                                    for http_url in &http_fallbacks {
                                        match self
                                            .submit_prepared_http(http_url, inner_hex, hash_ref)
                                            .await
                                        {
                                            Ok(hash) => {
                                                log::info!(
                                                    target: "bot::submit",
                                                    "remote forward c_rpc → {} {ws_url} (fallback {http_url}) ok hash={hash}",
                                                    node.name,
                                                );
                                                ok = true;
                                                break;
                                            }
                                            Err(http_err) => {
                                                last_err = format!(
                                                    "ws: {last_err}; http {http_url}: {http_err}"
                                                );
                                            }
                                        }
                                    }
                                    if !ok {
                                        log::warn!(
                                            target: "bot::submit",
                                            "remote forward c_rpc → {} {} failed: {last_err}",
                                            node.name,
                                            ws_url,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    SubmitType::Rpc => {
                        let http_urls = dedupe_urls(&node.rpc);
                        if http_urls.is_empty() {
                            log::warn!(
                                target: "bot::submit",
                                "remote forward {} rpc: missing rpc URL(s)",
                                node.name,
                            );
                            continue;
                        }
                        let Some(wire_hex) = wire_hex.as_deref() else {
                            log::warn!(
                                target: "bot::submit",
                                "remote forward {} rpc: missing wire extrinsic",
                                node.name,
                            );
                            continue;
                        };
                        for http_url in http_urls {
                            match self.submit_extrinsic_http(&http_url, wire_hex).await {
                                Ok(hash) => log::info!(
                                    target: "bot::submit",
                                    "remote forward rpc → {} {} ok hash={hash}",
                                    node.name,
                                    http_url,
                                ),
                                Err(e) => log::warn!(
                                    target: "bot::submit",
                                    "remote forward rpc → {} {} failed: {e}",
                                    node.name,
                                    http_url,
                                ),
                            }
                        }
                    }
                }
            }
        }
    }

    async fn json_rpc_http(
        &self,
        url: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let response = self
            .http
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| format!("http post {url}: {e}"))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| format!("read response from {url}: {e}"))?;
        if !status.is_success() {
            return Err(format!("http {url} status {status}: {text}"));
        }
        let value: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("json parse from {url}: {e}"))?;
        if let Some(err) = value.get("error") {
            return Err(format!("rpc error from {url}: {err}"));
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| format!("missing result from {url}: {value}"))
    }

    async fn submit_prepared_ws(
        &self,
        url: &str,
        inner_hex: &str,
        hash: Option<&str>,
    ) -> Result<String, String> {
        let result = self
            .ws_pool
            .json_rpc(
                url,
                "node_submitPreparedExtrinsic",
                serde_json::json!([inner_hex, hash, null, null, null]),
            )
            .await?;
        result
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("node_submitPreparedExtrinsic missing result: {result}"))
    }

    async fn submit_prepared_http(
        &self,
        url: &str,
        inner_hex: &str,
        hash: Option<&str>,
    ) -> Result<String, String> {
        let result = self
            .json_rpc_http(
                url,
                "node_submitPreparedExtrinsic",
                serde_json::json!([inner_hex, hash, null, null, null]),
            )
            .await?;
        result
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("node_submitPreparedExtrinsic missing result: {result}"))
    }

    async fn submit_extrinsic_http(&self, url: &str, wire_hex: &str) -> Result<String, String> {
        let result = self
            .json_rpc_http(url, "author_submitExtrinsic", serde_json::json!([wire_hex]))
            .await?;
        result
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("author_submitExtrinsic missing result: {result}"))
    }
}

struct WsSubmitPool {
    inner: Mutex<HashMap<String, EndpointWorker>>,
}

struct EndpointWorker {
    tx: mpsc::UnboundedSender<WsRpcRequest>,
    task: tokio::task::JoinHandle<()>,
}

struct WsRpcRequest {
    frame: String,
    response: oneshot::Sender<Result<serde_json::Value, String>>,
}

impl WsSubmitPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
        })
    }

    async fn json_rpc(
        self: &Arc<Self>,
        ws_url: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let url = ws_url.trim().to_string();
        if !url.starts_with("ws://") && !url.starts_with("wss://") {
            return Err(format!("ws submit pool requires ws:// or wss:// URL, got {url}"));
        }

        let worker_tx = {
            let mut inner = self.inner.lock().expect("poisoned");
            if let Some(worker) = inner.get(&url) {
                worker.tx.clone()
            } else {
                let (tx, rx) = mpsc::unbounded_channel();
                let task = tokio::spawn(ws_rpc_writer_loop(url.clone(), rx));
                inner.insert(url.clone(), EndpointWorker { tx: tx.clone(), task });
                tx
            }
        };

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let (response_tx, response_rx) = oneshot::channel();
        worker_tx
            .send(WsRpcRequest {
                frame: body.to_string(),
                response: response_tx,
            })
            .map_err(|_| format!("ws submit channel closed for {url}"))?;

        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(format!("ws submit response dropped for {url}")),
        }
    }

    fn disconnect(&self, url: &str) {
        let url = url.trim();
        if url.is_empty() {
            return;
        }
        let Some(worker) = self.inner.lock().expect("poisoned").remove(url) else {
            return;
        };
        drop(worker.tx);
        worker.task.abort();
    }

    fn clear(&self) {
        let workers: Vec<EndpointWorker> = self
            .inner
            .lock()
            .expect("poisoned")
            .drain()
            .map(|(_, worker)| worker)
            .collect();
        for worker in workers {
            drop(worker.tx);
            worker.task.abort();
        }
    }

    fn retain(&self, keep: &HashSet<String>) {
        let stale: Vec<String> = self
            .inner
            .lock()
            .expect("poisoned")
            .keys()
            .filter(|url| !keep.contains(url.as_str()))
            .cloned()
            .collect();
        for url in stale {
            self.disconnect(&url);
        }
    }
}

fn c_rpc_urls_from_nodes(nodes: &[RemoteNodeEntry]) -> HashSet<String> {
    let mut urls = HashSet::new();
    for node in nodes {
        for url in &node.c_rpc {
            let trimmed = url.trim();
            if !trimmed.is_empty() {
                urls.insert(trimmed.to_string());
            }
        }
    }
    urls
}

async fn ws_rpc_writer_loop(url: String, mut rx: mpsc::UnboundedReceiver<WsRpcRequest>) {
    loop {
        match connect_async(&url).await {
            Ok((ws, _)) => {
                log::info!(target: "bot::submit", "remote submit ws connected: {url}");
                let (mut write, mut read) = ws.split();
                loop {
                    tokio::select! {
                        req = rx.recv() => {
                            match req {
                                Some(WsRpcRequest { frame, response }) => {
                                    if write.send(Message::Text(frame.into())).await.is_err() {
                                        let _ = response.send(Err("ws send failed".into()));
                                        break;
                                    }
                                    let text = match read_next_json_text(&mut read).await {
                                        Ok(text) => text,
                                        Err(e) => {
                                            let _ = response.send(Err(e));
                                            break;
                                        }
                                    };
                                    let value: serde_json::Value = match serde_json::from_str(&text) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            let _ = response.send(Err(format!("ws json parse: {e}")));
                                            continue;
                                        }
                                    };
                                    if let Some(err) = value.get("error") {
                                        let _ = response.send(Err(format!("ws rpc error: {err}")));
                                        continue;
                                    }
                                    let result = value
                                        .get("result")
                                        .cloned()
                                        .ok_or_else(|| format!("ws rpc missing result: {text}"));
                                    let _ = response.send(result);
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
                    target: "bot::submit",
                    "remote submit ws connect to {url} failed: {e}; retrying in 2s",
                );
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

async fn read_next_json_text(
    read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
) -> Result<String, String> {
    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
            Some(Ok(Message::Binary(bin))) => {
                return String::from_utf8(bin.to_vec()).map_err(|e| format!("ws binary utf8: {e}"));
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) => return Err("ws closed before response".into()),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(format!("ws read: {e}")),
            None => return Err("ws closed before response".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_nodes::{RemoteNodeEntry, RemoteNodesControl};

    fn sample_node(c_rpc: &[&str]) -> RemoteNodeEntry {
        RemoteNodeEntry {
            name: "B".into(),
            peer_id: String::new(),
            multiaddr: None,
            rpc: vec![],
            c_rpc: c_rpc.iter().map(|s| (*s).to_string()).collect(),
            submit_types: None,
        }
    }

    #[test]
    fn c_rpc_urls_from_nodes_trims_and_skips_empty() {
        let nodes = vec![sample_node(&[" ws://a ", ""])];
        let urls = c_rpc_urls_from_nodes(&nodes);
        assert_eq!(urls.len(), 1);
        assert!(urls.contains("ws://a"));
    }

    #[test]
    fn disable_clears_ws_pool_without_panic() {
        let nodes = Arc::new(RemoteNodesControl::new());
        nodes
            .set_all(vec![sample_node(&["ws://127.0.0.1:9952"])])
            .unwrap();
        let ctrl = RemoteSubmitControl::new(nodes);
        ctrl.disable();
        ctrl.sync_ws_connections();
    }
}
