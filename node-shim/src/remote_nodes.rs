//! Runtime registry of remote submit targets (B, C, …) used by the node after IPC transactions.

use std::fs;
use std::sync::{Arc, RwLock};

use crate::config_paths::remote_nodes_file;

/// One remote node entry (operator-controlled via `node_setRemoteNodes` / `remote_nodes.json`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoteNodeEntry {
    pub name: String,
    #[serde(
        default,
        alias = "peerId",
        alias = "peer_id",
        skip_serializing_if = "String::is_empty"
    )]
    pub peer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiaddr: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rpc: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_string_or_vec",
        alias = "cRpc",
        alias = "c_rpc",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub c_rpc: Vec<String>,
    #[serde(
        default,
        alias = "submitTypes",
        alias = "submit_types",
        skip_serializing_if = "Option::is_none"
    )]
    pub submit_types: Option<Vec<String>>,
}

/// Result of mutating the remote node registry.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoteNodesResult {
    pub nodes: Vec<RemoteNodeEntry>,
}

pub struct RemoteNodesControl {
    nodes: RwLock<Vec<RemoteNodeEntry>>,
}

impl RemoteNodesControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            nodes: RwLock::new(Vec::new()),
        })
    }

    pub fn list(&self) -> Vec<RemoteNodeEntry> {
        self.nodes.read().expect("poisoned").clone()
    }

    pub fn set_all(&self, nodes: Vec<RemoteNodeEntry>) -> Result<RemoteNodesResult, String> {
        for node in &nodes {
            self.validate_node(node)?;
        }
        Self::validate_unique_names(&nodes)?;
        *self.nodes.write().expect("poisoned") = nodes;
        self.persist()?;
        log::info!(
            target: "bot::submit",
            "remote nodes replaced (total {})",
            self.list().len(),
        );
        Ok(RemoteNodesResult { nodes: self.list() })
    }

    pub fn add(&self, node: RemoteNodeEntry) -> Result<RemoteNodesResult, String> {
        self.validate_node(&node)?;
        let mut nodes = self.nodes.write().expect("poisoned");
        if let Some(existing) = nodes.iter_mut().find(|n| n.name == node.name) {
            *existing = node;
        } else {
            nodes.push(node);
        }
        let listed = nodes.clone();
        drop(nodes);
        self.persist()?;
        log::info!(
            target: "bot::submit",
            "remote node upserted (total {})",
            listed.len(),
        );
        Ok(RemoteNodesResult { nodes: listed })
    }

    pub fn remove(&self, name: &str) -> Result<RemoteNodesResult, String> {
        let name = name.trim();
        if name.is_empty() {
            return Err("remote node name must not be empty".into());
        }
        let mut nodes = self.nodes.write().expect("poisoned");
        let before = nodes.len();
        nodes.retain(|n| n.name != name);
        if nodes.len() == before {
            return Err(format!("remote node not found: {name}"));
        }
        let listed = nodes.clone();
        drop(nodes);
        self.persist()?;
        log::info!(
            target: "bot::submit",
            "remote node removed: {name} (total {})",
            listed.len(),
        );
        Ok(RemoteNodesResult { nodes: listed })
    }

    pub fn clear(&self) -> Result<RemoteNodesResult, String> {
        self.nodes.write().expect("poisoned").clear();
        self.persist()?;
        log::info!(target: "bot::submit", "remote nodes cleared");
        Ok(RemoteNodesResult { nodes: Vec::new() })
    }

    pub fn set_from_file(&self, path: &str) -> Result<RemoteNodesResult, String> {
        let path = path.trim();
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("read remote nodes file {path}: {e}"))?;
        let nodes: Vec<RemoteNodeEntry> = serde_json::from_str(&raw)
            .map_err(|e| format!("parse remote nodes JSON in {path}: {e}"))?;
        self.set_all(nodes)
    }

    pub fn load_from_default_file(&self) -> Result<u32, String> {
        let path = remote_nodes_file();
        let path = path
            .to_str()
            .ok_or_else(|| "remote nodes path is not valid UTF-8".to_string())?;
        if !std::path::Path::new(path).exists() {
            return Ok(0);
        }
        let before = self.list().len();
        self.set_from_file(path)?;
        Ok(self.list().len().saturating_sub(before) as u32)
    }

    fn validate_node(&self, node: &RemoteNodeEntry) -> Result<(), String> {
        let name = node.name.trim();
        if name.is_empty() {
            return Err("remote node name must not be empty".into());
        }
        for url in node.rpc.iter().chain(node.c_rpc.iter()) {
            validate_rpc_url(url)?;
        }
        Ok(())
    }

    fn validate_unique_names(nodes: &[RemoteNodeEntry]) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for node in nodes {
            let name = node.name.trim();
            if !seen.insert(name) {
                return Err(format!("duplicate remote node name: {name}"));
            }
        }
        Ok(())
    }

    fn persist(&self) -> Result<(), String> {
        let path = remote_nodes_file();
        let path = path
            .to_str()
            .ok_or_else(|| "remote nodes path is not valid UTF-8".to_string())?;
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create remote nodes dir {}: {e}", parent.display()))?;
        }
        let nodes = self.list();
        let json = serde_json::to_string_pretty(&nodes)
            .map_err(|e| format!("serialize remote nodes: {e}"))?;
        fs::write(path, format!("{json}\n"))
            .map_err(|e| format!("write remote nodes file {path}: {e}"))?;
        Ok(())
    }
}

fn validate_rpc_url(url: &str) -> Result<(), String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err("RPC URL must not be empty".into());
    }
    if trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("ws://")
        || trimmed.starts_with("wss://")
    {
        Ok(())
    } else {
        Err(format!(
            "RPC URL must start with http://, https://, ws://, or wss://, got {trimmed}"
        ))
    }
}

fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct StringOrVec;

    impl<'de> Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or array of RPC URLs")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value.to_string()])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut urls = Vec::new();
            while let Some(url) = seq.next_element::<String>()? {
                urls.push(url);
            }
            Ok(urls)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_list_round_trip() {
        let ctrl = RemoteNodesControl::new();
        let result = ctrl
            .set_all(vec![RemoteNodeEntry {
                name: "B".into(),
                peer_id: "12D3".into(),
                multiaddr: None,
                rpc: vec![],
                c_rpc: vec!["ws://127.0.0.1:9945".into()],
                submit_types: Some(vec!["c_rpc".into()]),
            }])
            .unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(ctrl.list()[0].name, "B");
    }
}
