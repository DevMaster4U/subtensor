//! Length-prefixed JSON IPC protocol between `node-subtensor` and the bot client.
//!
//! ## Message types
//!
//! | `type` | Direction | Purpose |
//! |--------|-----------|---------|
//! | `header` | node → bot | Block announce (`header_number` + metadata) |
//! | `mempool` | node → bot | Pool import notification (`info` = JSON object string) |
//! | `find_peer` | node → bot | Discovered peer (`peer_id` + `multiaddr`) |
//! | `transaction` | bot → node | Submit tx (`extrinsic` = client-validated inner hex, or legacy `hash` wire hex) |
//! | `set_announce` | bot → node | Per-client announce filter (`announce_type`, `value`) |
//! | `set_require_mempool` | bot → node | Opt in to mempool notifications |
//! | `set_require_peer_find` | bot → node | Opt in to peer-find notifications (when node peer log is on) |

use codec::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Default Unix socket path (`SUBTENSOR_IPC_PATH` overrides).
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/subtensor-bot.sock";

/// Peer propagation mode for tx gossip (mirrors node `PeerManageMode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerManageMode {
    OnlyCustom,
    Both,
    OnlySystem,
}

impl PeerManageMode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::OnlyCustom),
            1 => Some(Self::Both),
            2 => Some(Self::OnlySystem),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Self::OnlyCustom => 0,
            Self::Both => 1,
            Self::OnlySystem => 2,
        }
    }
}

/// Announce filter type for [`IpcMessage::SetAnnounce`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnounceFilterType {
    /// Send first `value` announces per block (`0` = all).
    Count,
    /// Send when `(elapsed_secs % 12) * 1000` matches `value` (milliseconds).
    DelayTime,
}

/// Transaction propagation strategy on [`IpcMessage::Transaction`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropagateType {
    /// Rank peers by `propagate_param` (`avg_announce_time` or `first_announce_hit_count`).
    Normal,
    /// Propagate in parallel to the first N block announcers (`propagate_param` = count).
    Announce,
    /// Propagate in parallel to all connected tx peers.
    Parallel,
}

/// IPC frame (node ↔ bot).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcMessage {
    /// Block announce header (node → bot).
    Header {
        header_number: u32,
        hash: String,
        parent_hash: String,
        slot: Option<u64>,
        announcing_peer: Option<String>,
        /// 1-based announce index for this block height on this node.
        announce_index: u32,
        /// Milliseconds within the 12-second slot cycle when this announce arrived.
        delay_time_ms: u64,
    },
    /// Mempool watcher notification (node → bot).
    ///
    /// `info` is a serialized JSON object: `{"tx_hash":"0x..","extrinsic":"0x.."}`.
    Mempool {
        info: String,
    },
    /// Peer discovered via node peer logging (node → bot).
    FindPeer {
        peer_id: String,
        multiaddr: String,
    },
    /// Transaction to import and gossip (bot → node).
    ///
    /// Prefer [`Self::Transaction::extrinsic`]: hex-encoded **inner** opaque payload,
    /// already validated on the client (node skips SCALE decode).
    ///
    /// Legacy [`Self::Transaction::hash`]: full wire hex (`0x` + SCALE opaque extrinsic);
    /// the node hex-decodes and SCALE-decodes on the hot path.
    Transaction {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        hash: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extrinsic: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        propagate_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        propagate_param: Option<String>,
    },
    /// Configure per-client block announce filtering (bot → node).
    SetAnnounce {
        announce_type: String,
        value: u64,
    },
    /// Opt in to mempool notifications (bot → node).
    SetRequireMempool {
        enabled: bool,
    },
    /// Opt in to peer-find notifications when node peer logging is enabled (bot → node).
    SetRequirePeerFind {
        enabled: bool,
    },
}

impl IpcMessage {
    pub fn header(
        header_number: u32,
        hash: String,
        parent_hash: String,
        slot: Option<u64>,
        announcing_peer: Option<String>,
        announce_index: u32,
        delay_time_ms: u64,
    ) -> Self {
        Self::Header {
            header_number,
            hash,
            parent_hash,
            slot,
            announcing_peer,
            announce_index,
            delay_time_ms,
        }
    }

    pub fn mempool(info: String) -> Self {
        Self::Mempool { info }
    }

    pub fn find_peer(peer_id: String, multiaddr: String) -> Self {
        Self::FindPeer {
            peer_id,
            multiaddr,
        }
    }

    pub fn transaction(hash: String) -> Self {
        Self::Transaction {
            hash,
            extrinsic: None,
            propagate_type: None,
            propagate_param: None,
        }
    }

    /// Client-prevalidated inner opaque payload (hex). Node only hex-decodes — no SCALE decode.
    pub fn transaction_prepared(extrinsic_hex: String) -> Self {
        Self::Transaction {
            hash: String::new(),
            extrinsic: Some(extrinsic_hex),
            propagate_type: None,
            propagate_param: None,
        }
    }

    pub fn transaction_with_propagate(
        hash: String,
        propagate_type: impl Into<String>,
        propagate_param: impl Into<String>,
    ) -> Self {
        Self::Transaction {
            hash,
            extrinsic: None,
            propagate_type: Some(propagate_type.into()),
            propagate_param: Some(propagate_param.into()),
        }
    }

    pub fn transaction_prepared_with_propagate(
        extrinsic_hex: String,
        propagate_type: impl Into<String>,
        propagate_param: impl Into<String>,
    ) -> Self {
        Self::Transaction {
            hash: String::new(),
            extrinsic: Some(extrinsic_hex),
            propagate_type: Some(propagate_type.into()),
            propagate_param: Some(propagate_param.into()),
        }
    }

    pub fn set_announce(announce_type: impl Into<String>, value: u64) -> Self {
        Self::SetAnnounce {
            announce_type: announce_type.into(),
            value,
        }
    }

    pub fn set_require_mempool(enabled: bool) -> Self {
        Self::SetRequireMempool { enabled }
    }

    pub fn set_require_peer_find(enabled: bool) -> Self {
        Self::SetRequirePeerFind { enabled }
    }
}

/// Encode a frame: 4-byte big-endian length + JSON payload.
pub fn encode_frame<T: Serialize>(msg: &T) -> Result<Vec<u8>, String> {
    let body = serde_json::to_vec(msg).map_err(|e| format!("serialize: {e}"))?;
    if body.len() > u32::MAX as usize {
        return Err("frame too large".into());
    }
    let len = (body.len() as u32).to_be_bytes();
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&len);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode one frame from a buffer; returns `(message, consumed_bytes)`.
pub fn decode_frame<T: for<'de> Deserialize<'de>>(buf: &[u8]) -> Result<Option<(T, usize)>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let msg: T = serde_json::from_slice(&buf[4..4 + len]).map_err(|e| format!("deserialize: {e}"))?;
    Ok(Some((msg, 4 + len)))
}

/// Strip optional `0x` and hex-decode.
pub fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    let stripped = hex.trim().trim_start_matches("0x");
    hex::decode(stripped).map_err(|e| format!("invalid hex: {e}"))
}

/// Extract the inner opaque extrinsic payload from SCALE wire bytes
/// (`compact(length)` + payload).
pub fn opaque_inner_from_wire(wire: &[u8]) -> Result<Vec<u8>, String> {
    let mut cursor = wire;
    let len = codec::Compact::<u32>::decode(&mut cursor).map_err(|e| format!("compact length: {e}"))?;
    let n = len.0 as usize;
    if cursor.len() < n {
        return Err("truncated extrinsic payload".into());
    }
    Ok(cursor[..n].to_vec())
}

/// Rebuild SCALE wire bytes (`compact(length)` + payload) from inner opaque payload.
pub fn encode_opaque_wire(inner: &[u8]) -> Vec<u8> {
    let mut wire = Vec::with_capacity(inner.len() + 4);
    codec::Compact(inner.len() as u32).encode_to(&mut wire);
    wire.extend_from_slice(inner);
    wire
}

/// Validate wire bytes and return hex-encoded inner payload for [`IpcMessage::transaction_prepared`].
pub fn prepare_extrinsic_hex(wire_hex: &str) -> Result<String, String> {
    let wire = decode_hex(wire_hex)?;
    let mut cursor = &wire[..];
    let _opaque = OpaqueExtrinsic::decode(&mut cursor).map_err(|e| format!("invalid extrinsic: {e}"))?;
    let inner = opaque_inner_from_wire(&wire)?;
    Ok(format!("0x{}", hex::encode(inner)))
}

/// Opaque extrinsic wrapper used only for client-side validation in [`prepare_extrinsic_hex`].
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
struct OpaqueExtrinsic(Vec<u8>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_header() {
        let msg = IpcMessage::header(
            42,
            "0xabc".into(),
            "0xdef".into(),
            Some(7),
            Some("peer".into()),
            1,
            983,
        );
        let frame = encode_frame(&msg).unwrap();
        let (decoded, n) = decode_frame::<IpcMessage>(&frame).unwrap().unwrap();
        assert_eq!(n, frame.len());
        assert!(matches!(
            decoded,
            IpcMessage::Header {
                header_number: 42,
                announce_index: 1,
                delay_time_ms: 983,
                ..
            }
        ));
    }

    #[test]
    fn roundtrip_mempool() {
        let msg = IpcMessage::mempool(r#"{"tx_hash":"0x1"}"#.into());
        let frame = encode_frame(&msg).unwrap();
        let (decoded, _) = decode_frame::<IpcMessage>(&frame).unwrap().unwrap();
        assert!(matches!(decoded, IpcMessage::Mempool { .. }));
    }

    #[test]
    fn roundtrip_transaction() {
        let msg = IpcMessage::transaction_prepared_with_propagate(
            "0xdeadbeef".into(),
            "announce",
            "3",
        );
        let frame = encode_frame(&msg).unwrap();
        let (decoded, _) = decode_frame::<IpcMessage>(&frame).unwrap().unwrap();
        assert!(matches!(
            decoded,
            IpcMessage::Transaction {
                extrinsic: Some(ref e),
                propagate_type: Some(ref t),
                propagate_param: Some(ref p),
                ..
            } if e == "0xdeadbeef" && t == "announce" && p == "3"
        ));
    }

    #[test]
    fn delay_time_examples() {
        assert_eq!(slot_delay_ms(12.983), 983);
        assert_eq!(slot_delay_ms(13.23), 1230);
    }

    fn slot_delay_ms(elapsed_secs: f64) -> u64 {
        let remainder = elapsed_secs % 12.0;
        (remainder * 1000.0).round() as u64
    }
}
