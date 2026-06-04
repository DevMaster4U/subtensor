//! Log libp2p ping RTT per connected peer (RPC-controlled).

use std::sync::Arc;

use futures::{FutureExt, StreamExt};
use sc_network::{Event, NetworkEventStream};
use sc_service::TaskManager;

use crate::metrics_log::MetricsLogControl;
use crate::peer_scoreboard::PeerScoreboard;

pub fn start_peer_ping_log_watcher(
    task_manager: &TaskManager,
    network: Arc<dyn NetworkEventStream + Send + Sync>,
    scoreboard: Arc<PeerScoreboard>,
    control: Arc<MetricsLogControl>,
) {
    task_manager.spawn_handle().spawn(
        "bot-peer-ping-log",
        None,
        async move {
            let mut events = network.event_stream("bot-peer-ping-log");
            loop {
                let Some(event) = events.next().await else {
                    break;
                };
                let Event::Ping(ping) = event else {
                    continue;
                };
                let peer_id = ping.peer.to_base58();
                let rtt_ms = ping.rtt.as_millis() as u64;
                scoreboard.record_ping(&peer_id, rtt_ms);
                if control.peer_rtt() {
                    log::info!(
                        target: "bot::metrics",
                        "peer_rtt peer={peer_id} rtt_ms={rtt_ms}",
                    );
                }
            }
        }
        .boxed(),
    );
}
