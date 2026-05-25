#!/usr/bin/env python3
"""Idempotent subtensor-bot node integration. Safe to re-run."""
from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path


def root() -> Path:
    if len(sys.argv) > 1:
        return Path(sys.argv[1]).resolve()
    # scripts/apply-integration.py -> bot/scripts -> bot -> subtensor root
    return Path(__file__).resolve().parents[2]


def bot_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_line_after(content: str, anchor: str, line: str, label: str) -> tuple[str, bool]:
    if line in content:
        print(f"  ok   {label} (already present)")
        return content, False
    if anchor not in content:
        raise SystemExit(f"  FAIL {label}: anchor not found:\n    {anchor!r}")
    content = content.replace(anchor, anchor + line, 1)
    print(f"  add  {label}")
    return content, True


def ensure_workspace_member(content: str) -> tuple[str, bool]:
    if re.search(r'^\s*"bot",\s*$', content, re.MULTILINE):
        print("  ok   Cargo.toml workspace member bot (already present)")
        return content, False
    anchor = "members = [\n"
    line = '\t"bot",\n'
    return ensure_line_after(content, anchor, line, "Cargo.toml workspace member bot")


def ensure_workspace_dep(content: str) -> tuple[str, bool]:
    dep = 'subtensor-bot = { path = "bot" }'
    if dep in content:
        print("  ok   Cargo.toml subtensor-bot dep (already present)")
        return content, False
    anchor = 'node-subtensor-runtime = { path = "runtime", default-features = false }\n'
    line = f"{dep}\n"
    return ensure_line_after(content, anchor, line, "Cargo.toml subtensor-bot dep")


def ensure_node_dep(content: str) -> tuple[str, bool]:
    dep = "subtensor-bot = { workspace = true }"
    if dep in content:
        print("  ok   node/Cargo.toml subtensor-bot (already present)")
        return content, False
    anchor = 'node-subtensor-runtime = { workspace = true, features = ["std"] }\n'
    line = f"{dep}\n"
    return ensure_line_after(content, anchor, line, "node/Cargo.toml subtensor-bot")


def ensure_lib_rs(content: str) -> tuple[str, bool]:
    line = "pub mod bot_block_announce;\n"
    if line in content:
        print("  ok   node/src/lib.rs bot_block_announce mod (already present)")
        return content, False
    print("  add  node/src/lib.rs bot_block_announce mod")
    return line + content, True


def ensure_main_rs(content: str) -> tuple[str, bool]:
    shim = """mod bot_block_announce {
    pub use node_subtensor::bot_block_announce::*;
}
"""
    if shim in content:
        print("  ok   node/src/main.rs bot_block_announce mod (already present)")
        return content, False
    if "mod bot_block_announce;" in content:
        content = content.replace("mod bot_block_announce;\n", shim, 1)
        print("  update node/src/main.rs bot_block_announce re-export")
        return content, True
    anchor = "#![warn(missing_docs)]\n\n"
    return ensure_line_after(content, anchor, shim, "node/src/main.rs bot_block_announce re-export")


def patch_service_rs(content: str) -> tuple[str, bool]:
    if "subtensor_bot::processor::start_bot" in content:
        print("  ok   node/src/service.rs (already integrated)")
        return content, False

    changed = False

    import_line = "use subtensor_bot::rpc::BotApiServer;\n"
    if import_line not in content:
        anchor = "use std::{sync::Arc, time::Duration};\n"
        content, c = ensure_line_after(content, anchor, import_line, "service.rs BotApiServer import")
        changed |= c

    hub_block = """\
    let (announce_hub, _) = subtensor_bot::announce::BlockAnnounceHub::new();
    let announce_hub_for_network = announce_hub.clone();
    let announce_rx = announce_hub.subscribe();
    let bot_control = Arc::new(subtensor_bot::control::BotControl::new());
    let peer_tracker = Arc::new(subtensor_bot::peers::PeerTracker::new());

"""
    if "BlockAnnounceHub::new()" not in content:
        anchor = "    let (network, system_rpc_tx, tx_handler_controller, sync_service) =\n"
        content, c = ensure_line_after(content, anchor, hub_block, "service.rs announce hub")
        changed |= c

    old_validator = "            block_announce_validator_builder: None,\n"
    new_validator = """            block_announce_validator_builder: Some(Box::new(move |client| {
                Box::new(crate::bot_block_announce::NotifyingBlockAnnounceValidator::new(
                    client,
                    announce_hub_for_network,
                ))
            })),
"""
    if old_validator in content:
        content = content.replace(old_validator, new_validator, 1)
        print("  add  service.rs block_announce_validator_builder")
        changed = True
    elif "NotifyingBlockAnnounceValidator::new" not in content:
        raise SystemExit(
            "  FAIL service.rs: expected block_announce_validator_builder: None,\n"
            "       or existing NotifyingBlockAnnounceValidator (manual merge needed)"
        )

    rpc_clone = """        let bot_control = bot_control.clone();
        let peer_tracker = peer_tracker.clone();
"""
    if "let bot_control = bot_control.clone();" not in content:
        anchor = "        )?;\n        Box::new(move |subscription_task_executor| {\n"
        if anchor not in content:
            raise SystemExit(
                "  FAIL service.rs: rpc builder anchor not found (manual merge needed)"
            )
        content = content.replace(
            anchor,
            "        )?;\n" + rpc_clone + "        Box::new(move |subscription_task_executor| {\n",
            1,
        )
        print("  add  service.rs rpc bot clones")
        changed = True

    old_rpc = """            crate::rpc::create_full(
                deps,
                subscription_task_executor,
                pubsub_notification_sinks.clone(),
                CM::frontier_consensus_data_provider(client.clone())?,
                rpc_methods.as_slice(),
            )
            .map_err(Into::into)
"""
    new_rpc = """            let mut module = crate::rpc::create_full(
                deps,
                subscription_task_executor,
                pubsub_notification_sinks.clone(),
                CM::frontier_consensus_data_provider(client.clone())?,
                rpc_methods.as_slice(),
            )
            .map_err(Into::into)?;
            module.merge(subtensor_bot::rpc::BotRpc::new(bot_control, peer_tracker).into_rpc())?;
            Ok(module)
"""
    if old_rpc in content:
        content = content.replace(old_rpc, new_rpc, 1)
        print("  add  service.rs BotRpc merge")
        changed = True
    elif "BotRpc::new" not in content:
        raise SystemExit(
            "  FAIL service.rs: create_full block not recognized (manual merge needed)"
        )

    propagator_block = """\
    let tx_propagator =
        subtensor_bot::TxPropagator::new(tx_handler_controller.clone());

"""
    spawn_anchor = "    let _rpc_handlers = sc_service::spawn_tasks(sc_service::SpawnTasksParams {\n"
    if "TxPropagator::new" not in content:
        if spawn_anchor not in content:
            raise SystemExit(
                "  FAIL service.rs: spawn_tasks anchor not found (manual merge needed)"
            )
        content = content.replace(
            spawn_anchor,
            propagator_block + spawn_anchor,
            1,
        )
        print("  add  service.rs tx propagator")
        changed = True

    bot_start = """\
    // -- Bot ------------------------------------------------------------------
    subtensor_bot::processor::start_bot(
        &task_manager,
        client.clone(),
        transaction_pool.clone(),
        sync_service.clone(),
        announce_rx,
        bot_control.clone(),
        peer_tracker,
        tx_propagator.clone(),
    );
    subtensor_bot::pool_inject::start_pool_injector(
        &task_manager,
        client.clone(),
        transaction_pool.clone(),
        bot_control,
        tx_propagator,
    );
    // subtensor_bot::mempool::start_mempool_watcher(&task_manager, transaction_pool.clone());
    // -------------------------------------------------------------------------

"""
    if "start_pool_injector" not in content:
        anchor = "    )\n    .await;\n\n    if role.is_authority() {\n"
        if anchor not in content:
            raise SystemExit(
                "  FAIL service.rs: spawn_frontier_tasks anchor not found (manual merge needed)"
            )
        content = content.replace(anchor, "    )\n    .await;\n\n" + bot_start + "    if role.is_authority() {\n", 1)
        print("  add  service.rs start_bot + pool_injector")
        changed = True

    return content, changed


def write_if_changed(path: Path, new: str) -> bool:
    old = path.read_text(encoding="utf-8") if path.exists() else ""
    if old == new:
        return False
    path.write_text(new, encoding="utf-8", newline="\n")
    return True


def main() -> None:
    r = root()
    b = bot_dir()

    print(f"subtensor root: {r}")
    print(f"bot dir:        {b}")

    for d in ("node", "runtime"):
        if not (r / d).is_dir():
            raise SystemExit(f"error: {d}/ not found — run from subtensor repo root")

    if not (b / "Cargo.toml").is_file():
        raise SystemExit(f"error: bot crate missing at {b}")

    any_change = False

    cargo = (r / "Cargo.toml").read_text(encoding="utf-8")
    cargo, c1 = ensure_workspace_member(cargo)
    cargo, c2 = ensure_workspace_dep(cargo)
    if write_if_changed(r / "Cargo.toml", cargo):
        any_change |= c1 or c2

    node_cargo = (r / "node" / "Cargo.toml").read_text(encoding="utf-8")
    node_cargo, c3 = ensure_node_dep(node_cargo)
    if write_if_changed(r / "node" / "Cargo.toml", node_cargo):
        any_change |= c3

    lib = (r / "node" / "src" / "lib.rs").read_text(encoding="utf-8")
    lib, c4 = ensure_lib_rs(lib)
    if write_if_changed(r / "node" / "src" / "lib.rs", lib):
        any_change |= c4

    main = (r / "node" / "src" / "main.rs").read_text(encoding="utf-8")
    main, c4b = ensure_main_rs(main)
    if write_if_changed(r / "node" / "src" / "main.rs", main):
        any_change |= c4b

    src = b / "node-integration" / "bot_block_announce.rs"
    dst = r / "node" / "src" / "bot_block_announce.rs"
    if not src.is_file():
        src = b / "src" / ".." / "node-integration" / "bot_block_announce.rs"
    src = (b / "node-integration" / "bot_block_announce.rs").resolve()
    shutil.copy2(src, dst)
    print(f"  copy node/src/bot_block_announce.rs")

    service_path = r / "node" / "src" / "service.rs"
    service, c5 = patch_service_rs(service_path.read_text(encoding="utf-8"))
    if write_if_changed(service_path, service):
        any_change |= c5

    if any_change:
        print("\nIntegration applied. Run: cargo build -p node-subtensor --release")
    else:
        print("\nAlready fully integrated — nothing to change.")


if __name__ == "__main__":
    main()
