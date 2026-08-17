#!/usr/bin/env python3
"""Slack watchdog for Zakura fleet status dashboards.

Polls one or more `zakura-cluster-status.py` `/data` endpoints, tracks sustained
node failures and extended block intervals in a small JSON state file, and posts
transition alerts to Slack.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


DOWN_HEALTH = {"down", "rpc_error"}
STATE_VERSION = 1
BLOCK_INTERVAL_TOLERANCE_SECONDS = 30.0


@dataclass(frozen=True)
class Fleet:
    name: str
    url: str
    dashboard_url: str
    target_block_seconds: float = 75.0
    block_explorer_url: str = ""


class SuccessorEvidenceKind(Enum):
    WAITING = "waiting"
    SAME_BRANCH = "same_branch"
    BRANCH_CHANGED = "branch_changed"
    RETRYABLE_MISSING = "retryable_missing"
    TERMINAL_MISSING = "terminal_missing"


@dataclass(frozen=True)
class SuccessorEvidence:
    kind: SuccessorEvidenceKind
    interval_seconds: int | None = None
    detail: str = ""


def load_fleets(config_path: Path) -> list[Fleet]:
    with config_path.open("rb") as config_file:
        data = tomllib.load(config_file)

    fleets = []
    seen = set()
    for raw in data.get("fleets", []):
        for required in ("name", "url"):
            if required not in raw:
                raise SystemExit(f"fleet missing required field '{required}': {raw}")

        name = str(raw["name"])
        if name in seen:
            raise SystemExit(f"duplicate fleet name: {name}")
        seen.add(name)

        url = str(raw["url"])
        dashboard_url = str(raw.get("dashboard_url") or url.removesuffix("/data"))
        block_explorer_url = str(raw.get("block_explorer_url") or "").rstrip("/")
        try:
            target_block_seconds = float(raw.get("target_block_seconds", 75.0))
        except (TypeError, ValueError) as error:
            raise SystemExit(
                f"fleet {name} has invalid target_block_seconds: "
                f"{raw.get('target_block_seconds')}"
            ) from error
        if not math.isfinite(target_block_seconds) or target_block_seconds <= 0:
            raise SystemExit(
                f"fleet {name} target_block_seconds must be a finite number "
                "greater than zero"
            )
        fleets.append(
            Fleet(
                name=name,
                url=url,
                dashboard_url=dashboard_url,
                target_block_seconds=target_block_seconds,
                block_explorer_url=block_explorer_url,
            )
        )

    if not fleets:
        raise SystemExit(f"no [[fleets]] defined in {config_path}")

    return fleets


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"version": STATE_VERSION, "nodes": {}, "fleets": {}, "chains": {}}

    with state_path.open(encoding="utf-8") as state_file:
        state = json.load(state_file)

    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        return {"version": STATE_VERSION, "nodes": {}, "fleets": {}, "chains": {}}

    state.setdefault("nodes", {})
    state.setdefault("fleets", {})
    state.setdefault("chains", {})
    return state


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    with tmp_path.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)
        state_file.write("\n")
    tmp_path.replace(state_path)


def fetch_json(url: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()

    decoded = json.loads(body.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"expected JSON object from {url}")
    return decoded


def coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"

    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m" if seconds == 0 else f"{minutes}m {seconds}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h" if minutes == 0 else f"{hours}h {minutes}m"


def format_header_interval(seconds: int) -> str:
    if seconds < 0:
        return f"-{format_duration(-seconds)}"
    return format_duration(seconds)


def suppression_until(path: Path) -> float | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as error:
        print(f"warning: could not read suppression file {path}: {error}", file=sys.stderr)
        return None

    try:
        return float(raw)
    except ValueError:
        print(f"warning: invalid suppression timestamp in {path}: {raw}", file=sys.stderr)
        return None


def slack_webhook_url() -> str:
    """Return the configured incoming webhook URL for #zakura-alerts.

    Bot tokens are intentionally unsupported: a token without channel
    membership fails with `not_in_channel` and previously masked webhook
    misconfiguration.
    """
    return (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )


def post_slack(text: str, args: argparse.Namespace) -> bool:
    webhook = slack_webhook_url()
    if args.dry_run:
        print(f"dry-run Slack message:\n{text}\n")
        return True

    if not webhook:
        print(
            "SLACK_WEB_HOOK (or SLACK_WEBHOOK_URL / SLACK_WEBHOOK) is not set; "
            f"cannot post:\n{text}\n",
            file=sys.stderr,
        )
        return False

    return post_slack_webhook(webhook, text, args)


def post_slack_webhook(webhook: str, text: str, args: argparse.Namespace) -> bool:
    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=args.slack_timeout) as response:
            body = response.read().decode("utf-8", errors="replace").strip()
    except (OSError, urllib.error.URLError) as error:
        print(f"Slack webhook post failed: {error}", file=sys.stderr)
        return False

    if response.status < 200 or response.status >= 300 or body != "ok":
        print(
            f"Slack webhook post failed: status={response.status} body={body}",
            file=sys.stderr,
        )
        return False

    return True


def node_condition(
    row: dict[str, Any],
    now: float,
    grace_since: float,
    args: argparse.Namespace,
    suppress_stall: bool = False,
) -> tuple[str, float, float]:
    health = str(row.get("health") or "unknown")
    seconds_since_advanced = coerce_float(row.get("seconds_since_advanced"))

    if health == "starting" and now - grace_since < args.starting_grace:
        return ("ok", now, 0)

    if health in DOWN_HEALTH:
        return ("down", now, args.down_after)

    if (
        not suppress_stall
        and seconds_since_advanced is not None
        and seconds_since_advanced >= args.stalled_after
    ):
        return ("stalled", now - seconds_since_advanced, args.stalled_after)

    return ("ok", now, 0)


def agreed_int(rows: list[dict[str, Any]], key: str) -> int | None:
    values = [
        value
        for row in rows
        if (value := coerce_int(row.get(key))) is not None
    ]
    if len(values) < 2:
        return None
    return values[0] if len(set(values)) == 1 else None


def agreed_hash(rows: list[dict[str, Any]], key: str) -> str | None:
    values = [str(row.get(key) or "") for row in rows if row.get(key)]
    if len(values) < 2:
        return None
    return values[0] if len(set(values)) == 1 else None


def dashboard_poll_age(snapshot: dict[str, Any]) -> float | None:
    generated_at = coerce_float(snapshot.get("generated_at"))
    last_poll = coerce_float(snapshot.get("last_poll"))
    if generated_at is None or last_poll is None:
        return None
    return generated_at - last_poll


def majority_tip(
    snapshot: dict[str, Any], rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Return the usable fleet-majority tip and whether it can replace node stalls."""
    chain = snapshot.get("chain")
    if not isinstance(chain, dict):
        return None

    height = coerce_int(chain.get("majority_height"))
    block_hash = str(chain.get("majority_hash") or "")
    if height is None or not block_hash:
        return None

    tipped_rows = [
        row
        for row in rows
        if coerce_int(row.get("height")) is not None and row.get("block_hash")
    ]
    members = [
        row
        for row in tipped_rows
        if coerce_int(row.get("height")) == height
        and str(row.get("block_hash") or "") == block_hash
        and str(row.get("health") or "unknown") not in DOWN_HEALTH
    ]
    if not members:
        return None

    has_ahead_tip = any(coerce_int(row.get("height")) > height for row in tipped_rows)
    has_competing_tip = any(
        coerce_int(row.get("height")) == height
        and str(row.get("block_hash") or "") != block_hash
        for row in tipped_rows
    )
    stalled_for = [coerce_float(row.get("seconds_since_advanced")) for row in members]
    min_stalled_for = (
        min(value for value in stalled_for if value is not None)
        if all(value is not None for value in stalled_for)
        else None
    )
    return {
        "height": height,
        "block_hash": block_hash,
        "node_names": sorted(str(row.get("name") or "unknown") for row in members),
        "client_names": sorted(
            {str(row.get("client_name") or "unknown") for row in members}
        ),
        "node_count": len(members),
        "min_stalled_for": min_stalled_for,
        "block_time": agreed_int(members, "block_time"),
        "previous_block_time": agreed_int(members, "previous_block_time"),
        "previous_hash": agreed_hash(members, "previous_hash"),
        "grandparent_hash": agreed_hash(members, "grandparent_hash"),
        "can_consolidate": (
            len(members) >= 2 and not has_ahead_tip and not has_competing_tip
        ),
    }


def reorg_observations_since(chain: object, since: float) -> int:
    """Count real orphan/reorg observations recorded during an interval."""
    if not isinstance(chain, dict):
        return 0
    events = chain.get("recent_reorgs")
    if not isinstance(events, list):
        return 0

    count = 0
    for event in events:
        if not isinstance(event, dict) or event.get("demo"):
            continue
        observed_at = coerce_float(event.get("at"))
        if observed_at is not None and observed_at >= since:
            count += 1
    return count


def format_clients(client_names: object) -> str:
    if not isinstance(client_names, (list, tuple)):
        return "unknown client"
    names = [str(name) for name in client_names if name]
    return " + ".join(names) if names else "unknown client"


def target_multiple(age: float, fleet: Fleet) -> str:
    return f"{age / fleet.target_block_seconds:.1f}×"


def block_explorer_links(fleet: Fleet, height: int | None) -> str:
    if not fleet.block_explorer_url or height is None:
        return ""

    successor_height = height + 1
    return (
        f"CipherScan: <{fleet.block_explorer_url}/{height}|block {height}> → "
        f"<{fleet.block_explorer_url}/{successor_height}|block {successor_height}>\n"
    )


def chain_interval_candidate(tip: dict[str, Any] | None, stalled_after: float) -> bool:
    return bool(
        tip is not None
        and tip["can_consolidate"]
        and tip["min_stalled_for"] is not None
        and tip["min_stalled_for"] >= stalled_after
    )


def successor_evidence(
    incident: dict[str, Any], tip: dict[str, Any]
) -> SuccessorEvidence:
    """Classify progress relative to the immutable block that opened an incident."""
    anchor_height = coerce_int(incident.get("anchor_height"))
    anchor_hash = str(incident.get("anchor_hash") or "")
    current_height = coerce_int(tip.get("height"))
    current_hash = str(tip.get("block_hash") or "")
    if anchor_height is None or not anchor_hash or current_height is None:
        return SuccessorEvidence(
            SuccessorEvidenceKind.TERMINAL_MISSING,
            detail="the persisted incident anchor or current agreed height is unavailable",
        )

    if current_height == anchor_height and current_hash == anchor_hash:
        return SuccessorEvidence(SuccessorEvidenceKind.WAITING)

    distance = current_height - anchor_height
    if current_height < anchor_height:
        return SuccessorEvidence(
            SuccessorEvidenceKind.BRANCH_CHANGED,
            detail=(
                f"the agreed tip rolled back from alerted height {anchor_height} "
                f"to {current_height}"
            ),
        )

    if current_height == anchor_height:
        return SuccessorEvidence(
            SuccessorEvidenceKind.BRANCH_CHANGED,
            detail=(
                f"alerted block `{anchor_hash}` at height {anchor_height} was "
                f"replaced by `{current_hash}`"
            ),
        )

    if distance > 2:
        return SuccessorEvidence(
            SuccessorEvidenceKind.TERMINAL_MISSING,
            detail=(
                f"the fleet advanced to H+{distance}, beyond the dashboard's exact "
                "H+1 timestamp window"
            ),
        )

    ancestor_key = "previous_hash" if distance == 1 else "grandparent_hash"
    ancestor_hash = str(tip.get(ancestor_key) or "")
    if not ancestor_hash:
        return SuccessorEvidence(
            SuccessorEvidenceKind.RETRYABLE_MISSING,
            detail=(
                f"the agreed tip did not provide a consistent hash-linked "
                f"depth-{distance} ancestor"
            ),
        )

    if ancestor_hash != anchor_hash:
        return SuccessorEvidence(
            SuccessorEvidenceKind.BRANCH_CHANGED,
            detail=(
                f"alerted block `{anchor_hash}` at height {anchor_height} is not "
                f"the agreed tip's canonical ancestor; height {anchor_height} is "
                f"now `{ancestor_hash}`"
            ),
        )

    if distance == 1:
        block_time = coerce_int(tip.get("block_time"))
        previous_block_time = coerce_int(tip.get("previous_block_time"))
        if block_time is not None and previous_block_time is not None:
            return SuccessorEvidence(
                SuccessorEvidenceKind.SAME_BRANCH,
                interval_seconds=block_time - previous_block_time,
            )
        return SuccessorEvidence(
            SuccessorEvidenceKind.RETRYABLE_MISSING,
            detail="the agreed H+1 header timestamps were unavailable or inconsistent",
        )

    if distance == 2:
        anchor_block_time = coerce_int(incident.get("anchor_block_time"))
        successor_block_time = coerce_int(tip.get("previous_block_time"))
        if anchor_block_time is not None and successor_block_time is not None:
            return SuccessorEvidence(
                SuccessorEvidenceKind.SAME_BRANCH,
                interval_seconds=successor_block_time - anchor_block_time,
            )
        return SuccessorEvidence(
            SuccessorEvidenceKind.RETRYABLE_MISSING,
            detail="the agreed H or H+1 header timestamp was unavailable",
        )

    raise AssertionError("successor distance must be one or two")


def chain_interval_alert_text(
    fleet: Fleet,
    tip: dict[str, Any],
    age: float,
    orphan_observations: int,
) -> str:
    clients = tip["client_names"]
    if {"zakurad", "zcashd"}.issubset(clients):
        classification = (
            "Zakura and its pinned zcashd compatibility sidecar agree, but "
            "the sidecar is not an independent network reference. The cause "
            "stays unclassified until the next agreed tip."
        )
    else:
        classification = (
            f"The agreeing tip is reported by {format_clients(clients)}; "
            "a shared fleet stall is not ruled out yet."
        )

    return (
        f":hourglass_flowing_sand: *Zcash {fleet.name}* extended block interval\n"
        f"no new agreed tip for {format_duration(age)} "
        f"({target_multiple(age, fleet)} the {fleet.target_block_seconds:g}s target)\n"
        f"{tip['node_count']} nodes ({format_clients(clients)}) agree at "
        f"height {tip['height']} - hash {tip['block_hash']}\n"
        f"{classification}\n"
        "individual stall notices for these agreeing nodes are consolidated here\n"
        f"orphan/reorg observations during interval: {orphan_observations}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def chain_interval_resolution_text(
    fleet: Fleet,
    incident: dict[str, Any],
    tip: dict[str, Any],
    evidence: SuccessorEvidence,
    age: float,
    orphan_observations: int,
    stalled_after: float,
) -> str:
    previous_height = coerce_int(incident.get("anchor_height"))
    current_height = tip["height"]
    clients = tip["client_names"]
    header_interval = evidence.interval_seconds
    interval_heights = (
        f" {previous_height}→{previous_height + 1}"
        if previous_height is not None
        else ""
    )
    threshold = coerce_float(incident.get("threshold_seconds")) or stalled_after
    tolerance = max(
        0.0,
        coerce_float(incident.get("tolerance_seconds")) or 0.0,
    )
    long_interval_floor = max(0.0, threshold - tolerance)
    fleet_delay_ceiling = 2 * fleet.target_block_seconds

    if evidence.kind is SuccessorEvidenceKind.BRANCH_CHANGED:
        title = f":twisted_rightwards_arrows: *Zcash {fleet.name}* canonical branch changed"
        classification = (
            f"{evidence.detail}; this is consistent with an orphan/reorg and the "
            "original interval is not classified as a Zakura fleet delay"
        )
    elif header_interval is not None and header_interval >= threshold:
        title = (
            f":stopwatch: *Zcash {fleet.name}* "
            "long canonical block interval confirmed"
        )
        classification = (
            f"canonical header interval{interval_heights}: "
            f"{format_header_interval(header_interval)}; "
            "this confirms a long canonical block interval, so a Zakura fleet "
            "delay is not needed to explain the alert"
        )
    elif header_interval is not None and header_interval >= long_interval_floor:
        title = (
            f":stopwatch: *Zcash {fleet.name}* "
            "long canonical block interval consistent with alert"
        )
        classification = (
            f"canonical header interval{interval_heights}: "
            f"{format_header_interval(header_interval)}; this is within the "
            f"{format_duration(tolerance)} observation allowance around the "
            "alert threshold, so a Zakura fleet delay is not needed to explain it"
        )
    elif (
        header_interval is not None
        and header_interval >= 0
        and header_interval <= fleet_delay_ceiling
    ):
        title = f":warning: *Zcash {fleet.name}* fleet delay suspected"
        classification = (
            f"canonical header interval{interval_heights}: "
            f"{format_header_interval(header_interval)}; "
            f"this was at most twice the {fleet.target_block_seconds:g}s target "
            "and materially shorter than the fleet's observed delay, so the fleet "
            "did not observe the canonical successor promptly"
        )
    elif header_interval is not None:
        title = f":information_source: *Zcash {fleet.name}* interval cause inconclusive"
        if header_interval < 0:
            classification = (
                f"canonical header interval{interval_heights}: "
                f"{format_header_interval(header_interval)}; header timestamps were "
                "nonmonotonic, so they cannot classify this event"
            )
        else:
            classification = (
                f"canonical header interval{interval_heights}: "
                f"{format_header_interval(header_interval)}; this was longer than "
                f"the {fleet.target_block_seconds:g}s target but not long enough to "
                "explain the observed delay within the polling allowance"
            )
    else:
        title = f":white_check_mark: *Zcash {fleet.name}* fleet progress resumed"
        classification = (
            f"{evidence.detail}; the exact successor header interval remained "
            "unavailable, so the event remains unclassified"
        )

    if (
        evidence.kind is not SuccessorEvidenceKind.BRANCH_CHANGED
        or (previous_height is not None and current_height > previous_height)
    ):
        explorer_links = block_explorer_links(fleet, previous_height)
    else:
        explorer_links = (
            f"CipherScan: <{fleet.block_explorer_url}/{current_height}|"
            f"canonical block {current_height}>\n"
            if fleet.block_explorer_url
            else ""
        )

    return (
        f"{title}\n"
        f"observed interval: {format_duration(age)} "
        f"({target_multiple(age, fleet)} the {fleet.target_block_seconds:g}s target)\n"
        f"agreed height {previous_height if previous_height is not None else '-'} "
        f"→ {current_height}\n"
        f"{tip['node_count']} nodes ({format_clients(clients)}) agree on the new tip\n"
        f"{classification}\n"
        f"{explorer_links}"
        f"orphan/reorg observations during interval: {orphan_observations}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def update_chain_interval_state(
    state_bucket: dict[str, Any],
    fleet: Fleet,
    tip: dict[str, Any] | None,
    chain: object,
    now: float,
    suppressed: bool,
    args: argparse.Namespace,
) -> bool:
    """Track one fleet-wide no-progress interval and confirm it on advancement."""
    key = fleet.name
    incident = dict(state_bucket.get(key, {}))
    is_open = coerce_int(incident.get("anchor_height")) is not None and bool(
        incident.get("anchor_hash")
    )
    candidate = chain_interval_candidate(tip, args.stalled_after)

    if not is_open:
        state_bucket.pop(key, None)
        if not candidate or tip is None:
            return False

        bad_since = now - float(tip["min_stalled_for"])
        age = now - bad_since
        orphan_observations = reorg_observations_since(chain, bad_since)
        if suppressed:
            print(
                f"suppressed alert for {key}: extended block interval for "
                f"{format_duration(age)}"
            )
            return True
        if post_slack(
            chain_interval_alert_text(fleet, tip, age, orphan_observations), args
        ):
            state_bucket[key] = {
                "bad_since": bad_since,
                "anchor_height": tip["height"],
                "anchor_hash": tip["block_hash"],
                "anchor_block_time": coerce_int(tip.get("block_time")),
                "threshold_seconds": float(args.stalled_after),
                "tolerance_seconds": BLOCK_INTERVAL_TOLERANCE_SECONDS,
            }
        return True

    pending_resolution_text = str(incident.get("pending_resolution_text") or "")
    if pending_resolution_text:
        if post_slack(pending_resolution_text, args):
            state_bucket.pop(key, None)
        return True

    if tip is None or not tip["can_consolidate"]:
        state_bucket[key] = incident
        return False

    evidence = successor_evidence(incident, tip)
    if evidence.kind is SuccessorEvidenceKind.WAITING:
        incident.pop("evidence_deadline", None)
        state_bucket[key] = incident
        return True

    if evidence.kind is SuccessorEvidenceKind.RETRYABLE_MISSING:
        evidence_deadline = coerce_float(incident.get("evidence_deadline"))
        if evidence_deadline is None:
            evidence_deadline = now + 2 * float(args.interval)
        if now < evidence_deadline:
            incident["evidence_deadline"] = evidence_deadline
            state_bucket[key] = incident
            return True
        evidence = SuccessorEvidence(
            SuccessorEvidenceKind.TERMINAL_MISSING,
            detail=(
                f"{evidence.detail} through the "
                f"{format_duration(2 * float(args.interval))} retry window"
            ),
        )

    bad_since = float(incident.get("bad_since", now))
    age = now - bad_since
    orphan_observations = reorg_observations_since(chain, bad_since)
    resolution_text = chain_interval_resolution_text(
        fleet,
        incident,
        tip,
        evidence,
        age,
        orphan_observations,
        args.stalled_after,
    )
    if post_slack(resolution_text, args):
        state_bucket.pop(key, None)
    else:
        incident["pending_resolution_text"] = resolution_text
        state_bucket[key] = incident
    return True


def stall_cleared(
    entry: dict[str, Any],
    height: float | None,
    block_hash: str | None = None,
) -> bool:
    """True when a stalled alert may be retired.

    A higher height or a different known tip hash proves that the node moved.
    The dashboard's stall timer lives in memory, so a timer reset at the same
    height and hash must not clear the alert.
    """
    if entry.get("condition") != "stalled":
        return True
    alert_height = coerce_float(entry.get("alert_height"))
    if alert_height is None:
        return True
    if height is not None and height > alert_height:
        return True
    alert_hash = str(entry.get("alert_hash") or "")
    current_hash = str(block_hash or "")
    return bool(alert_hash and current_hash and current_hash != alert_hash)


def update_alert_state(
    state_bucket: dict[str, Any],
    key: str,
    condition: str,
    bad_since: float,
    threshold: float,
    alert_text: str,
    recovery_text: str,
    now: float,
    suppressed: bool,
    args: argparse.Namespace,
    height: float | None = None,
    block_hash: str | None = None,
) -> None:
    entry = state_bucket.get(key, {"condition": "ok", "alerting": False})
    was_alerting = bool(entry.get("alerting"))

    if condition == "ok":
        if was_alerting:
            if not stall_cleared(entry, height, block_hash):
                # Keep the alert latched; the timer reset but the node did not move.
                anchor = coerce_float(entry.get("alert_height"))
                if height is not None and anchor is not None and height < anchor:
                    # The node was wiped, rolled back, or restarted onto a shorter
                    # chain. Follow it down: anchored at the old tip the alert could
                    # only clear once it re-synced past it, so a node that was fixed
                    # by a resync would never post a recovery.
                    entry = {**entry, "alert_height": height}
                if not entry.get("alert_hash") and block_hash:
                    entry = {**entry, "alert_hash": block_hash}
                state_bucket[key] = entry
                return
            if post_slack(recovery_text, args):
                state_bucket[key] = {"condition": "ok", "alerting": False}
            return

        state_bucket[key] = {"condition": "ok", "alerting": False}
        return

    if entry.get("condition") == condition:
        bad_since = min(float(entry.get("bad_since", bad_since)), bad_since)
        alerting = was_alerting
    else:
        alerting = False

    age = now - bad_since
    next_entry = {
        "condition": condition,
        "bad_since": bad_since,
        "alerting": alerting,
        "last_seen": now,
    }

    if alerting:
        anchor = entry.get("alert_height")
        if anchor is None and condition == "stalled":
            # The alert fired on a sample with no usable height, so it has nothing
            # to compare against and would clear on the first reset timer. Anchor
            # on the first sample that does report one.
            anchor = height
        if anchor is not None:
            next_entry["alert_height"] = anchor
        anchor_hash = entry.get("alert_hash")
        if not anchor_hash and condition == "stalled":
            anchor_hash = block_hash
        if anchor_hash:
            next_entry["alert_hash"] = anchor_hash

    if not alerting and age >= threshold:
        if suppressed:
            print(f"suppressed alert for {key}: {condition} for {format_duration(age)}")
        elif post_slack(alert_text, args):
            next_entry["alerting"] = True
            next_entry["last_alert_at"] = now
            if condition == "stalled" and height is not None:
                # Anchor recovery to this height, not to the dashboard's timer.
                next_entry["alert_height"] = height
            if condition == "stalled" and block_hash:
                next_entry["alert_hash"] = block_hash

    state_bucket[key] = next_entry


def node_alert_text(fleet: Fleet, row: dict[str, Any], condition: str, age: float) -> str:
    name = row.get("name") or "unknown"
    health = row.get("health") or "unknown"
    height = row.get("height")
    detail = row.get("detail") or "no detail"
    height_text = str(height) if height is not None else "-"

    return (
        f":rotating_light: *Zakura {fleet.name}* - `{name}` {condition} "
        f"for {format_duration(age)}\n"
        f"health: {health} - height: {height_text} - detail: {detail}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def node_recovery_text(fleet: Fleet, row: dict[str, Any], previous: dict[str, Any]) -> str:
    name = row.get("name") or "unknown"
    condition = previous.get("condition") or "unhealthy"
    height = row.get("height")
    height_text = str(height) if height is not None else "-"

    return (
        f":white_check_mark: *Zakura {fleet.name}* - `{name}` recovered "
        f"from {condition}\n"
        f"health: {row.get('health') or 'unknown'} - height: {height_text}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def fleet_alert_text(fleet: Fleet, error: Exception, age: float) -> str:
    return (
        f":rotating_light: *Zakura {fleet.name}* dashboard telemetry unavailable "
        f"for {format_duration(age)}\n"
        f"endpoint: {fleet.url}\n"
        f"error: {error}"
    )


def fleet_recovery_text(fleet: Fleet, previous: dict[str, Any]) -> str:
    return (
        f":white_check_mark: *Zakura {fleet.name}* dashboard telemetry recovered\n"
        f"endpoint: {fleet.url}"
    )


class Watchdog:
    def __init__(self, fleets: list[Fleet], args: argparse.Namespace):
        self.fleets = fleets
        self.args = args
        self.started_at = time.time()
        self.fetch_recovered_at: dict[str, float] = {}

    def run_once(self, state: dict[str, Any]) -> None:
        now = time.time()
        suppressed_until = suppression_until(self.args.suppression_file)
        suppressed = suppressed_until is not None and suppressed_until > now

        for fleet in self.fleets:
            try:
                snapshot = fetch_json(fleet.url, self.args.request_timeout)
            except Exception as error:
                self.handle_fleet_error(state, fleet, error, now, suppressed)
                continue

            poll_age = dashboard_poll_age(snapshot)
            if poll_age is None:
                self.handle_fleet_error(
                    state,
                    fleet,
                    RuntimeError("snapshot is missing generated_at or last_poll"),
                    now,
                    suppressed,
                )
                continue
            if poll_age < 0:
                freshness_error = RuntimeError(
                    "snapshot generated_at precedes last_poll"
                )
            elif poll_age > max(float(self.args.interval), 1.0):
                freshness_error = RuntimeError(
                    f"last completed poll is {poll_age:.1f}s behind the snapshot"
                )
            else:
                freshness_error = None
            if freshness_error is not None:
                self.handle_fleet_error(
                    state,
                    fleet,
                    freshness_error,
                    now,
                    suppressed,
                )
                continue

            self.handle_fleet_recovered(state, fleet, now)
            rows = snapshot.get("rows", [])
            if not isinstance(rows, list):
                rows = []
            rows = [row for row in rows if isinstance(row, dict)]
            consolidated_nodes = self.handle_chain(
                state, fleet, snapshot, rows, now, suppressed
            )

            for row in rows:
                self.handle_node(
                    state,
                    fleet,
                    row,
                    now,
                    suppressed,
                    suppress_stall=str(row.get("name") or "unknown")
                    in consolidated_nodes,
                )

    def handle_chain(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        snapshot: dict[str, Any],
        rows: list[dict[str, Any]],
        now: float,
        suppressed: bool,
    ) -> set[str]:
        tip = majority_tip(snapshot, rows)
        bucket = state.setdefault("chains", {})
        was_open = fleet.name in bucket
        suppress_stalls = update_chain_interval_state(
            bucket,
            fleet,
            tip,
            snapshot.get("chain"),
            now,
            suppressed,
            self.args,
        )
        if tip is None or not tip["can_consolidate"] or not suppress_stalls:
            return set()

        consolidated_nodes = set(tip["node_names"])
        if was_open or fleet.name in bucket:
            node_bucket = state.setdefault("nodes", {})
            for node_name in consolidated_nodes:
                node_key = f"{fleet.name}/{node_name}"
                if node_bucket.get(node_key, {}).get("condition") == "stalled":
                    node_bucket[node_key] = {"condition": "ok", "alerting": False}

        return consolidated_nodes

    def handle_fleet_error(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        error: Exception,
        now: float,
        suppressed: bool,
    ) -> None:
        key = fleet.name
        bucket = state.setdefault("fleets", {})
        entry = bucket.get(key, {})
        bad_since = (
            float(entry.get("bad_since", now))
            if entry.get("condition") == "unreachable"
            else now
        )
        age = now - bad_since
        update_alert_state(
            bucket,
            key,
            "unreachable",
            bad_since,
            self.args.dashboard_down_after,
            fleet_alert_text(fleet, error, age),
            fleet_recovery_text(fleet, entry),
            now,
            suppressed,
            self.args,
        )

    def handle_fleet_recovered(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        now: float,
    ) -> None:
        key = fleet.name
        bucket = state.setdefault("fleets", {})
        previous = dict(bucket.get(key, {}))
        if previous.get("condition") == "unreachable":
            self.fetch_recovered_at[fleet.name] = now

        update_alert_state(
            bucket,
            key,
            "ok",
            now,
            0,
            "",
            fleet_recovery_text(fleet, previous),
            now,
            False,
            self.args,
        )

    def handle_node(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        row: dict[str, Any],
        now: float,
        suppressed: bool,
        suppress_stall: bool = False,
    ) -> None:
        node_name = str(row.get("name") or "unknown")
        key = f"{fleet.name}/{node_name}"
        bucket = state.setdefault("nodes", {})
        previous = dict(bucket.get(key, {}))
        grace_since = max(self.started_at, self.fetch_recovered_at.get(fleet.name, 0))
        condition, bad_since, threshold = node_condition(
            row, now, grace_since, self.args, suppress_stall
        )
        if condition != "ok" and previous.get("condition") == condition:
            bad_since = min(float(previous.get("bad_since", bad_since)), bad_since)
        age = now - bad_since

        update_alert_state(
            bucket,
            key,
            condition,
            bad_since,
            threshold,
            node_alert_text(fleet, row, condition, age),
            node_recovery_text(fleet, row, previous),
            now,
            suppressed,
            self.args,
            coerce_float(row.get("height")),
            str(row.get("block_hash") or "") or None,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Alert Slack about unhealthy Zakura nodes and extended block intervals."
        )
    )
    parser.add_argument("--config", required=True, type=Path, help="fleet TOML config")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("/var/lib/zakura-fleet-watchdog/state.json"),
        help="JSON file used to persist alert state",
    )
    parser.add_argument("--interval", type=float, default=60.0, help="poll interval seconds")
    parser.add_argument(
        "--down-after",
        type=float,
        default=600.0,
        help="alert after down/rpc_error has persisted this many seconds",
    )
    parser.add_argument(
        "--stalled-after",
        type=float,
        default=600.0,
        help="alert after no node or agreed-fleet block progress for this many seconds",
    )
    parser.add_argument(
        "--dashboard-down-after",
        type=float,
        default=600.0,
        help="alert after a dashboard fetch failure persists this many seconds",
    )
    parser.add_argument(
        "--starting-grace",
        type=float,
        default=120.0,
        help="ignore starting nodes for this many seconds after startup or fetch recovery",
    )
    parser.add_argument(
        "--suppression-file",
        type=Path,
        default=Path("/run/zakura-fleet-watchdog/deploy-suppressed-until"),
        help="Unix timestamp file that suppresses failure alerts while in the future",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=20.0,
        help="dashboard request timeout seconds",
    )
    parser.add_argument(
        "--slack-timeout",
        type=float,
        default=20.0,
        help="Slack webhook request timeout seconds",
    )
    parser.add_argument("--once", action="store_true", help="poll once, update state, and exit")
    parser.add_argument("--dry-run", action="store_true", help="log Slack messages instead")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fleets = load_fleets(args.config)
    watchdog = Watchdog(fleets, args)

    while True:
        state = load_state(args.state_file)
        watchdog.run_once(state)
        save_state(args.state_file, state)

        if args.once:
            return 0

        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
