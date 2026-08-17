#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("zakura-cluster-watchdog.py")
SPEC = importlib.util.spec_from_file_location("zakura_cluster_watchdog", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
watchdog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = watchdog
SPEC.loader.exec_module(watchdog)


def make_args(**overrides):
    defaults = {
        "down_after": 600.0,
        "stalled_after": 600.0,
        "dashboard_down_after": 600.0,
        "interval": 60.0,
        "request_timeout": 20.0,
        "slack_timeout": 20.0,
        "suppression_file": Path("/tmp/zakura-test-suppression-missing"),
        "starting_grace": 120.0,
        "slack_webhook": None,
        "dry_run": True,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class FleetConfigTests(unittest.TestCase):
    def test_non_finite_target_block_intervals_are_rejected(self):
        for value in ("nan", "inf"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "fleets.toml"
                path.write_text(
                    "[[fleets]]\n"
                    'name = "mainnet"\n'
                    'url = "http://dashboard/data"\n'
                    f"target_block_seconds = {value}\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(SystemExit, "finite number greater"):
                    watchdog.load_fleets(path)


class StallRecoveryTests(unittest.TestCase):
    """A stalled node must not be declared recovered at an unchanged height."""

    def setUp(self):
        self.posted: list[str] = []
        self._real_post = watchdog.post_slack
        self._real_fetch = watchdog.fetch_json
        self._real_time = watchdog.time.time
        watchdog.post_slack = lambda text, args: (self.posted.append(text), True)[1]

    def tearDown(self):
        watchdog.post_slack = self._real_post
        watchdog.fetch_json = self._real_fetch
        watchdog.time.time = self._real_time

    def fire_stall(
        self,
        bucket,
        key="fleet/node-a",
        height=4129396,
        now=1000.0,
        block_hash=None,
    ):
        """Drive the alert past its threshold so it latches."""
        watchdog.update_alert_state(
            bucket, key, "stalled", now - 700.0, 600.0,
            "STALLED", "RECOVERED", now, False, make_args(), height, block_hash,
        )

    def test_stall_alert_records_the_height_it_fired_at(self):
        bucket = {}
        self.fire_stall(bucket)
        self.assertEqual(len(self.posted), 1)
        entry = bucket["fleet/node-a"]
        self.assertTrue(entry["alerting"])
        self.assertEqual(entry["alert_height"], 4129396)

    def test_unchanged_height_does_not_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()

        # The dashboard restarted: its in-memory timer reset, so the condition
        # reads "ok" again -- but the node has not moved a single block.
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, [], "posted a recovery at an unchanged height")
        self.assertTrue(bucket["fleet/node-a"]["alerting"], "alert should stay latched")
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 4129396)

    def test_repeated_timer_resets_never_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        # Five dashboard restarts, as happened on 2026-08-03.
        for cycle in range(5):
            watchdog.update_alert_state(
                bucket, "fleet/node-a", "ok", 2000.0 + cycle, 0.0,
                "STALLED", "RECOVERED", 2000.0 + cycle, False, make_args(), 4129396,
            )
        self.assertEqual(self.posted, [], "stall oscillated recovered/stalled")

    def test_forward_progress_clears_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), 4129397,
        )
        self.assertEqual(self.posted, ["RECOVERED"])
        self.assertFalse(bucket["fleet/node-a"]["alerting"])

    def test_same_height_tip_replacement_clears_the_stall(self):
        bucket = {}
        self.fire_stall(bucket, height=100, block_hash="aa")
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 100, "bb",
        )

        self.assertEqual(self.posted, ["RECOVERED"])
        self.assertEqual(
            bucket["fleet/node-a"],
            {"condition": "ok", "alerting": False},
        )

    def test_tip_rollback_clears_the_stall_when_the_hash_changed(self):
        bucket = {}
        self.fire_stall(bucket, height=100, block_hash="aa")
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 99, "99",
        )

        self.assertEqual(self.posted, ["RECOVERED"])

    def test_unchanged_tip_hash_does_not_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket, height=100, block_hash="aa")
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 100, "aa",
        )

        self.assertEqual(self.posted, [])
        self.assertTrue(bucket["fleet/node-a"]["alerting"])

    def test_missing_height_does_not_clear_the_stall(self):
        # The "starting" grace window reports ok with no usable height.
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), None,
        )
        self.assertEqual(self.posted, [])
        self.assertTrue(bucket["fleet/node-a"]["alerting"])

    def test_height_going_backwards_does_not_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), 4000000,
        )
        self.assertEqual(self.posted, [])

    def test_a_node_resynced_from_a_lower_tip_can_still_recover(self):
        # Wiping and resyncing is a normal fix for a stuck node. Anchored at the
        # pre-stall tip the alert would stay latched for the whole resync and no
        # recovery would ever post, so the anchor follows the node down.
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), 5_000,
        )
        self.assertEqual(self.posted, [], "a lower tip is not forward progress")
        self.assertTrue(bucket["fleet/node-a"]["alerting"])
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 5_000)

        # It is now climbing again, so the recovery posts on the next sample.
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3600.0, 0.0,
            "STALLED", "RECOVERED", 3600.0, False, make_args(), 5_100,
        )
        self.assertEqual(self.posted, ["RECOVERED"])
        self.assertFalse(bucket["fleet/node-a"]["alerting"])

    def test_alert_height_is_backfilled_when_it_was_unknown_at_fire_time(self):
        # A row can alert without a usable height. With no anchor the latch has
        # nothing to compare against and the first reset timer clears it.
        bucket = {}
        self.fire_stall(bucket, height=None)
        self.assertEqual(len(self.posted), 1)
        self.assertIsNone(bucket["fleet/node-a"].get("alert_height"))
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "stalled", 300.0, 600.0,
            "STALLED", "RECOVERED", 1100.0, False, make_args(), 4129396,
        )
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 4129396)

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, [], "cleared at an unchanged height")
        self.assertTrue(bucket["fleet/node-a"]["alerting"])

    def test_down_alerts_still_recover_without_height_progress(self):
        # Only stalls are height-gated; a restarted node legitimately recovers
        # at whatever height it comes back on.
        bucket = {}
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "down", 300.0, 600.0,
            "DOWN", "RECOVERED", 1000.0, False, make_args(), None,
        )
        self.assertEqual(self.posted, ["DOWN"])
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "DOWN", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, ["RECOVERED"])

    def test_stall_alert_height_survives_intermediate_cycles(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        # Still stalled on the next poll: the anchor must not be lost.
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "stalled", 300.0, 600.0,
            "STALLED", "RECOVERED", 1100.0, False, make_args(), 4129396,
        )
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 4129396)
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, [])


class FleetIntervalTests(unittest.TestCase):
    """Fleet agreement replaces duplicate node stalls with one chain interval."""

    def setUp(self):
        self.posted: list[str] = []
        self._real_post = watchdog.post_slack
        self._real_fetch = watchdog.fetch_json
        self._real_time = watchdog.time.time
        watchdog.post_slack = lambda text, args: (self.posted.append(text), True)[1]
        self.fleet = watchdog.Fleet(
            name="mainnet",
            url="http://dashboard/data",
            dashboard_url="http://dashboard/",
            target_block_seconds=75.0,
            block_explorer_url="https://cipherscan.app/block",
        )
        self.subject = watchdog.Watchdog([self.fleet], make_args())

    def tearDown(self):
        watchdog.post_slack = self._real_post
        watchdog.fetch_json = self._real_fetch
        watchdog.time.time = self._real_time

    @staticmethod
    def row(
        name: str,
        client_name: str,
        *,
        height: int = 100,
        block_hash: str = "aa",
        stalled_for: float = 700.0,
        health: str = "stale",
        block_time: int | None = 10_000,
        previous_block_time: int | None = 9_925,
        ancestor_hashes=None,
        previous_hash: str | None = None,
        grandparent_hash: str | None = None,
        last_seen_at: float = 995.0,
    ):
        ancestors = dict(ancestor_hashes or {})
        return {
            "name": name,
            "client_name": client_name,
            "height": height,
            "block_hash": block_hash,
            "seconds_since_advanced": stalled_for,
            "health": health,
            "block_time": block_time,
            "previous_block_time": previous_block_time,
            "ancestor_hashes": ancestors,
            "previous_hash": (
                previous_hash if previous_hash is not None else ancestors.get("1", "")
            ),
            "grandparent_hash": (
                grandparent_hash
                if grandparent_hash is not None
                else ancestors.get("2", "")
            ),
            "last_seen_at": last_seen_at,
        }

    @staticmethod
    def snapshot(
        rows,
        *,
        majority_height: int = 100,
        majority_hash: str = "aa",
        recent_reorgs=None,
        generated_at: float = 1_000.0,
        last_poll: float = 995.0,
    ):
        return {
            "generated_at": generated_at,
            "last_poll": last_poll,
            "chain": {
                "majority_height": majority_height,
                "majority_hash": majority_hash,
                "recent_reorgs": list(recent_reorgs or []),
            },
            "rows": rows,
        }

    def agreed_snapshot(
        self,
        *,
        height: int = 100,
        block_hash: str = "aa",
        stalled_for: float = 700.0,
        health: str = "stale",
        block_time: int | None = 10_000,
        previous_block_time: int | None = 9_925,
        ancestor_hashes=None,
        previous_hash: str | None = None,
        grandparent_hash: str | None = None,
        recent_reorgs=None,
    ):
        common = {
            "height": height,
            "block_hash": block_hash,
            "stalled_for": stalled_for,
            "health": health,
            "block_time": block_time,
            "previous_block_time": previous_block_time,
            "ancestor_hashes": ancestor_hashes,
            "previous_hash": previous_hash,
            "grandparent_hash": grandparent_hash,
        }
        return self.snapshot(
            [
                self.row("node-a", "zakurad", **common),
                self.row("node-b", "zcashd", **common),
            ],
            majority_height=height,
            majority_hash=block_hash,
            recent_reorgs=recent_reorgs,
        )

    def drive(self, state, snapshot, now):
        rows = snapshot["rows"]
        consolidated = self.subject.handle_chain(
            state, self.fleet, snapshot, rows, now, False
        )
        for row in rows:
            self.subject.handle_node(
                state,
                self.fleet,
                row,
                now,
                False,
                suppress_stall=row["name"] in consolidated,
            )

    @staticmethod
    def empty_state():
        return {"version": 1, "nodes": {}, "fleets": {}, "chains": {}}

    def test_cross_client_agreement_posts_one_chain_interval_alert(self):
        state = self.empty_state()
        snapshot = self.agreed_snapshot(
            recent_reorgs=[
                {"at": 900.0, "kind": "tip_switch", "demo": False},
                {"at": 950.0, "kind": "tip_switch", "demo": True},
            ],
        )

        self.drive(state, snapshot, now=1_000.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("extended block interval", self.posted[0])
        self.assertIn("Zakura and its pinned zcashd", self.posted[0])
        self.assertIn("not an independent network reference", self.posted[0])
        self.assertIn("stays unclassified", self.posted[0])
        self.assertIn("9.3× the 75s target", self.posted[0])
        self.assertIn("orphan/reorg observations during interval: 1", self.posted[0])
        self.assertNotIn("`node-a` stalled", self.posted[0])
        self.assertEqual(state["chains"]["mainnet"]["anchor_height"], 100)
        self.assertEqual(state["chains"]["mainnet"]["anchor_hash"], "aa")
        self.assertFalse(state["nodes"]["mainnet/node-a"]["alerting"])
        self.assertFalse(state["nodes"]["mainnet/node-b"]["alerting"])

        self.drive(state, snapshot, now=1_060.0)
        self.assertEqual(len(self.posted), 1)

    def test_consolidated_alert_retires_an_existing_duplicate_stall(self):
        state = self.empty_state()
        state["nodes"]["mainnet/node-a"] = {
            "condition": "stalled",
            "alerting": True,
            "alert_height": 100,
        }
        snapshot = self.agreed_snapshot()

        self.drive(state, snapshot, now=1_000.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("extended block interval", self.posted[0])
        self.assertEqual(
            state["nodes"]["mainnet/node-a"],
            {"condition": "ok", "alerting": False},
        )

    def test_next_agreed_height_confirms_the_observed_interval_once(self):
        state = self.empty_state()
        stalled = self.agreed_snapshot()
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()
        state["nodes"]["mainnet/node-a"] = {
            "condition": "stalled",
            "alerting": True,
            "alert_height": 100,
        }

        advanced = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=10_831,
            previous_block_time=10_000,
            ancestor_hashes={"1": "aa"},
            recent_reorgs=[
                {"at": 900.0, "kind": "tip_switch", "demo": False},
                {"at": 1_100.0, "kind": "tip_switch", "demo": False},
            ],
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("long canonical block interval confirmed", self.posted[0])
        self.assertIn("observed interval: 13m 51s", self.posted[0])
        self.assertIn("agreed height 100 → 101", self.posted[0])
        self.assertIn("canonical header interval 100→101: 13m 51s", self.posted[0])
        self.assertIn("confirms a long canonical block interval", self.posted[0])
        self.assertIn("fleet delay is not needed", self.posted[0])
        self.assertIn(
            "<https://cipherscan.app/block/100|block 100> → "
            "<https://cipherscan.app/block/101|block 101>",
            self.posted[0],
        )
        self.assertIn("orphan/reorg observations during interval: 2", self.posted[0])
        self.assertNotIn("mainnet", state["chains"])
        self.assertEqual(
            state["nodes"]["mainnet/node-a"],
            {"condition": "ok", "alerting": False},
        )

        self.drive(state, advanced, now=1_191.0)
        self.assertEqual(len(self.posted), 1)

    def test_short_successor_header_interval_reports_a_shared_fleet_delay(self):
        state = self.empty_state()
        stalled = self.agreed_snapshot()
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        advanced = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=10_090,
            previous_block_time=10_000,
            ancestor_hashes={"1": "aa"},
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("fleet delay suspected", self.posted[0])
        self.assertIn("canonical header interval 100→101: 1m 30s", self.posted[0])
        self.assertIn("did not observe the canonical successor promptly", self.posted[0])
        self.assertIn("cipherscan.app/block/100", self.posted[0])
        self.assertEqual(watchdog.format_header_interval(-30), "-30s")

    def test_missing_successor_header_interval_retries_before_unclassified(self):
        state = self.empty_state()
        stalled = self.agreed_snapshot()
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        advanced = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=None,
            previous_block_time=None,
            ancestor_hashes={"1": "aa"},
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(self.posted, [])
        self.assertEqual(state["chains"]["mainnet"]["evidence_deadline"], 1_251.0)

        self.drive(state, advanced, now=1_191.0)
        self.assertEqual(self.posted, [])
        self.drive(state, advanced, now=1_251.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("fleet progress resumed", self.posted[0])
        self.assertIn("event remains unclassified", self.posted[0])
        self.assertNotIn("mainnet", state["chains"])

    def test_one_extra_block_still_classifies_the_alerted_successor(self):
        state = self.empty_state()
        stalled = self.agreed_snapshot()
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        advanced = self.agreed_snapshot(
            height=102,
            block_hash="cc",
            stalled_for=5.0,
            health="healthy",
            block_time=10_900,
            previous_block_time=10_831,
            ancestor_hashes={"2": "aa"},
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("long canonical block interval confirmed", self.posted[0])
        self.assertIn("agreed height 100 → 102", self.posted[0])
        self.assertIn("canonical header interval 100→101: 13m 51s", self.posted[0])

    def test_extra_block_on_a_different_branch_reports_branch_change(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        advanced = self.agreed_snapshot(
            height=102,
            block_hash="cc",
            stalled_for=5.0,
            health="healthy",
            block_time=10_900,
            previous_block_time=10_831,
            ancestor_hashes={"2": "different-height-100-hash"},
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("canonical branch changed", self.posted[0])
        self.assertIn("is not the agreed tip's canonical ancestor", self.posted[0])
        self.assertNotIn("fleet delay suspected", self.posted[0])

    def test_orphan_rollback_resolves_without_reanchoring(self):
        state = self.empty_state()
        stalled = self.agreed_snapshot()
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        rolled_back = self.agreed_snapshot(
            height=99,
            block_hash="99",
            block_time=9_900,
            previous_block_time=9_825,
            recent_reorgs=[
                {"at": 1_020.0, "kind": "reorg_height_drop", "demo": False}
            ],
        )
        self.drive(state, rolled_back, now=1_020.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("canonical branch changed", self.posted[0])
        self.assertIn("rolled back from alerted height 100 to 99", self.posted[0])
        self.assertIn("orphan/reorg observations during interval: 1", self.posted[0])
        self.assertNotIn("mainnet", state["chains"])

    def test_timer_reset_at_the_same_height_does_not_confirm_the_interval(self):
        state = self.empty_state()
        stalled = self.agreed_snapshot()
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        reset = self.agreed_snapshot(stalled_for=5.0, health="healthy")
        self.drive(state, reset, now=1_010.0)

        self.assertEqual(self.posted, [])
        self.assertEqual(state["chains"]["mainnet"]["anchor_hash"], "aa")

    def test_same_height_replacement_resolves_against_the_original_anchor(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        replacement = self.agreed_snapshot(
            block_hash="bb",
            block_time=10_050,
            previous_block_time=9_925,
            ancestor_hashes={"1": "parent"},
            recent_reorgs=[{"at": 1_020.0, "kind": "tip_switch", "demo": False}],
        )
        self.drive(state, replacement, now=1_020.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("canonical branch changed", self.posted[0])
        self.assertIn("`aa` at height 100 was replaced by `bb`", self.posted[0])
        self.assertNotIn("fleet delay suspected", self.posted[0])
        self.assertNotIn("mainnet", state["chains"])

        successor = self.agreed_snapshot(
            height=101,
            block_hash="cc",
            stalled_for=5.0,
            health="healthy",
            block_time=10_125,
            previous_block_time=10_050,
            ancestor_hashes={"1": "bb"},
        )
        self.drive(state, successor, now=1_080.0)
        self.assertEqual(len(self.posted), 1)

    def test_skipped_replacement_cannot_be_called_a_fleet_delay(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        successor = self.agreed_snapshot(
            height=101,
            block_hash="cc",
            stalled_for=5.0,
            health="healthy",
            block_time=10_080,
            previous_block_time=10_050,
            ancestor_hashes={"1": "bb"},
        )
        self.drive(state, successor, now=1_060.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("canonical branch changed", self.posted[0])
        self.assertIn("height 100 is now `bb`", self.posted[0])
        self.assertNotIn("fleet delay suspected", self.posted[0])
        self.assertIn("orphan/reorg observations during interval: 0", self.posted[0])
        self.assertIn(
            "<https://cipherscan.app/block/100|block 100> → "
            "<https://cipherscan.app/block/101|block 101>",
            self.posted[0],
        )

    def test_hash_linked_parent_wins_over_racy_height_lookup(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        successor = self.agreed_snapshot(
            height=101,
            block_hash="cc",
            stalled_for=5.0,
            health="healthy",
            block_time=10_090,
            previous_block_time=10_000,
            ancestor_hashes={"1": "aa"},
            previous_hash="bb",
        )
        self.drive(state, successor, now=1_060.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("canonical branch changed", self.posted[0])
        self.assertIn("height 100 is now `bb`", self.posted[0])
        self.assertNotIn("fleet delay suspected", self.posted[0])

    def test_two_matching_header_reports_ignore_one_missing_member(self):
        state = self.empty_state()
        stalled = self.snapshot(
            [
                self.row("node-a", "zakurad"),
                self.row("node-b", "zcashd"),
                self.row("node-c", "zakurad"),
            ]
        )
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        common = {
            "height": 101,
            "block_hash": "bb",
            "stalled_for": 5.0,
            "health": "healthy",
            "block_time": 10_831,
            "previous_block_time": 10_000,
            "ancestor_hashes": {"1": "aa"},
        }
        advanced = self.snapshot(
            [
                self.row("node-a", "zakurad", **common),
                self.row("node-b", "zcashd", **common),
                self.row(
                    "node-c",
                    "zakurad",
                    **{
                        **common,
                        "block_time": None,
                        "previous_block_time": None,
                        "ancestor_hashes": {},
                    },
                ),
            ],
            majority_height=101,
            majority_hash="bb",
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("long canonical block interval confirmed", self.posted[0])

    def test_conflicting_header_reports_retry_instead_of_classifying(self):
        state = self.empty_state()
        stalled = self.snapshot(
            [
                self.row("node-a", "zakurad"),
                self.row("node-b", "zcashd"),
                self.row("node-c", "zakurad"),
            ]
        )
        self.drive(state, stalled, now=1_000.0)
        self.posted.clear()

        common = {
            "height": 101,
            "block_hash": "bb",
            "stalled_for": 5.0,
            "health": "healthy",
            "previous_block_time": 10_000,
            "ancestor_hashes": {"1": "aa"},
        }
        advanced = self.snapshot(
            [
                self.row("node-a", "zakurad", block_time=10_831, **common),
                self.row("node-b", "zcashd", block_time=10_831, **common),
                self.row("node-c", "zakurad", block_time=10_830, **common),
            ],
            majority_height=101,
            majority_hash="bb",
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(self.posted, [])
        self.assertEqual(state["chains"]["mainnet"]["evidence_deadline"], 1_251.0)

    def test_missing_evidence_can_recover_before_the_deadline(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        missing = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=None,
            previous_block_time=None,
            ancestor_hashes={"1": "aa"},
        )
        self.drive(state, missing, now=1_131.0)
        self.assertEqual(self.posted, [])

        recovered = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=10_831,
            previous_block_time=10_000,
            ancestor_hashes={"1": "aa"},
        )
        self.drive(state, recovered, now=1_191.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("long canonical block interval confirmed", self.posted[0])
        self.assertNotIn("mainnet", state["chains"])

    def test_return_to_anchor_resets_the_evidence_deadline(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()
        missing = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=None,
            previous_block_time=None,
            ancestor_hashes={"1": "aa"},
        )
        self.drive(state, missing, now=1_131.0)
        self.assertIn("evidence_deadline", state["chains"]["mainnet"])

        self.drive(
            state,
            self.agreed_snapshot(stalled_for=5.0, health="healthy"),
            now=1_191.0,
        )

        self.assertNotIn("evidence_deadline", state["chains"]["mainnet"])

    def test_advancing_beyond_header_window_resolves_unclassified(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        advanced = self.agreed_snapshot(
            height=103,
            block_hash="dd",
            stalled_for=5.0,
            health="healthy",
            block_time=11_000,
            previous_block_time=10_900,
            ancestor_hashes={"3": "aa"},
        )
        self.drive(state, advanced, now=1_131.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("fleet progress resumed", self.posted[0])
        self.assertIn("beyond the dashboard's exact H+1 timestamp window", self.posted[0])
        self.assertIn("event remains unclassified", self.posted[0])

    def test_stale_dashboard_routes_to_one_telemetry_alert(self):
        state = self.empty_state()
        stale = self.agreed_snapshot()
        stale["generated_at"] = 1_000.0
        stale["last_poll"] = 600.0
        watchdog.fetch_json = lambda url, timeout: stale
        watchdog.time.time = lambda: 1_000.0

        self.subject.run_once(state)
        self.assertEqual(self.posted, [])
        self.assertEqual(state["fleets"]["mainnet"]["condition"], "unreachable")
        self.assertEqual(state["nodes"], {})
        self.assertEqual(state["chains"], {})

        watchdog.time.time = lambda: 1_600.0
        self.subject.run_once(state)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("dashboard telemetry unavailable", self.posted[0])
        self.assertIn("last completed poll is 400.0s behind", self.posted[0])

    def test_completed_slow_collection_is_not_treated_as_stale(self):
        state = self.empty_state()
        snapshot = self.agreed_snapshot()
        snapshot["generated_at"] = 1_000.0
        snapshot["last_poll"] = 995.0
        snapshot["rows"][0]["last_seen_at"] = 936.0
        watchdog.fetch_json = lambda url, timeout: snapshot
        watchdog.time.time = lambda: 1_000.0

        self.subject.run_once(state)

        self.assertEqual(state["fleets"]["mainnet"]["condition"], "ok")
        self.assertEqual(len(self.posted), 1)
        self.assertIn("extended block interval", self.posted[0])

    def test_interval_classification_bands_are_conservative(self):
        expected_titles = {
            -1: "interval cause inconclusive",
            150: "fleet delay suspected",
            151: "interval cause inconclusive",
            569: "interval cause inconclusive",
            570: "long canonical block interval consistent with alert",
            599: "long canonical block interval consistent with alert",
            600: "long canonical block interval confirmed",
        }
        for interval, expected_title in expected_titles.items():
            with self.subTest(interval=interval):
                state = self.empty_state()
                self.drive(state, self.agreed_snapshot(), now=1_000.0)
                self.posted.clear()
                advanced = self.agreed_snapshot(
                    height=101,
                    block_hash="bb",
                    stalled_for=5.0,
                    health="healthy",
                    block_time=10_000 + interval,
                    previous_block_time=10_000,
                    ancestor_hashes={"1": "aa"},
                )
                self.drive(state, advanced, now=1_131.0)
                self.assertEqual(len(self.posted), 1)
                self.assertIn(expected_title, self.posted[0])

    def test_observed_age_must_reach_the_threshold(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(stalled_for=599.0), now=1_000.0)
        self.assertEqual(self.posted, [])
        self.assertNotIn("mainnet", state["chains"])

        self.drive(state, self.agreed_snapshot(stalled_for=600.0), now=1_001.0)
        self.assertEqual(len(self.posted), 1)
        self.assertIn("extended block interval", self.posted[0])
        self.assertEqual(state["chains"]["mainnet"]["anchor_hash"], "aa")

    def test_failed_resolution_delivery_preserves_the_exact_result(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()

        attempts: list[str] = []
        results = iter((False, True))
        watchdog.post_slack = lambda text, args: (
            attempts.append(text),
            next(results),
        )[1]
        replacement = self.agreed_snapshot(block_hash="bb")
        self.drive(state, replacement, now=1_020.0)

        incident = state["chains"]["mainnet"]
        self.assertEqual(incident["anchor_hash"], "aa")
        self.assertIn("canonical branch changed", incident["pending_resolution_text"])

        successor = self.agreed_snapshot(
            height=103,
            block_hash="dd",
            stalled_for=5.0,
            health="healthy",
            ancestor_hashes={"3": "bb"},
        )
        self.drive(state, successor, now=1_080.0)

        self.assertEqual(attempts[0], attempts[1])
        self.assertNotIn("mainnet", state["chains"])

    def test_pending_evidence_survives_state_reload(self):
        state = self.empty_state()
        self.drive(state, self.agreed_snapshot(), now=1_000.0)
        self.posted.clear()
        missing = self.agreed_snapshot(
            height=101,
            block_hash="bb",
            stalled_for=5.0,
            health="healthy",
            block_time=None,
            previous_block_time=None,
            ancestor_hashes={"1": "aa"},
        )
        self.drive(state, missing, now=1_131.0)

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            watchdog.save_state(state_path, state)
            restored = watchdog.load_state(state_path)

        incident = restored["chains"]["mainnet"]
        self.assertEqual(incident["anchor_hash"], "aa")
        self.assertEqual(incident["evidence_deadline"], 1_251.0)

    def test_mixed_stall_ages_do_not_hide_an_individual_stall(self):
        state = self.empty_state()
        snapshot = self.snapshot(
            [
                self.row("node-a", "zakurad"),
                self.row(
                    "node-b",
                    "zcashd",
                    stalled_for=5.0,
                    health="healthy",
                ),
            ]
        )

        self.drive(state, snapshot, now=1_000.0)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("`node-a` stalled", self.posted[0])
        self.assertNotIn("extended block interval", self.posted[0])

    def test_behind_node_still_alerts_beside_the_majority_interval(self):
        state = self.empty_state()
        snapshot = self.snapshot(
            [
                self.row("node-a", "zakurad"),
                self.row("node-b", "zcashd"),
                self.row("node-c", "zakurad", height=99, block_hash="99"),
            ]
        )

        self.drive(state, snapshot, now=1_000.0)

        self.assertEqual(len(self.posted), 2)
        self.assertEqual(
            sum("extended block interval" in text for text in self.posted), 1
        )
        self.assertEqual(sum("`node-c` stalled" in text for text in self.posted), 1)
        self.assertEqual(sum("`node-a` stalled" in text for text in self.posted), 0)
        self.assertEqual(sum("`node-b` stalled" in text for text in self.posted), 0)

    def test_ahead_reference_keeps_old_majority_node_stall_alerts(self):
        state = self.empty_state()
        snapshot = self.snapshot(
            [
                self.row("node-a", "zakurad"),
                self.row("node-b", "zakurad"),
                self.row(
                    "node-c",
                    "zcashd",
                    height=101,
                    block_hash="bb",
                    stalled_for=5.0,
                    health="healthy",
                ),
            ]
        )

        self.drive(state, snapshot, now=1_000.0)

        self.assertEqual(len(self.posted), 2)
        self.assertFalse(any("extended block interval" in text for text in self.posted))
        self.assertTrue(any("`node-a` stalled" in text for text in self.posted))
        self.assertTrue(any("`node-b` stalled" in text for text in self.posted))


if __name__ == "__main__":
    unittest.main()
