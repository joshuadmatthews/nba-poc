"""Unit tests for the journey-scorer's PURE scoring functions -- no kafka, no live stack.

The scorer is the deterministic, Databricks-free policy that drives the local flywheel: its argmax must walk each
member FORWARD through their funnel (fresh actions first, completed/in-flight ones sink). These tests pin that
contract -- the score bands + their ordering + determinism -- so a change that would (e.g.) re-pick a hard-completed
action or rank an in-flight action above a fresh one fails the build instead of silently breaking journeys.

Run:  python -m unittest -v   (from nba/services/nba-journey-scorer/)
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from scorer import stable01, score_action  # noqa: E402  (kafka import is lazy in main(), so this is import-safe)


def ca(action="action_hra", channel="email", **kw):
    d = {"actionId": action, "channel": channel, "eligible": True}
    d.update(kw)
    return d


class TestStable01(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(stable01("m1", "a", "email"), stable01("m1", "a", "email"))

    def test_in_unit_interval(self):
        for parts in [("m1", "a", "email"), ("m2", "b", "sms"), ("x", "y", "z")]:
            v = stable01(*parts)
            self.assertGreaterEqual(v, 0.0)
            self.assertLess(v, 1.0)

    def test_varies_by_input(self):
        vals = {stable01("m", a, c) for a in ("a", "b", "c") for c in ("email", "sms", "push")}
        self.assertGreater(len(vals), 1)   # different (action,channel) -> different offsets (varied journey)


class TestScoreAction(unittest.TestCase):
    # --- each state lands in its own non-overlapping band ---
    def test_fresh_top_band(self):
        s = score_action("m1", ca())
        self.assertGreaterEqual(s, 10.0)
        self.assertLess(s, 20.0)

    def test_hard_completed_sinks_lowest(self):
        self.assertLess(score_action("m1", ca(hardCompleted=True)), -99.0)

    def test_in_flight_via_active(self):
        s = score_action("m1", ca(active=True))
        self.assertTrue(-50.0 <= s < -49.0)

    def test_in_flight_via_workflow_state(self):
        for st in ("CREATED", "IN_PROCESS", "PRESENTED", "SUPPRESSING"):
            s = score_action("m1", ca(workflowState=st))
            self.assertTrue(-50.0 <= s < -49.0, st)

    def test_soft_completed_band(self):
        s = score_action("m1", ca(softCompleted=True))
        self.assertTrue(-10.0 <= s < -5.0)

    def test_negative_band(self):
        for st in ("DECLINED", "FAILED", "EXPIRED"):
            s = score_action("m1", ca(workflowState=st))
            self.assertTrue(-20.0 <= s < -17.0, st)

    # --- the contract the router relies on ---
    def test_router_argmax_walks_forward(self):
        fresh = score_action("m1", ca())
        soft = score_action("m1", ca(softCompleted=True))
        neg = score_action("m1", ca(workflowState="DECLINED"))
        infl = score_action("m1", ca(active=True))
        hard = score_action("m1", ca(hardCompleted=True))
        self.assertGreater(fresh, soft)   # un-attempted picked before engaged
        self.assertGreater(soft, neg)     # engaged before declined/expired
        self.assertGreater(neg, infl)     # declined before something still in flight
        self.assertGreater(infl, hard)    # in-flight before goal-reached (never re-pick the goal)

    def test_hard_completed_takes_priority_over_active(self):
        # hardCompleted is checked FIRST -> a goal-reached action sinks lowest even if also flagged active.
        self.assertLess(score_action("m1", ca(hardCompleted=True, active=True)), -99.0)

    def test_deterministic(self):
        c = ca()
        self.assertEqual(score_action("m1", c), score_action("m1", c))

    def test_varied_journey_across_members(self):
        # different members get different FRESH offsets for the same action -> they traverse in different orders.
        scores = {score_action(m, ca()) for m in ("m1", "m2", "m3", "m4", "m5")}
        self.assertGreater(len(scores), 1)


if __name__ == "__main__":
    unittest.main()
