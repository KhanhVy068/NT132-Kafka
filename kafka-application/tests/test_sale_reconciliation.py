from copy import deepcopy
import unittest
from load_testing.verify_sale import is_settled


class ReconciliationTests(unittest.TestCase):
    def snapshot(self):
        return {
            "processed": 10, "reserved": 5, "rejected": 5, "released": 0,
            "foreign_orders": 0, "pending_outbox": 0, "errors": 0,
            "inventory": {"stock": 0, "version": 5},
            "projection": {"stock_after": 0, "version": 5},
            "payments": {"total": 5, "success": 5, "failure": 0},
            "notifications": {"total": 5, "success": 5, "failure": 0},
        }

    def check(self, value, mode="success"):
        return is_settled(value, count=10, initial_stock=5, quantity=1,
                          mode=mode, initial_errors=0)

    def test_success_and_incomplete_backlogs(self):
        self.assertTrue(self.check(self.snapshot()))
        for key, value in [("pending_outbox", 1), ("processed", 9),
                           ("errors", 1), ("foreign_orders", 1)]:
            snapshot = self.snapshot()
            snapshot[key] = value
            self.assertFalse(self.check(snapshot))
        stale = self.snapshot()
        stale["projection"]["version"] = 4
        self.assertFalse(self.check(stale))
        missing = self.snapshot()
        missing["notifications"]["total"] = 4
        self.assertFalse(self.check(missing))

    def test_release_accounting(self):
        snapshot = self.snapshot()
        snapshot.update(reserved=0, released=5,
                        inventory={"stock": 5, "version": 10},
                        projection={"stock_after": 5, "version": 10},
                        payments={"total": 5, "success": 0, "failure": 5},
                        notifications={"total": 5, "success": 0, "failure": 5})
        self.assertTrue(self.check(snapshot, mode="failure"))
        snapshot["inventory"]["stock"] = 6
        self.assertFalse(self.check(snapshot, mode="failure"))
