import os
from datetime import datetime


class StripeClient:
    def __init__(self):
        import stripe as _stripe
        _stripe.api_key = os.environ["STRIPE_API_KEY"]
        self._s = _stripe

    def _ts(self, dt: datetime | None) -> int | None:
        return int(dt.timestamp()) if dt else None

    def _collect(self, resource, **kwargs) -> list:
        items = []
        for item in resource.list(limit=100, **kwargs).auto_paging_iter():
            items.append(item)
        return items

    def get_balance_transactions(self, created_after: datetime | None = None) -> list:
        params = {}
        if created_after:
            params["created"] = {"gte": self._ts(created_after)}
        return self._collect(self._s.BalanceTransaction, **params)

    def get_payouts(self, created_after: datetime | None = None) -> list:
        params = {}
        if created_after:
            params["created"] = {"gte": self._ts(created_after)}
        return self._collect(self._s.Payout, **params)

    def get_charges(self, created_after: datetime | None = None) -> list:
        params = {}
        if created_after:
            params["created"] = {"gte": self._ts(created_after)}
        return self._collect(self._s.Charge, **params)

    def get_refunds(self, created_after: datetime | None = None) -> list:
        params = {}
        if created_after:
            params["created"] = {"gte": self._ts(created_after)}
        return self._collect(self._s.Refund, **params)
