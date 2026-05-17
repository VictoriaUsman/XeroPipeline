import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests


class XeroClient:
    TOKEN_URL = "https://identity.xero.com/connect/token"
    API_BASE = "https://api.xero.com/api.xro/2.0"

    def __init__(self):
        self.client_id = os.environ["XERO_CLIENT_ID"]
        self.client_secret = os.environ["XERO_CLIENT_SECRET"]
        self.tenant_id = os.environ["XERO_TENANT_ID"]
        self._token_file = "/tmp/xero_token.json"
        self._ssm_path = os.environ.get("XERO_TOKEN_SSM_PATH", "/xero-pipeline/refresh-token")
        self._access_token = None
        self._refresh()

    # ── token storage (SSM preferred, local file fallback) ──────────────────

    def _load_token_data(self) -> dict:
        try:
            ssm = boto3.client("ssm", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2"))
            value = ssm.get_parameter(Name=self._ssm_path, WithDecryption=True)["Parameter"]["Value"]
            return json.loads(value)
        except Exception:
            pass
        if os.path.exists(self._token_file):
            with open(self._token_file) as f:
                return json.load(f)
        env_token = os.environ.get("XERO_REFRESH_TOKEN")
        if env_token:
            return {"refresh_token": env_token}
        raise RuntimeError("No Xero refresh token found. Set XERO_REFRESH_TOKEN or run auth setup.")

    def _save_token_data(self, data: dict) -> None:
        try:
            ssm = boto3.client("ssm", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2"))
            ssm.put_parameter(
                Name=self._ssm_path,
                Value=json.dumps(data),
                Type="SecureString",
                Overwrite=True,
            )
        except Exception:
            pass
        with open(self._token_file, "w") as f:
            json.dump(data, f)

    # ── token refresh ────────────────────────────────────────────────────────

    def _refresh(self) -> None:
        token_data = self._load_token_data()
        resp = requests.post(
            self.TOKEN_URL,
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "refresh_token", "refresh_token": token_data["refresh_token"]},
            timeout=30,
        )
        resp.raise_for_status()
        new_token = resp.json()
        self._save_token_data(new_token)
        self._access_token = new_token["access_token"]

    # ── HTTP ─────────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Xero-tenant-id": self.tenant_id,
            "Accept": "application/json",
        }

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        url = f"{self.API_BASE}/{endpoint}"
        for attempt in range(5):
            resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
            if resp.status_code == 429:
                time.sleep(60 * (2 ** attempt))
                continue
            if resp.status_code == 401:
                self._refresh()
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Xero API failed after retries: {endpoint}")

    # ── data fetchers (all paginated) ────────────────────────────────────────

    def _paginate(self, endpoint: str, key: str, modified_after: str | None = None) -> list:
        results = []
        page = 1
        while True:
            params: dict = {"page": page}
            if modified_after:
                params["ModifiedAfter"] = modified_after
            data = self._get(endpoint, params=params)
            records = data.get(key, [])
            results.extend(records)
            if len(records) < 100:
                break
            page += 1
            time.sleep(0.5)  # stay under 60 calls/min
        return results

    def get_bank_transactions(self, modified_after: str | None = None) -> list:
        return self._paginate("BankTransactions", "BankTransactions", modified_after)

    def get_invoices(self, modified_after: str | None = None) -> list:
        return self._paginate("Invoices", "Invoices", modified_after)

    def get_accounts(self) -> list:
        return self._get("Accounts").get("Accounts", [])

    def get_contacts(self, modified_after: str | None = None) -> list:
        return self._paginate("Contacts", "Contacts", modified_after)

    def get_payments(self, modified_after: str | None = None) -> list:
        return self._paginate("Payments", "Payments", modified_after)
