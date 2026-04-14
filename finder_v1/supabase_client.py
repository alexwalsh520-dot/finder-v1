from __future__ import annotations

import json
import mimetypes
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class SupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.key = service_role_key
        self.base = f"{self.url}/rest/v1"
        self.storage_base = f"{self.url}/storage/v1"

    def enabled(self) -> bool:
        return bool(self.url and self.key)

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def _request(self, method: str, path: str, body: Optional[Dict] = None, extra_headers: Optional[Dict[str, str]] = None):
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        last_error: Exception | None = None
        for attempt in range(3):
            req = urllib.request.Request(f"{self.base}{path}", data=data, headers=self._headers(extra_headers), method=method)
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    raw = response.read().decode()
                    return response.headers, json.loads(raw) if raw else []
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in {408, 425, 429, 500, 502, 503, 504} or attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
            except Exception as exc:
                last_error = exc
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def fetch_rows(self, table: str, select: str = "*", filters: Optional[List[str]] = None, limit: Optional[int] = None) -> List[Dict]:
        clauses = [f"select={urllib.parse.quote(select, safe='*,()')}"]
        for flt in filters or []:
            clauses.append(flt)
        if limit is not None:
            clauses.append(f"limit={limit}")
        path = f"/{table}?{'&'.join(clauses)}"
        _, rows = self._request("GET", path)
        return rows

    def fetch_existing_handles(self, handles: List[str]) -> Dict[str, Dict]:
        if not handles:
            return {}
        query = ",".join(handles)
        paths = [
            (
                "/leads?"
                "select=instagram_handle,email,status,email_source,sent_to_smartlead,smartlead_campaign_id,smartlead_sent_at"
                f"&instagram_handle=in.({urllib.parse.quote(query, safe=',()')})"
            ),
            (
                "/leads?"
                "select=instagram_handle,email,status,email_source"
                f"&instagram_handle=in.({urllib.parse.quote(query, safe=',()')})"
            ),
        ]
        rows = []
        for path in paths:
            try:
                _, rows = self._request("GET", path)
                break
            except Exception:
                rows = []
        return {row["instagram_handle"]: row for row in rows}

    def fetch_reviewable_leads(
        self,
        *,
        limit: int,
        review_status: str | None = None,
        historical: bool = False,
    ) -> List[Dict]:
        filters = [
            "select=id,instagram_handle,email,review_status,sent_to_smartlead,smartlead_campaign_id,smartlead_sent_at,export_batch_id",
            "email=not.is.null",
            "smartlead_sent_at=is.null",
        ]
        if historical:
            filters.append("sent_to_smartlead=is.false")
            filters.append("order=batch_date.asc.nullslast,created_at.asc")
        else:
            filters.append("review_status=eq.exported_pending_confirmation")
            filters.append("order=created_at.desc")
        if review_status:
            filters.append(f"review_status=eq.{urllib.parse.quote(review_status)}")
        filters.append(f"limit={limit}")
        _, rows = self._request("GET", f"/leads?{'&'.join(filters)}")
        return rows if isinstance(rows, list) else []

    def update_lead_by_id(self, lead_id: str, payload: Dict) -> None:
        self._request(
            "PATCH",
            f"/leads?id=eq.{urllib.parse.quote(lead_id)}",
            body=payload,
            extra_headers={"Prefer": "return=representation"},
        )

    def insert_lead_review_event(self, payload: Dict) -> None:
        self._request(
            "POST",
            "/lead_review_events",
            body=payload,
            extra_headers={"Prefer": "return=representation"},
        )

    def probe_leads_schema(self) -> Dict[str, object]:
        if not self.enabled():
            return {
                "enabled": False,
                "reachable": False,
                "smartlead_tracking_fields": False,
                "review_app_fields": False,
                "error": "Supabase credentials are not configured.",
            }
        basic_path = "/leads?select=instagram_handle&limit=1"
        smartlead_path = "/leads?select=instagram_handle,sent_to_smartlead,smartlead_campaign_id,smartlead_sent_at&limit=1"
        review_path = "/leads?select=instagram_handle,review_status,exported_at,export_batch_id&limit=1"
        try:
            self._request("GET", basic_path)
        except Exception as basic_error:
            return {
                "enabled": True,
                "reachable": False,
                "smartlead_tracking_fields": False,
                "review_app_fields": False,
                "error": str(basic_error),
            }

        smartlead_ok = True
        review_ok = True
        errors: list[str] = []
        try:
            self._request("GET", smartlead_path)
        except Exception as exc:
            smartlead_ok = False
            errors.append(f"smartlead_fields: {exc}")

        try:
            self._request("GET", review_path)
        except Exception as exc:
            review_ok = False
            errors.append(f"review_fields: {exc}")

        return {
            "enabled": True,
            "reachable": True,
            "smartlead_tracking_fields": smartlead_ok,
            "review_app_fields": review_ok,
            "error": "; ".join(errors),
        }

    def count_today_emails_for_source(self, credited_date: str, source: str) -> int:
        path = f"/leads?select=id&email=not.is.null&batch_date=eq.{credited_date}&source=eq.{urllib.parse.quote(source)}"
        headers, _ = self._request("GET", path, extra_headers={"Prefer": "count=exact", "Range": "0-0"})
        content_range = headers.get("Content-Range", "")
        if "/" not in content_range:
            return 0
        return int(content_range.split("/")[-1])

    def count_today_emails(self, credited_date: str) -> int:
        path = f"/leads?select=id&email=not.is.null&batch_date=eq.{credited_date}"
        headers, _ = self._request("GET", path, extra_headers={"Prefer": "count=exact", "Range": "0-0"})
        content_range = headers.get("Content-Range", "")
        if "/" not in content_range:
            return 0
        return int(content_range.split("/")[-1])

    def fetch_youtube_only_without_email(self) -> List[Dict]:
        path = "/leads?select=instagram_handle,full_name,follower_count,instagram_url,external_url,youtube_channel,bio,business_category,source_detail,status&status=eq.youtube_only&order=follower_count.desc.nullslast"
        _, rows = self._request("GET", path)
        return rows

    def fetch_no_email_without_youtube(self) -> List[Dict]:
        path = "/leads?select=instagram_handle,full_name,follower_count,instagram_url,external_url,youtube_channel,bio,business_category,source_detail,status&email=is.null&youtube_channel=is.null&order=follower_count.desc.nullslast"
        _, rows = self._request("GET", path)
        return rows

    def upsert_lead(self, payload: Dict) -> None:
        self._request(
            "POST",
            "/leads?on_conflict=instagram_handle",
            body=payload,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )

    def upsert_cron_job(self, payload: Dict) -> None:
        self._request(
            "POST",
            "/cron_jobs?on_conflict=id",
            body=payload,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )

    def insert_agent_event(self, payload: Dict) -> None:
        self._request(
            "POST",
            "/agent_events",
            body=payload,
            extra_headers={"Prefer": "return=representation"},
        )

    def _storage_request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        req = urllib.request.Request(
            f"{self.storage_base}{path}",
            data=body,
            headers=self._headers(headers),
            method=method,
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=60) as response:
                    raw = response.read().decode()
                    return response.headers, json.loads(raw) if raw else {}
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="ignore")
                last_error = RuntimeError(raw or str(exc))
                if exc.code not in {408, 425, 429, 500, 502, 503, 504} or attempt == 2:
                    raise last_error
                time.sleep(1.5 * (attempt + 1))
            except Exception as exc:
                last_error = exc
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def list_storage_buckets(self) -> List[Dict]:
        _, rows = self._storage_request("GET", "/bucket")
        if isinstance(rows, list):
            return rows
        return []

    def ensure_storage_bucket(self, bucket_name: str, public: bool = False) -> bool:
        if not bucket_name:
            return False
        buckets = self.list_storage_buckets()
        if any((row.get("id") or row.get("name")) == bucket_name for row in buckets):
            return True
        body = json.dumps({"id": bucket_name, "name": bucket_name, "public": public}).encode("utf-8")
        try:
            self._storage_request(
                "POST",
                "/bucket",
                body=body,
                headers={"Content-Type": "application/json"},
            )
            return True
        except Exception:
            buckets = self.list_storage_buckets()
            return any((row.get("id") or row.get("name")) == bucket_name for row in buckets)

    def upload_storage_bytes(self, bucket_name: str, object_path: str, payload: bytes, content_type: str) -> Dict:
        _, data = self._storage_request(
            "POST",
            f"/object/{bucket_name}/{urllib.parse.quote(object_path, safe='/')}",
            body=payload,
            headers={
                "Content-Type": content_type,
                "x-upsert": "true",
            },
        )
        return data if isinstance(data, dict) else {}

    def upload_storage_file(self, bucket_name: str, object_path: str, local_path: str) -> Dict:
        path = Path(local_path)
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        return self.upload_storage_bytes(bucket_name, object_path, path.read_bytes(), content_type)

    def probe_storage_bucket(self, bucket_name: str) -> Dict[str, object]:
        if not self.enabled():
            return {
                "enabled": False,
                "reachable": False,
                "bucket_exists": False,
                "error": "Supabase credentials are not configured.",
            }
        if not bucket_name:
            return {
                "enabled": True,
                "reachable": True,
                "bucket_exists": False,
                "error": "FINDER_OUTPUT_BUCKET is not configured.",
            }
        try:
            buckets = self.list_storage_buckets()
            exists = any((row.get("id") or row.get("name")) == bucket_name for row in buckets)
            return {
                "enabled": True,
                "reachable": True,
                "bucket_exists": exists,
                "error": "",
            }
        except Exception as exc:
            return {
                "enabled": True,
                "reachable": False,
                "bucket_exists": False,
                "error": str(exc),
            }

def today_utc_date() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")
