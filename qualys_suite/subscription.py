"""
subscription.py - Qualys Subscription, Alerts & CMDB Sync Module
Covers: Subscription Info, Alert Schedules, CMDB Sync, Lookup Tables
API: /api/2.0/fo/subscription/, /api/2.0/fo/alert/
"""
from .base import QualysSession
from typing import Optional


class SubscriptionModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # SUBSCRIPTION INFO
    # ------------------------------------------------------------------
    def get_subscription_info(self) -> str:
        """Retrieve subscription details, asset count, modules."""
        return self.s._get("/api/2.0/fo/subscription/", params={"action": "get"}).text

    # ------------------------------------------------------------------
    # ALERTS
    # ------------------------------------------------------------------
    def list_alerts(self) -> str:
        return self.s._get("/api/2.0/fo/alert/", params={"action": "list"}).text

    def create_alert(self, title: str, schedule_daily: bool = False,
                     notification_list: Optional[str] = None,
                     trigger: Optional[str] = None) -> str:
        """
        trigger: SCAN_COMPLETE, NEW_VULN, VULNERABILITY_STATUS
        """
        data = {"action": "add", "title": title,
                "schedule_daily": "1" if schedule_daily else "0"}
        if notification_list: data["notification_email"] = notification_list
        if trigger: data["trigger"] = trigger
        return self.s._post("/api/2.0/fo/alert/", data=data).text

    def update_alert(self, id: str, **kwargs) -> str:
        data = {"action": "edit", "id": id}
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/alert/", data=data).text

    def delete_alert(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/alert/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # LOOKUP TABLES (CVE / Bugtraq / CERT)
    # ------------------------------------------------------------------
    def get_cve_info(self, cve_ids: str) -> str:
        """Get vulnerability details by CVE ID(s) e.g. 'CVE-2021-44228'"""
        return self.s._get("/api/2.0/fo/knowledge_base/vuln/", params={"action": "list", "details": "All", "cve_ids": cve_ids}).text

    # ------------------------------------------------------------------
    # CMDB SYNC (ServiceNow & others)
    # ------------------------------------------------------------------
    def get_cmdb_sync_status(self) -> str:
        """Check the status of the last CMDB sync job."""
        return self.s._get("/api/2.0/fo/cmdb/sync/", params={"action": "status"}).text

    def launch_cmdb_sync(self, cmdb_id: str) -> str:
        """Launch a CMDB sync job."""
        return self.s._post("/api/2.0/fo/cmdb/sync/", data={"action": "sync", "cmdb_id": cmdb_id}).text

    # ------------------------------------------------------------------
    # FAVORITE REPORTS (Bookmarks)
    # ------------------------------------------------------------------
    def list_favorite_reports(self) -> str:
        return self.s._get("/api/2.0/fo/report/", params={"action": "list", "state": "Finished"}).text
