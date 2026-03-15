"""
vm_pc.py - Qualys Vulnerability Management / Policy Compliance Module
Covers: Scans, KnowledgeBase, Host Detection, Auth Records, Reports, Scheduled Scans
API v2 (XML-based): /api/2.0/fo/...
"""
from .base import QualysSession
from typing import Optional, Dict, List


class VMModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # SCANS
    # ------------------------------------------------------------------
    def list_scans(self, state: str = "Running", action: str = "list") -> str:
        """List scans. States: Running, Paused, Canceled, Finished, Error, Queued."""
        return self.s._get("/api/2.0/fo/scan/", params={"action": action, "state": state}).text

    def launch_scan(self, title: str, option_title: str, iscanner_name: str,
                    target_from: str = "tags", ip: Optional[str] = None,
                    tag_include_selector: Optional[str] = None,
                    tag_set_by: Optional[str] = "id", tag_set_include: Optional[str] = None) -> str:
        """Launch a VM scan. Target by IP or asset tags."""
        data = {
            "action": "launch", "scan_title": title, "option_title": option_title,
            "iscanner_name": iscanner_name, "target_from": target_from,
        }
        if ip: data["ip"] = ip
        if tag_set_include: data["tag_set_include"] = tag_set_include
        if tag_include_selector: data["tag_include_selector"] = tag_include_selector
        return self.s._post("/api/2.0/fo/scan/", data=data).text

    def pause_scan(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/", data={"action": "pause", "scan_ref": scan_ref}).text

    def resume_scan(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/", data={"action": "resume", "scan_ref": scan_ref}).text

    def cancel_scan(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/", data={"action": "cancel", "scan_ref": scan_ref}).text

    def delete_scan(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/", data={"action": "delete", "scan_ref": scan_ref}).text

    def fetch_scan_results(self, scan_ref: str, output_format: str = "XML") -> str:
        return self.s._post("/api/2.0/fo/scan/", data={"action": "fetch", "scan_ref": scan_ref, "output_format": output_format}).text

    # ------------------------------------------------------------------
    # HOST DETECTION (host results)
    # ------------------------------------------------------------------
    def list_host_detections(self, ips: Optional[str] = None, ag_ids: Optional[str] = None,
                             qids: Optional[str] = None, severities: Optional[str] = None,
                             status: Optional[str] = None) -> str:
        """List host vulnerability detection for current user."""
        params = {"action": "list"}
        if ips: params["ips"] = ips
        if ag_ids: params["ag_ids"] = ag_ids
        if qids: params["qids"] = qids
        if severities: params["severities"] = severities
        if status: params["status"] = status
        return self.s._get("/api/2.0/fo/asset/host/vm/detection/", params=params).text

    # ------------------------------------------------------------------
    # KNOWLEDGE BASE
    # ------------------------------------------------------------------
    def list_kb_vulnerabilities(self, qids: Optional[str] = None,
                                severities: Optional[str] = None,
                                published_after: Optional[str] = None) -> str:
        """List KnowledgeBase vulnerabilities."""
        params = {"action": "list"}
        if qids: params["ids"] = qids
        if severities: params["severities"] = severities
        if published_after: params["published_after"] = published_after
        return self.s._get("/api/2.0/fo/knowledge_base/vuln/", params=params).text

    def get_kb_vulnerability(self, qid: int) -> str:
        return self.s._get("/api/2.0/fo/knowledge_base/vuln/", params={"action": "list", "ids": str(qid)}).text

    # ------------------------------------------------------------------
    # IP / HOST ASSETS
    # ------------------------------------------------------------------
    def list_ips(self, ips: Optional[str] = None, network_id: Optional[str] = None,
                 compliance_enabled: bool = False) -> str:
        params = {"action": "list"}
        if ips: params["ips"] = ips
        if network_id: params["network_id"] = network_id
        if compliance_enabled: params["compliance_enabled"] = "1"
        return self.s._get("/api/2.0/fo/asset/ip/", params=params).text

    def add_ips(self, ips: str, tracking_method: str = "IP",
                ag_title: Optional[str] = None, comment: Optional[str] = None) -> str:
        data = {"action": "add", "ips": ips, "tracking_method": tracking_method}
        if ag_title: data["ag_title"] = ag_title
        if comment: data["comment"] = comment
        return self.s._post("/api/2.0/fo/asset/ip/", data=data).text

    def update_ips(self, ips: str, tracking_method: Optional[str] = None,
                   comment: Optional[str] = None) -> str:
        data = {"action": "update", "ips": ips}
        if tracking_method: data["tracking_method"] = tracking_method
        if comment: data["comment"] = comment
        return self.s._post("/api/2.0/fo/asset/ip/", data=data).text

    def purge_hosts(self, ips: str) -> str:
        return self.s._post("/api/2.0/fo/asset/host/", data={"action": "purge", "ips": ips}).text

    # ------------------------------------------------------------------
    # ASSET GROUPS
    # ------------------------------------------------------------------
    def list_asset_groups(self, title: Optional[str] = None) -> str:
        params = {"action": "list"}
        if title: params["title"] = title
        return self.s._get("/api/2.0/fo/asset/group/", params=params).text

    def create_asset_group(self, title: str, ips: Optional[str] = None, comment: Optional[str] = None) -> str:
        data = {"action": "add", "title": title}
        if ips: data["ips"] = ips
        if comment: data["comments"] = comment
        return self.s._post("/api/2.0/fo/asset/group/", data=data).text

    def update_asset_group(self, id: str, title: Optional[str] = None,
                           add_ips: Optional[str] = None, remove_ips: Optional[str] = None) -> str:
        data = {"action": "edit", "id": id}
        if title: data["set_title"] = title
        if add_ips: data["add_ips"] = add_ips
        if remove_ips: data["remove_ips"] = remove_ips
        return self.s._post("/api/2.0/fo/asset/group/", data=data).text

    def delete_asset_group(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/asset/group/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # SCHEDULED SCANS
    # ------------------------------------------------------------------
    def list_scheduled_scans(self) -> str:
        return self.s._get("/api/2.0/fo/schedule/scan/", params={"action": "list"}).text

    def create_scheduled_scan(self, scan_title: str, option_title: str,
                              iscanner_name: str, frequency_days: int,
                              start_date: str, start_hour: int, start_minute: int,
                              ip: Optional[str] = None) -> str:
        data = {
            "action": "create", "scan_title": scan_title,
            "option_title": option_title, "iscanner_name": iscanner_name,
            "frequency_days": str(frequency_days),
            "start_date": start_date, "start_hour": str(start_hour),
            "start_minute": str(start_minute), "active": "1"
        }
        if ip: data["ip"] = ip
        return self.s._post("/api/2.0/fo/schedule/scan/", data=data).text

    def update_scheduled_scan(self, id: str, **kwargs) -> str:
        data = {"action": "update", "id": id}
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/schedule/scan/", data=data).text

    def delete_scheduled_scan(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/schedule/scan/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # AUTHENTICATION RECORDS
    # ------------------------------------------------------------------
    def list_auth_records(self, target: str = "unix") -> str:
        """target: unix, windows, oracle, sybase, db2, http, iis, vmware"""
        return self.s._get(f"/api/2.0/fo/auth/{target}/", params={"action": "list"}).text

    def create_auth_record(self, target: str, title: str, ips: str, login: str, password: str) -> str:
        data = {"action": "create", "title": title, "ips": ips,
                "username": login, "password": password}
        return self.s._post(f"/api/2.0/fo/auth/{target}/", data=data).text

    def update_auth_record(self, target: str, id: str, **kwargs) -> str:
        data = {"action": "edit", "id": id}
        data.update(kwargs)
        return self.s._post(f"/api/2.0/fo/auth/{target}/", data=data).text

    def delete_auth_record(self, target: str, id: str) -> str:
        return self.s._post(f"/api/2.0/fo/auth/{target}/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # OPTION PROFILES
    # ------------------------------------------------------------------
    def list_option_profiles(self) -> str:
        return self.s._get("/api/2.0/fo/subscription/option_profile/vm/", params={"action": "list"}).text
