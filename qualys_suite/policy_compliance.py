"""
policy_compliance.py - Qualys Policy Compliance (PC) Module
Covers: PC Scans, Policies, Controls, Exceptions, Evidence, Posture Info
API: /api/2.0/fo/scan/ (compliance_flag=1), /api/2.0/fo/compliance/policy/
"""
from .base import QualysSession
from typing import Optional


class PolicyComplianceModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # PC SCANS
    # ------------------------------------------------------------------
    def list_pc_scans(self, state: Optional[str] = None) -> str:
        """List PC scans. State: Running, Paused, Finished, Error"""
        params = {"action": "list", "scan_type": "compliance"}
        if state: params["state"] = state
        return self.s._get("/api/2.0/fo/scan/compliance/", params=params).text

    def launch_pc_scan(self, title: str, option_title: str, iscanner_name: str,
                       ip: Optional[str] = None, asset_group_title: Optional[str] = None,
                       target_from: str = "ips") -> str:
        data = {
            "action": "launch", "scan_title": title,
            "option_title": option_title, "iscanner_name": iscanner_name,
            "target_from": target_from
        }
        if ip: data["ip"] = ip
        if asset_group_title: data["asset_group_title"] = asset_group_title
        return self.s._post("/api/2.0/fo/scan/compliance/", data=data).text

    def cancel_pc_scan(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/compliance/", data={"action": "cancel", "scan_ref": scan_ref}).text

    def delete_pc_scan(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/compliance/", data={"action": "delete", "scan_ref": scan_ref}).text

    def fetch_pc_scan_results(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/compliance/", data={"action": "fetch", "scan_ref": scan_ref}).text

    # ------------------------------------------------------------------
    # POSTURE INFORMATION
    # ------------------------------------------------------------------
    def list_posture_info(self, status: Optional[str] = None, policy_id: Optional[str] = None,
                          ips: Optional[str] = None) -> str:
        """List PC posture/control results. Status: PASSED, FAILED, ERROR"""
        params = {"action": "list"}
        if status: params["status"] = status
        if policy_id: params["policy_id"] = policy_id
        if ips: params["ips"] = ips
        return self.s._get("/api/2.0/fo/compliance/posture/info/", params=params).text

    # ------------------------------------------------------------------
    # POLICIES
    # ------------------------------------------------------------------
    def list_policies(self, title: Optional[str] = None, id: Optional[str] = None) -> str:
        params = {"action": "list"}
        if title: params["title"] = title
        if id: params["id"] = id
        return self.s._get("/api/2.0/fo/compliance/policy/", params=params).text

    def create_policy(self, title: str, tech_platforms: Optional[str] = None) -> str:
        data = {"action": "create", "title": title}
        if tech_platforms: data["tech_platforms"] = tech_platforms
        return self.s._post("/api/2.0/fo/compliance/policy/", data=data).text

    def update_policy(self, id: str, title: Optional[str] = None, **kwargs) -> str:
        data = {"action": "update", "id": id}
        if title: data["title"] = title
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/compliance/policy/", data=data).text

    def delete_policy(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/compliance/policy/", data={"action": "delete", "id": id}).text

    def export_policy(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/compliance/policy/", data={"action": "export", "id": id}).text

    # ------------------------------------------------------------------
    # CONTROLS
    # ------------------------------------------------------------------
    def list_controls(self, ids: Optional[str] = None, technologies: Optional[str] = None) -> str:
        params = {"action": "list"}
        if ids: params["control_ids"] = ids
        if technologies: params["technologies"] = technologies
        return self.s._get("/api/2.0/fo/compliance/control/", params=params).text

    # ------------------------------------------------------------------
    # EXCEPTIONS
    # ------------------------------------------------------------------
    def list_exceptions(self, policy_id: Optional[str] = None,
                        ips: Optional[str] = None, status: Optional[str] = None) -> str:
        """Status: PENDING, APPROVED, REJECTED"""
        params = {"action": "list"}
        if policy_id: params["policy_id"] = policy_id
        if ips: params["ips"] = ips
        if status: params["status"] = status
        return self.s._get("/api/2.0/fo/compliance/exception/", params=params).text

    def create_exception(self, policy_id: str, control_id: str, ips: str,
                         reason: str, comments: Optional[str] = None) -> str:
        data = {
            "action": "create", "policy_id": policy_id,
            "control_id": control_id, "ips": ips, "reason": reason
        }
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/compliance/exception/", data=data).text

    def approve_exception(self, exception_ids: str, comments: Optional[str] = None) -> str:
        data = {"action": "approve", "exception_id": exception_ids}
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/compliance/exception/", data=data).text

    def reject_exception(self, exception_ids: str, comments: Optional[str] = None) -> str:
        data = {"action": "reject", "exception_id": exception_ids}
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/compliance/exception/", data=data).text

    def delete_exception(self, exception_ids: str) -> str:
        return self.s._post("/api/2.0/fo/compliance/exception/", data={"action": "delete", "exception_id": exception_ids}).text

    # ------------------------------------------------------------------
    # SCHEDULED PC SCANS
    # ------------------------------------------------------------------
    def list_scheduled_pc_scans(self) -> str:
        return self.s._get("/api/2.0/fo/schedule/compliance/", params={"action": "list"}).text

    def create_scheduled_pc_scan(self, title: str, option_title: str,
                                 iscanner_name: str, frequency_days: int,
                                 start_date: str, start_hour: int,
                                 ip: Optional[str] = None) -> str:
        data = {
            "action": "create", "scan_title": title, "option_title": option_title,
            "iscanner_name": iscanner_name, "frequency_days": str(frequency_days),
            "start_date": start_date, "start_hour": str(start_hour), "active": "1"
        }
        if ip: data["ip"] = ip
        return self.s._post("/api/2.0/fo/schedule/compliance/", data=data).text

    def delete_scheduled_pc_scan(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/schedule/compliance/", data={"action": "delete", "id": id}).text
