"""
was_waf.py - Qualys WAS (Web Application Scanning) & WAF Module
Covers: Web Application CRUD, WAS Scans, Findings, WAF Webapp, Security Policies.
API: /qps/rest/3.0/...
"""
from .base import QualysSession
from typing import Optional, List


class WASWAFModule:
    def __init__(self, session: QualysSession):
        self.s = session
        self._h = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    # ==================================================================
    # WEB APPLICATIONS
    # ==================================================================
    def list_web_apps(self, name: Optional[str] = None, page_size: int = 100) -> dict:
        body: dict = {"ServiceRequest": {"preferences": {"limitResults": page_size}}}
        if name:
            body["ServiceRequest"]["filters"] = {"Criteria": [{"field": "name", "operator": "CONTAINS", "value": name}]}
        return self.s._post("/qps/rest/3.0/search/was/webapp", json=body, headers=self._h).json()

    def get_web_app(self, webapp_id: int) -> dict:
        return self.s._get(f"/qps/rest/3.0/get/was/webapp/{webapp_id}", headers=self._h).json()

    def create_web_app(self, name: str, url: str,
                       tag_ids: Optional[List[int]] = None,
                       auth_record_id: Optional[int] = None) -> dict:
        webapp: dict = {"name": name, "url": url}
        if tag_ids: webapp["tags"] = {"list": [{"Tag": {"id": tid}} for tid in tag_ids]}
        if auth_record_id: webapp["authRecords"] = {"list": [{"WebAppAuthRecord": {"id": auth_record_id}}]}
        body = {"ServiceRequest": {"data": {"WebApp": webapp}}}
        return self.s._post("/qps/rest/3.0/create/was/webapp", json=body, headers=self._h).json()

    def update_web_app(self, webapp_id: int, name: Optional[str] = None,
                       url: Optional[str] = None) -> dict:
        webapp: dict = {}
        if name: webapp["name"] = name
        if url: webapp["url"] = url
        body = {"ServiceRequest": {"data": {"WebApp": webapp}}}
        return self.s._post(f"/qps/rest/3.0/update/was/webapp/{webapp_id}", json=body, headers=self._h).json()

    def delete_web_app(self, webapp_id: int) -> dict:
        return self.s._post(f"/qps/rest/3.0/delete/was/webapp/{webapp_id}", headers=self._h).json()

    # ==================================================================
    # WAS SCANS
    # ==================================================================
    def list_was_scans(self, name: Optional[str] = None) -> dict:
        body: dict = {"ServiceRequest": {}}
        if name:
            body["ServiceRequest"]["filters"] = {"Criteria": [{"field": "name", "operator": "CONTAINS", "value": name}]}
        return self.s._post("/qps/rest/3.0/search/was/wasscan", json=body, headers=self._h).json()

    def get_was_scan(self, scan_id: int) -> dict:
        return self.s._get(f"/qps/rest/3.0/get/was/wasscan/{scan_id}", headers=self._h).json()

    def launch_was_scan(self, name: str, webapp_id: int, scan_type: str = "VULNERABILITY",
                        option_profile_id: Optional[int] = None,
                        scanner_appliance_type: str = "INTERNAL") -> dict:
        """
        Launch a WAS scan.
        scan_type: VULNERABILITY, DISCOVERY, BURP
        scanner_appliance_type: INTERNAL, EXTERNAL
        """
        scan_data: dict = {
            "name": name, "type": scan_type,
            "target": {"webApp": {"id": webapp_id},
                       "scannerAppliance": {"type": scanner_appliance_type}}
        }
        if option_profile_id:
            scan_data["profile"] = {"id": option_profile_id}
        body = {"ServiceRequest": {"data": {"WasScan": scan_data}}}
        return self.s._post("/qps/rest/3.0/launch/was/wasscan", json=body, headers=self._h).json()

    def cancel_was_scan(self, scan_id: int) -> dict:
        return self.s._post(f"/qps/rest/3.0/cancel/was/wasscan/{scan_id}", headers=self._h).json()

    def delete_was_scan(self, scan_id: int) -> dict:
        return self.s._post(f"/qps/rest/3.0/delete/was/wasscan/{scan_id}", headers=self._h).json()

    # ==================================================================
    # WAS FINDINGS
    # ==================================================================
    def search_findings(self, webapp_id: Optional[int] = None,
                        severity: Optional[List[int]] = None,
                        ftype: str = "VULNERABILITY") -> dict:
        """
        Search WAS findings.
        ftype: VULNERABILITY, SENSITIVE_CONTENT, INFORMATION_GATHERED
        severity: list of ints (1-5)
        """
        criteria = [{"field": "type", "operator": "EQUALS", "value": ftype}]
        if webapp_id:
            criteria.append({"field": "webApp.id", "operator": "EQUALS", "value": str(webapp_id)})
        if severity:
            criteria.append({"field": "severity", "operator": "IN", "value": ",".join(map(str, severity))})
        body = {"ServiceRequest": {"filters": {"Criteria": criteria}}}
        return self.s._post("/qps/rest/3.0/search/was/finding", json=body, headers=self._h).json()

    def get_finding(self, finding_id: int) -> dict:
        return self.s._get(f"/qps/rest/3.0/get/was/finding/{finding_id}", headers=self._h).json()

    def update_finding(self, finding_id: int, comment: Optional[str] = None,
                       ignored: Optional[bool] = None) -> dict:
        finding: dict = {}
        if comment: finding["lastComment"] = {"comment": comment}
        if ignored is not None: finding["ignored"] = str(ignored).lower()
        body = {"ServiceRequest": {"data": {"Finding": finding}}}
        return self.s._post(f"/qps/rest/3.0/update/was/finding/{finding_id}", json=body, headers=self._h).json()

    # ==================================================================
    # WAF WEB APPS & POLICIES
    # ==================================================================
    def list_waf_webapps(self) -> dict:
        return self.s._get("/qps/rest/3.0/search/waf/webapp", headers=self._h).json()

    def list_waf_policies(self) -> dict:
        return self.s._get("/qps/rest/3.0/search/waf/policy", headers=self._h).json()

    def create_waf_policy(self, name: str, policy_type: str = "STANDARD") -> dict:
        body = {"ServiceRequest": {"data": {"WAFPolicy": {"name": name, "type": policy_type}}}}
        return self.s._post("/qps/rest/3.0/create/waf/policy", json=body, headers=self._h).json()

    def update_waf_policy(self, policy_id: int, name: Optional[str] = None) -> dict:
        policy: dict = {}
        if name: policy["name"] = name
        body = {"ServiceRequest": {"data": {"WAFPolicy": policy}}}
        return self.s._post(f"/qps/rest/3.0/update/waf/policy/{policy_id}", json=body, headers=self._h).json()

    def delete_waf_policy(self, policy_id: int) -> dict:
        return self.s._post(f"/qps/rest/3.0/delete/waf/policy/{policy_id}", headers=self._h).json()
