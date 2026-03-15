"""
reports.py - Qualys Reporting Module
Covers: Report templates, launching reports, downloading reports, saved results.
API: /api/2.0/fo/report/...
"""
from .base import QualysSession
from typing import Optional


class ReportsModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # REPORT TEMPLATES
    # ------------------------------------------------------------------
    def list_report_templates(self) -> str:
        return self.s._get("/api/2.0/fo/report/template/", params={"action": "list"}).text

    # ------------------------------------------------------------------
    # REPORTS - CRUD
    # ------------------------------------------------------------------
    def list_reports(self, state: Optional[str] = None, id: Optional[str] = None) -> str:
        """List reports. State: Finished, Running, Error, Deleted"""
        params = {"action": "list"}
        if state: params["state"] = state
        if id: params["id"] = id
        return self.s._get("/api/2.0/fo/report/", params=params).text

    def launch_report(self, report_type: str, template_id: str,
                      output_format: str = "PDF", report_title: Optional[str] = None,
                      ips: Optional[str] = None, asset_group_ids: Optional[str] = None,
                      tag_id: Optional[str] = None) -> str:
        """
        Launch a report.
        report_type: 'Scan', 'Map', 'Patch', 'Remediation', 'Compliance', 'AssetSearch'
        output_format: PDF, HTML, MHT, XML, CSV, RTF
        """
        data = {
            "action": "launch",
            "report_type": report_type,
            "template_id": template_id,
            "output_format": output_format
        }
        if report_title: data["report_title"] = report_title
        if ips: data["ips"] = ips
        if asset_group_ids: data["asset_group_ids"] = asset_group_ids
        if tag_id: data["tag_id"] = tag_id
        return self.s._post("/api/2.0/fo/report/", data=data).text

    def fetch_report(self, report_id: str) -> bytes:
        """Download a finished report. Returns raw bytes (PDF, CSV, etc.)."""
        return self.s._get("/api/2.0/fo/report/", params={"action": "fetch", "id": report_id}).content

    def delete_report(self, report_id: str) -> str:
        return self.s._post("/api/2.0/fo/report/", data={"action": "delete", "id": report_id}).text

    def cancel_report(self, report_id: str) -> str:
        return self.s._post("/api/2.0/fo/report/", data={"action": "cancel", "id": report_id}).text

    # ------------------------------------------------------------------
    # SCAN-BASED REPORTS (Direct scan-to-report)
    # ------------------------------------------------------------------
    def launch_scan_report(self, scan_ref: str, template_id: str,
                           output_format: str = "PDF", report_title: Optional[str] = None) -> str:
        """Launch a report tied to a specific scan reference."""
        data = {
            "action": "launch", "template_id": template_id,
            "output_format": output_format, "report_type": "Scan",
            "ips_network_paths": {"scan": {"value": scan_ref}}
        }
        if report_title: data["report_title"] = report_title
        return self.s._post("/api/2.0/fo/report/", data=data).text

    # ------------------------------------------------------------------
    # SAVED SEARCHES
    # ------------------------------------------------------------------
    def save_last_scan_results(self, output_file: str, report_id: str):
        """Download and save a report to a local file."""
        content = self.fetch_report(report_id)
        with open(output_file, "wb") as f:
            f.write(content)
        print(f"Report saved to: {output_file}")

    # ------------------------------------------------------------------
    # SCORECARD / EXECUTIVE REPORTS
    # ------------------------------------------------------------------
    def launch_scorecard_report(self, template_id: str, output_format: str = "PDF",
                                report_title: Optional[str] = None) -> str:
        """Launch a Patch or Exec Report Scorecard."""
        data = {
            "action": "launch", "report_type": "Patch",
            "template_id": template_id, "output_format": output_format
        }
        if report_title: data["report_title"] = report_title
        return self.s._post("/api/2.0/fo/report/", data=data).text

    # ------------------------------------------------------------------
    # COMPLIANCE REPORTS
    # ------------------------------------------------------------------
    def launch_compliance_report(self, template_id: str, policy_id: str,
                                 output_format: str = "PDF", ips: Optional[str] = None) -> str:
        data = {
            "action": "launch", "report_type": "Compliance",
            "template_id": template_id, "output_format": output_format,
            "policy_id": policy_id
        }
        if ips: data["ips"] = ips
        return self.s._post("/api/2.0/fo/report/", data=data).text
