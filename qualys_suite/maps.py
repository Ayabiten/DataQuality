"""
maps.py - Qualys Network Maps (Discovery) Module
Covers: Map Scans (list, launch, cancel, delete, fetch), Scheduled Maps
API: /api/2.0/fo/scan/map/
"""
from .base import QualysSession
from typing import Optional


class MapsModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # MAP SCANS
    # ------------------------------------------------------------------
    def list_maps(self, state: Optional[str] = None, asset_group_ids: Optional[str] = None) -> str:
        """state: Running, Paused, Canceled, Finished, Error"""
        params = {"action": "list"}
        if state: params["state"] = state
        if asset_group_ids: params["asset_group_ids"] = asset_group_ids
        return self.s._get("/api/2.0/fo/scan/map/", params=params).text

    def launch_map(self, title: str, option_title: str, iscanner_name: str,
                   ip: Optional[str] = None, asset_group_ids: Optional[str] = None,
                   fqdn: Optional[str] = None, target_from: str = "ips") -> str:
        data = {
            "action": "launch", "scan_title": title,
            "option_title": option_title, "iscanner_name": iscanner_name,
            "target_from": target_from
        }
        if ip: data["ip"] = ip
        if asset_group_ids: data["asset_group_ids"] = asset_group_ids
        if fqdn: data["fqdn"] = fqdn
        return self.s._post("/api/2.0/fo/scan/map/", data=data).text

    def cancel_map(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/map/", data={"action": "cancel", "scan_ref": scan_ref}).text

    def pause_map(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/map/", data={"action": "pause", "scan_ref": scan_ref}).text

    def resume_map(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/map/", data={"action": "resume", "scan_ref": scan_ref}).text

    def delete_map(self, scan_ref: str) -> str:
        return self.s._post("/api/2.0/fo/scan/map/", data={"action": "delete", "scan_ref": scan_ref}).text

    def fetch_map_results(self, scan_ref: str, output_format: str = "XML") -> str:
        return self.s._post("/api/2.0/fo/scan/map/", data={"action": "fetch", "scan_ref": scan_ref, "output_format": output_format}).text

    # ------------------------------------------------------------------
    # SCHEDULED MAP SCANS
    # ------------------------------------------------------------------
    def list_scheduled_maps(self) -> str:
        return self.s._get("/api/2.0/fo/schedule/map/", params={"action": "list"}).text

    def create_scheduled_map(self, title: str, option_title: str, iscanner_name: str,
                             frequency_days: int, start_date: str, start_hour: int,
                             ip: Optional[str] = None) -> str:
        data = {
            "action": "create", "scan_title": title, "option_title": option_title,
            "iscanner_name": iscanner_name, "frequency_days": str(frequency_days),
            "start_date": start_date, "start_hour": str(start_hour), "active": "1"
        }
        if ip: data["ip"] = ip
        return self.s._post("/api/2.0/fo/schedule/map/", data=data).text

    def update_scheduled_map(self, id: str, **kwargs) -> str:
        data = {"action": "update", "id": id}
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/schedule/map/", data=data).text

    def delete_scheduled_map(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/schedule/map/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # MAP RESULTS (Existing stored results)
    # ------------------------------------------------------------------
    def list_map_results(self, asset_group_ids: Optional[str] = None,
                         since_date: Optional[str] = None) -> str:
        params = {"action": "list"}
        if asset_group_ids: params["asset_group_ids"] = asset_group_ids
        if since_date: params["since_datetime"] = since_date
        return self.s._get("/api/2.0/fo/scan/map/", params=params).text
