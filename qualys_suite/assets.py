"""
assets.py - Qualys Global AssetView (GAV) Host Asset Module
Covers: Search, Get, Update, Delete host assets.
Valid endpoints: /qps/rest/2.0/{search|get|update|delete}/am/hostasset
"""
from .base import QualysSession
from typing import Optional, List


class AssetsModule:
    def __init__(self, session: QualysSession):
        self.s = session
        self._h = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def search_assets(self, filter_str: Optional[str] = None,
                      fields: Optional[List[str]] = None,
                      page_size: int = 100, page_number: int = 1) -> dict:
        """
        Search host assets from Global AssetView.
        filter_str: simple name substring filter.
        fields: limit returned fields e.g. ['id','name','os','tags']
        """
        body: dict = {
            "ServiceRequest": {
                "preferences": {"limitResults": page_size, "startFromOffset": page_number}
            }
        }
        if filter_str:
            body["ServiceRequest"]["filters"] = {
                "Criteria": [{"field": "name", "operator": "CONTAINS", "value": filter_str}]
            }
        if fields:
            body["ServiceRequest"]["fields"] = ",".join(fields)
        return self.s._post("/qps/rest/2.0/search/am/hostasset", json=body, headers=self._h).json()

    def get_asset(self, asset_id: int) -> dict:
        return self.s._get(f"/qps/rest/2.0/get/am/hostasset/{asset_id}", headers=self._h).json()

    def update_asset(self, asset_id: int, name: Optional[str] = None,
                     comment: Optional[str] = None, tag_ids: Optional[List[int]] = None) -> dict:
        """Update asset name, comment, or assigned tags."""
        asset_data: dict = {}
        if name: asset_data["name"] = name
        if comment: asset_data["comments"] = comment
        if tag_ids:
            asset_data["tags"] = {"list": [{"TagSimple": {"id": tid}} for tid in tag_ids]}
        body = {"ServiceRequest": {"data": {"HostAsset": asset_data}}}
        return self.s._post(f"/qps/rest/2.0/update/am/hostasset/{asset_id}", json=body, headers=self._h).json()

    def delete_asset(self, asset_id: int) -> dict:
        return self.s._post(f"/qps/rest/2.0/delete/am/hostasset/{asset_id}", headers=self._h).json()

    def delete_assets_bulk(self, filter_str: str) -> dict:
        """Delete all assets matching name filter."""
        body = {"ServiceRequest": {"filters": {"Criteria": [
            {"field": "name", "operator": "CONTAINS", "value": filter_str}
        ]}}}
        return self.s._post("/qps/rest/2.0/delete/am/hostasset", json=body, headers=self._h).json()

    def get_tags_for_asset(self, asset_id: int) -> list:
        """Return the tag list for a specific asset."""
        result = self.get_asset(asset_id)
        try:
            return result["ServiceResponse"]["data"][0]["HostAsset"]["tags"]["list"]
        except (KeyError, IndexError):
            return []
