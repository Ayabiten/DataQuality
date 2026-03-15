"""
tags.py - Qualys Asset Tagging Module
Covers: Create, search, update, delete tags; assign tags to assets.

Valid endpoints: /qps/rest/2.0/{search|get|create|update|delete}/am/tag

NOTE on Tag Hierarchy:
  Qualys has NO separate 'Tag Category' resource.
  Model categories as root-level parent tags and nest children under them
  using the parentTagId field when creating/updating a tag.
"""
from .base import QualysSession
from typing import Optional, List


class TagsModule:
    def __init__(self, session: QualysSession):
        self.s = session
        self._h = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def list_tags(self, name: Optional[str] = None) -> dict:
        """Search all tags, optionally filtered by name substring."""
        body: dict = {"ServiceRequest": {}}
        if name:
            body["ServiceRequest"]["filters"] = {
                "Criteria": [{"field": "name", "operator": "CONTAINS", "value": name}]
            }
        return self.s._post("/qps/rest/2.0/search/am/tag", json=body, headers=self._h).json()

    def get_tag(self, tag_id: int) -> dict:
        return self.s._get(f"/qps/rest/2.0/get/am/tag/{tag_id}", headers=self._h).json()

    def create_tag(self, name: str, parent_id: Optional[int] = None,
                   color: Optional[str] = None, rule_type: Optional[str] = None,
                   rule_text: Optional[str] = None, children: Optional[List[str]] = None) -> dict:
        """
        Create a static or dynamic tag.
        rule_type: 'GROOVY' | 'STATIC' | 'OS_REGEX' | 'NETWORK_RANGE'
        rule_text: Groovy expression or regex (only when rule_type is provided)
        parent_id: Creates a child tag under an existing parent tag.
                   Use this to build a tag tree (e.g. category > sub-tags).
        children:  Optional list of immediate child tag names to create inline.
        """
        tag_data: dict = {"name": name}
        if parent_id: tag_data["parentTagId"] = parent_id
        if color: tag_data["color"] = color
        if rule_type and rule_text:
            tag_data["ruleType"] = rule_type
            tag_data["ruleText"] = rule_text
        if children:
            tag_data["children"] = {"list": [{"TagSimple": {"name": c}} for c in children]}
        body = {"ServiceRequest": {"data": {"Tag": tag_data}}}
        return self.s._post("/qps/rest/2.0/create/am/tag", json=body, headers=self._h).json()

    def update_tag(self, tag_id: int, name: Optional[str] = None,
                   color: Optional[str] = None, rule_text: Optional[str] = None,
                   parent_id: Optional[int] = None) -> dict:
        """Update an existing tag's name, color, rule, or parent."""
        tag_data: dict = {}
        if name: tag_data["name"] = name
        if color: tag_data["color"] = color
        if rule_text: tag_data["ruleText"] = rule_text
        if parent_id: tag_data["parentTagId"] = parent_id
        body = {"ServiceRequest": {"data": {"Tag": tag_data}}}
        return self.s._post(f"/qps/rest/2.0/update/am/tag/{tag_id}", json=body, headers=self._h).json()

    def delete_tag(self, tag_id: int) -> dict:
        return self.s._post(f"/qps/rest/2.0/delete/am/tag/{tag_id}", headers=self._h).json()

    def delete_tags_bulk(self, tag_ids: List[int]) -> dict:
        """Delete multiple tags by ID list in a single request."""
        body = {"ServiceRequest": {"filters": {"Criteria": [
            {"field": "id", "operator": "IN", "value": ",".join(map(str, tag_ids))}
        ]}}}
        return self.s._post("/qps/rest/2.0/delete/am/tag", json=body, headers=self._h).json()

    def add_tag_to_asset(self, asset_id: int, tag_ids: List[int]) -> dict:
        """
        Assign one or more tags to a host asset.
        Delegates to update /am/hostasset with the tag list.
        """
        tag_list = {"list": [{"TagSimple": {"id": tid}} for tid in tag_ids]}
        body = {"ServiceRequest": {"data": {"HostAsset": {"tags": tag_list}}}}
        return self.s._post(f"/qps/rest/2.0/update/am/hostasset/{asset_id}", json=body, headers=self._h).json()
