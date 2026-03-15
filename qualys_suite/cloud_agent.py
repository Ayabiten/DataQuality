"""
cloud_agent.py - Qualys Cloud Agent Module
Covers: Agent inventory, activation/deactivation, uninstall, activation keys.

Verified endpoints:
  Agents:          /qps/rest/2.0/{action}/am/hostasset/{id}
  Activation Keys: /qps/rest/1.0/{action}/ca/agentactkey  (v1.0, not 2.0)
"""
from .base import QualysSession
from typing import Optional
import json


class CloudAgentModule:
    def __init__(self, session: QualysSession):
        self.s = session
        self._h_json = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    # ------------------------------------------------------------------
    # AGENTS  (Host Assets with Cloud Agent — uses AM v2.0)
    # ------------------------------------------------------------------
    def list_agents(self, page_size: int = 100, page_number: int = 0,
                    filter_str: Optional[str] = None) -> dict:
        """
        List cloud agents.
        filter_str: name substring filter.
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
        resp = self.s._post("/qps/rest/2.0/search/am/hostasset", json=body, headers=self._h_json)
        return resp.json()

    def get_agent(self, agent_id: int) -> dict:
        resp = self.s._get(f"/qps/rest/2.0/get/am/hostasset/{agent_id}", headers=self._h_json)
        return resp.json()

    def update_agent(self, agent_id: int, name: Optional[str] = None,
                     comment: Optional[str] = None, tag_ids: Optional[list] = None) -> dict:
        asset_data: dict = {}
        if name: asset_data["name"] = name
        if comment: asset_data["comments"] = comment
        if tag_ids: asset_data["tags"] = {"list": [{"TagSimple": {"id": tid}} for tid in tag_ids]}
        body = {"ServiceRequest": {"data": {"HostAsset": asset_data}}}
        resp = self.s._post(f"/qps/rest/2.0/update/am/hostasset/{agent_id}", json=body, headers=self._h_json)
        return resp.json()

    def deactivate_agent(self, agent_id: int) -> dict:
        """Deactivate a cloud agent (stops it reporting)."""
        resp = self.s._post(f"/qps/rest/2.0/deactivate/am/hostasset/{agent_id}", headers=self._h_json)
        return resp.json()

    def uninstall_agent(self, agent_id: int) -> dict:
        """Request remote uninstall of the cloud agent."""
        resp = self.s._post(f"/qps/rest/2.0/uninstall/am/hostasset/{agent_id}", headers=self._h_json)
        return resp.json()

    def delete_agent(self, agent_id: int) -> dict:
        """Remove the agent record from Qualys."""
        resp = self.s._post(f"/qps/rest/2.0/delete/am/hostasset/{agent_id}", headers=self._h_json)
        return resp.json()

    # ------------------------------------------------------------------
    # ACTIVATION KEYS  (Cloud Agent API v1.0 — /qps/rest/1.0/ca/agentactkey)
    # ------------------------------------------------------------------
    def list_activation_keys(self) -> dict:
        """List all Cloud Agent activation keys. Uses CA v1.0 endpoint."""
        resp = self.s._post("/qps/rest/1.0/search/ca/agentactkey/",
                            json={"ServiceRequest": {}}, headers=self._h_json)
        return resp.json()

    def get_activation_key(self, key_id: int) -> dict:
        resp = self.s._get(f"/qps/rest/1.0/get/ca/agentactkey/{key_id}", headers=self._h_json)
        return resp.json()

    def create_activation_key(self, title: str, active: bool = True,
                              module: str = "VM", tag_ids: Optional[list] = None) -> dict:
        """
        Create a Cloud Agent activation key.
        module: 'VM' | 'PC' | 'SCA' | 'CSAM' | 'FIM' | 'AI'
        """
        key_data: dict = {
            "title": title,
            "active": str(active).lower(),
            "agentModules": module
        }
        if tag_ids:
            key_data["tags"] = {"list": [{"TagSimple": {"id": tid}} for tid in tag_ids]}
        body = {"ServiceRequest": {"data": {"AgentActKey": key_data}}}
        resp = self.s._post("/qps/rest/1.0/create/ca/agentactkey/", json=body, headers=self._h_json)
        return resp.json()

    def update_activation_key(self, key_id: int, title: Optional[str] = None,
                              active: Optional[bool] = None) -> dict:
        key_data: dict = {}
        if title: key_data["title"] = title
        if active is not None: key_data["active"] = str(active).lower()
        body = {"ServiceRequest": {"data": {"AgentActKey": key_data}}}
        resp = self.s._post(f"/qps/rest/1.0/update/ca/agentactkey/{key_id}", json=body, headers=self._h_json)
        return resp.json()

    def delete_activation_key(self, key_id: int) -> dict:
        resp = self.s._post(f"/qps/rest/1.0/delete/ca/agentactkey/{key_id}", headers=self._h_json)
        return resp.json()

    def change_agent_activation_key(self, agent_ids: list, new_key_id: int) -> dict:
        """
        Re-key agents to a different activation key.
        Uses the 'changeactkey' action endpoint.
        """
        body = {
            "ServiceRequest": {
                "data": {
                    "AgentActKey": {"id": new_key_id}
                },
                "filters": {
                    "Criteria": [
                        {"field": "id", "operator": "IN", "value": ",".join(map(str, agent_ids))}
                    ]
                }
            }
        }
        resp = self.s._post("/qps/rest/1.0/cak/ca/changeactkey/", json=body, headers=self._h_json)
        return resp.json()
