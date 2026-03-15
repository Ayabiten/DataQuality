"""
networks.py - Qualys Networks & Scanner Appliance Module
Covers: Virtual Networks, Scanner Appliances, Scanner Groups
API: /api/2.0/fo/network/, /api/2.0/fo/appliance/
"""
from .base import QualysSession
from typing import Optional


class NetworksModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # VIRTUAL NETWORKS
    # ------------------------------------------------------------------
    def list_networks(self, id: Optional[str] = None, name: Optional[str] = None) -> str:
        params = {"action": "list"}
        if id: params["id"] = id
        if name: params["name"] = name
        return self.s._get("/api/2.0/fo/network/", params=params).text

    def create_network(self, name: str, ips: Optional[str] = None, description: Optional[str] = None) -> str:
        data = {"action": "create", "name": name}
        if ips: data["ips"] = ips
        if description: data["description"] = description
        return self.s._post("/api/2.0/fo/network/", data=data).text

    def update_network(self, id: str, name: Optional[str] = None,
                       add_ips: Optional[str] = None, remove_ips: Optional[str] = None) -> str:
        data = {"action": "edit", "id": id}
        if name: data["set_name"] = name
        if add_ips: data["add_ips"] = add_ips
        if remove_ips: data["remove_ips"] = remove_ips
        return self.s._post("/api/2.0/fo/network/", data=data).text

    def delete_network(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/network/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # SCANNER APPLIANCES
    # ------------------------------------------------------------------
    def list_appliances(self, id: Optional[str] = None, name: Optional[str] = None,
                        type: Optional[str] = None) -> str:
        """type: physical, virtual, cloud"""
        params = {"action": "list"}
        if id: params["id"] = id
        if name: params["name"] = name
        if type: params["type"] = type
        return self.s._get("/api/2.0/fo/appliance/", params=params).text

    def update_appliance(self, id: str, name: Optional[str] = None,
                         default_network_id: Optional[str] = None) -> str:
        data = {"action": "edit", "id": id}
        if name: data["name"] = name
        if default_network_id: data["default_network_id"] = default_network_id
        return self.s._post("/api/2.0/fo/appliance/", data=data).text

    def delete_appliance(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/appliance/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # SCANNER GROUPS
    # ------------------------------------------------------------------
    def list_scanner_groups(self) -> str:
        return self.s._get("/api/2.0/fo/scanner_group/", params={"action": "list"}).text

    def create_scanner_group(self, name: str, scanner_appliances: Optional[str] = None) -> str:
        data = {"action": "add", "name": name}
        if scanner_appliances: data["scanner_appliances"] = scanner_appliances
        return self.s._post("/api/2.0/fo/scanner_group/", data=data).text

    def update_scanner_group(self, id: str, name: Optional[str] = None,
                             add_appliances: Optional[str] = None,
                             remove_appliances: Optional[str] = None) -> str:
        data = {"action": "edit", "id": id}
        if name: data["set_name"] = name
        if add_appliances: data["add_scanner_appliances"] = add_appliances
        if remove_appliances: data["remove_scanner_appliances"] = remove_appliances
        return self.s._post("/api/2.0/fo/scanner_group/", data=data).text

    def delete_scanner_group(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/scanner_group/", data={"action": "delete", "id": id}).text
