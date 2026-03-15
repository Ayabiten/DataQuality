"""
search_lists.py - Qualys Search Lists & Option Profiles Module
Covers: Static/Dynamic search lists, VM/PC Option Profiles (full CRUD)
API: /api/2.0/fo/qid/search_list/static/, /api/2.0/fo/qid/search_list/dynamic/
"""
from .base import QualysSession
from typing import Optional


class SearchListsOptionProfilesModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # STATIC SEARCH LISTS
    # ------------------------------------------------------------------
    def list_static_lists(self, id: Optional[str] = None, title: Optional[str] = None) -> str:
        params = {"action": "list"}
        if id: params["id"] = id
        if title: params["title"] = title
        return self.s._get("/api/2.0/fo/qid/search_list/static/", params=params).text

    def create_static_list(self, title: str, qids: str,
                           global_: bool = False, comments: Optional[str] = None) -> str:
        """qids: Comma-separated list of QIDs e.g. '38170,38171'"""
        data = {"action": "create", "title": title, "qids": qids,
                "global": "1" if global_ else "0"}
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/qid/search_list/static/", data=data).text

    def update_static_list(self, id: str, title: Optional[str] = None,
                           add_qids: Optional[str] = None, remove_qids: Optional[str] = None,
                           comments: Optional[str] = None) -> str:
        data = {"action": "update", "id": id}
        if title: data["title"] = title
        if add_qids: data["add_qids"] = add_qids
        if remove_qids: data["remove_qids"] = remove_qids
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/qid/search_list/static/", data=data).text

    def delete_static_list(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/qid/search_list/static/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # DYNAMIC SEARCH LISTS
    # ------------------------------------------------------------------
    def list_dynamic_lists(self, id: Optional[str] = None, title: Optional[str] = None) -> str:
        params = {"action": "list"}
        if id: params["id"] = id
        if title: params["title"] = title
        return self.s._get("/api/2.0/fo/qid/search_list/dynamic/", params=params).text

    def create_dynamic_list(self, title: str, severities: Optional[str] = None,
                            vuln_types: Optional[str] = None,
                            categories: Optional[str] = None,
                            global_: bool = False) -> str:
        """vuln_types: Confirmed, Potential; severities: 1-5"""
        data = {"action": "create", "title": title, "global": "1" if global_ else "0"}
        if severities: data["severities"] = severities
        if vuln_types: data["vuln_types"] = vuln_types
        if categories: data["categories"] = categories
        return self.s._post("/api/2.0/fo/qid/search_list/dynamic/", data=data).text

    def update_dynamic_list(self, id: str, title: Optional[str] = None, **kwargs) -> str:
        data = {"action": "update", "id": id}
        if title: data["title"] = title
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/qid/search_list/dynamic/", data=data).text

    def delete_dynamic_list(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/qid/search_list/dynamic/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # OPTION PROFILES (VM)
    # ------------------------------------------------------------------
    def list_option_profiles_vm(self, title: Optional[str] = None) -> str:
        params = {"action": "list"}
        if title: params["title"] = title
        return self.s._get("/api/2.0/fo/subscription/option_profile/vm/", params=params).text

    def create_option_profile_vm(self, title: str, scan_ports: str = "All",
                                 tcp_ports: Optional[str] = None,
                                 udp_ports: Optional[str] = None) -> str:
        data = {"action": "create", "title": title, "scan_ports": scan_ports}
        if tcp_ports: data["tcp_ports"] = tcp_ports
        if udp_ports: data["udp_ports"] = udp_ports
        return self.s._post("/api/2.0/fo/subscription/option_profile/vm/", data=data).text

    def update_option_profile_vm(self, id: str, title: Optional[str] = None, **kwargs) -> str:
        data = {"action": "edit", "id": id}
        if title: data["title"] = title
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/subscription/option_profile/vm/", data=data).text

    def delete_option_profile_vm(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/subscription/option_profile/vm/", data={"action": "delete", "id": id}).text

    # ------------------------------------------------------------------
    # OPTION PROFILES (PC)
    # ------------------------------------------------------------------
    def list_option_profiles_pc(self) -> str:
        return self.s._get("/api/2.0/fo/subscription/option_profile/compliance/", params={"action": "list"}).text

    def create_option_profile_pc(self, title: str, **kwargs) -> str:
        data = {"action": "create", "title": title}
        data.update(kwargs)
        return self.s._post("/api/2.0/fo/subscription/option_profile/compliance/", data=data).text

    def delete_option_profile_pc(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/subscription/option_profile/compliance/", data={"action": "delete", "id": id}).text
