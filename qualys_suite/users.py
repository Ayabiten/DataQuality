"""
users.py - Qualys User Management Module
Covers: Users (list, create, edit, delete), roles, security settings.
API: /msp/user.php, /msp/role.php
"""
from .base import QualysSession
from typing import Optional


class UsersModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # USERS
    # ------------------------------------------------------------------
    def list_users(self, login: Optional[str] = None) -> str:
        """List users in the subscription."""
        params = {"action": "list"}
        if login: params["login"] = login
        return self.s._get("/msp/user.php", params=params).text

    def create_user(self, login: str, first_name: str, last_name: str, email: str,
                    title: str = "user", phone: Optional[str] = None,
                    send_email: bool = True) -> str:
        data = {
            "action": "add",
            "login": login, "first_name": first_name, "last_name": last_name,
            "email": email, "title": title,
            "send_email": "1" if send_email else "0"
        }
        if phone: data["phone"] = phone
        return self.s._post("/msp/user.php", data=data).text

    def edit_user(self, login: str, **kwargs) -> str:
        """Edit user properties. Pass keyword args matching API field names."""
        data = {"action": "edit", "login": login}
        data.update(kwargs)
        return self.s._post("/msp/user.php", data=data).text

    def delete_user(self, login: str) -> str:
        return self.s._post("/msp/user.php", data={"action": "delete", "login": login}).text

    def reset_password(self, login: str) -> str:
        return self.s._post("/msp/user.php", data={"action": "reset_password", "login": login}).text

    # ------------------------------------------------------------------
    # ROLES
    # ------------------------------------------------------------------
    def list_roles(self) -> str:
        return self.s._get("/msp/role.php", params={"action": "list"}).text

    def create_role(self, name: str, desc: Optional[str] = None) -> str:
        data = {"action": "add", "name": name}
        if desc: data["description"] = desc
        return self.s._post("/msp/role.php", data=data).text

    def edit_role(self, role_id: str, name: Optional[str] = None, desc: Optional[str] = None) -> str:
        data = {"action": "edit", "id": role_id}
        if name: data["name"] = name
        if desc: data["description"] = desc
        return self.s._post("/msp/role.php", data=data).text

    def delete_role(self, role_id: str) -> str:
        return self.s._post("/msp/role.php", data={"action": "delete", "id": role_id}).text

    # ------------------------------------------------------------------
    # BUSINESS UNITS / DIVISION MANAGEMENT
    # ------------------------------------------------------------------
    def list_division(self) -> str:
        return self.s._get("/msp/division.php", params={"action": "list"}).text

    def create_division(self, name: str) -> str:
        return self.s._post("/msp/division.php", data={"action": "add", "name": name}).text

    def delete_division(self, id: str) -> str:
        return self.s._post("/msp/division.php", data={"action": "delete", "id": id}).text
