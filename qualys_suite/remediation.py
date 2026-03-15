"""
remediation.py - Qualys Remediation & Ticketing Module
Covers: Remediation Tickets, Ignored Vulnerabilities, Ignored Hosts
API: /api/2.0/fo/ticket/, /api/2.0/fo/ignore_vuln/, /api/2.0/fo/ignore_host/
"""
from .base import QualysSession
from typing import Optional


class RemediationModule:
    def __init__(self, session: QualysSession):
        self.s = session

    # ------------------------------------------------------------------
    # REMEDIATION TICKETS
    # ------------------------------------------------------------------
    def list_tickets(self, since_ticket_number: Optional[str] = None,
                     status: Optional[str] = None,
                     assignee_login: Optional[str] = None,
                     qids: Optional[str] = None,
                     ips: Optional[str] = None,
                     show_full_output: bool = True) -> str:
        """
        status: OPEN, RESOLVED, CLOSED, IGNORED
        """
        params = {"action": "list", "show_full_output": str(int(show_full_output))}
        if since_ticket_number: params["since_ticket_number"] = since_ticket_number
        if status: params["status"] = status
        if assignee_login: params["assignee_login"] = assignee_login
        if qids: params["qids"] = qids
        if ips: params["ips"] = ips
        return self.s._get("/api/2.0/fo/ticket/", params=params).text

    def create_ticket(self, ips: str, qids: str, assignee_login: str,
                      resolution_date: Optional[str] = None,
                      comments: Optional[str] = None) -> str:
        data = {
            "action": "create", "ips": ips, "qids": qids,
            "assignee_login": assignee_login
        }
        if resolution_date: data["resolution_date"] = resolution_date
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/ticket/", data=data).text

    def update_ticket(self, ticket_number: str, assignee_login: Optional[str] = None,
                      status: Optional[str] = None, comments: Optional[str] = None) -> str:
        data = {"action": "update", "ticket_number": ticket_number}
        if assignee_login: data["assignee_login"] = assignee_login
        if status: data["change_status"] = status
        if comments: data["add_comment"] = comments
        return self.s._post("/api/2.0/fo/ticket/", data=data).text

    def delete_ticket(self, ticket_number: str) -> str:
        return self.s._post("/api/2.0/fo/ticket/", data={"action": "delete", "ticket_number": ticket_number}).text

    # ------------------------------------------------------------------
    # IGNORED VULNERABILITIES
    # ------------------------------------------------------------------
    def list_ignored_vulns(self, ips: Optional[str] = None, qids: Optional[str] = None) -> str:
        params = {"action": "list"}
        if ips: params["ips"] = ips
        if qids: params["qids"] = qids
        return self.s._get("/api/2.0/fo/ignore_vuln/", params=params).text

    def ignore_vuln(self, ips: str, qids: str, comments: Optional[str] = None) -> str:
        data = {"action": "ignore", "ips": ips, "qids": qids}
        if comments: data["comments"] = comments
        return self.s._post("/api/2.0/fo/ignore_vuln/", data=data).text

    def unignore_vuln(self, ips: str, qids: str) -> str:
        return self.s._post("/api/2.0/fo/ignore_vuln/", data={"action": "reopen", "ips": ips, "qids": qids}).text

    # ------------------------------------------------------------------
    # ACTIVITY LOGS
    # ------------------------------------------------------------------
    def list_activity_log(self, since_date: Optional[str] = None,
                          until_date: Optional[str] = None,
                          user_login: Optional[str] = None,
                          action: Optional[str] = None) -> str:
        params = {"action": "list"}
        if since_date: params["since_date"] = since_date
        if until_date: params["until_date"] = until_date
        if user_login: params["user_login"] = user_login
        if action: params["action_log"] = action
        return self.s._get("/api/2.0/fo/activity_log/", params=params).text

    # ------------------------------------------------------------------
    # DISTRIBUTION GROUPS (Notification Lists)
    # ------------------------------------------------------------------
    def list_distribution_groups(self) -> str:
        return self.s._get("/api/2.0/fo/group/", params={"action": "list"}).text

    def create_distribution_group(self, title: str, emails: str) -> str:
        return self.s._post("/api/2.0/fo/group/", data={"action": "add", "title": title, "delivery_email": emails}).text

    def update_distribution_group(self, id: str, title: Optional[str] = None, emails: Optional[str] = None) -> str:
        data = {"action": "edit", "id": id}
        if title: data["title"] = title
        if emails: data["delivery_email"] = emails
        return self.s._post("/api/2.0/fo/group/", data=data).text

    def delete_distribution_group(self, id: str) -> str:
        return self.s._post("/api/2.0/fo/group/", data={"action": "delete", "id": id}).text
