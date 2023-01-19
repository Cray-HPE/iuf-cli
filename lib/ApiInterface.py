import requests
import lib.Auth
import os

class ApiInterface(object):
    def __init__(self, apiurl="https://api-gw-service-nmn.local/apis", resource="/iuf/v1"):
        self.auth = lib.Auth.Auth()
        self.apiurl = os.getenv("IUF_API_URL", apiurl)
        self.resource = os.getenv("IUF_API_URL_RESOURCE", resource)

    def activity_exists(self, activity):
        try:
            self.get_activity(activity)
            return True
        except:
            return False

    def request(self, method, path, payload=None):
        method = method.upper()
        assert method in ['GET', 'HEAD', 'DELETE', 'POST', 'PUT',
                          'PATCH', 'OPTIONS']

        url=self.apiurl + self.resource + path

        headers = dict()
        try:
            token = self.auth.token
            headers["Authorization"] = f"Bearer {token}"
        except:
            if "gw-service" in self.apiurl:
                raise
            else:
                # if we're not using the "official" api and don't get a token just try without it.  Mostly for local testing.
                pass

        method_func = method.lower()
        try:
            if payload:
                result = getattr(requests, method_func)(url, headers=headers, json=payload, verify=False)
            else:
                result = getattr(requests, method_func)(url, headers=headers, verify=False)
        except:
            raise

        # throw an exception for bad status codes
        result.raise_for_status()

        return result

    def get_activity(self, activity):
        api_path = f"/activities/{activity}"

        try:
            api_response = self.request("GET", api_path)
            return api_response
        except:
            raise
    
    def post_activity(self, payload):
        api_path = "/activities"

        try:
            api_response = self.request("POST", api_path, payload)
            return api_response
        except:
            raise

    def patch_activity(self, activity, payload):
        api_path = f"/activities/{activity}"

        try:
            api_response = self.request("PATCH", api_path, payload)
            return api_response
        except:
            raise
    
    def post_activity_history_run(self, activity, payload):
        api_path = f"/activities/{activity}/history/run"

        try:
            api_response = self.request("POST", api_path, payload)
            return api_response
        except:
            raise

    def get_activity_sessions(self, activity):
        api_path = f"/activities/{activity}/sessions"
        try:
            api_response = self.request("GET", api_path)
            return api_response
        except:
            raise

    def get_activity_session(self, activity, session):
        api_path = f"/activities/{activity}/sessions/{session}"
        try:
            api_response = self.request("GET", api_path)
            return api_response
        except:
            raise
        
