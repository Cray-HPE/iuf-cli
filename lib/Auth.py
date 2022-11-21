from kubernetes import client, config
import base64
from keycloak import KeycloakOpenID

import urllib3
urllib3.disable_warnings()

class AuthException(Exception):
    """A wrapper for raising an AuthException exception."""
    pass

class Auth():
    def __init__(self):
        self._token = None
    
    def get_secrets(self):
        try:
            config.load_kube_config()
            v1 = client.CoreV1Api()
            sec = v1.read_namespaced_secret("admin-client-auth", "default").data
            username = base64.b64decode(sec.get("client-id").strip()).decode('utf-8')
            password = base64.b64decode(sec.get("client-secret").strip()).decode('utf-8')
        except:
            raise AuthException("Unable to load secrets from Kubernetes")

        return username, password
    
    def get_token(self, username, password):
        try:
            keycloak_openid = KeycloakOpenID(server_url="https://api-gw-service-nmn.local/keycloak/",
                                    client_id=username,
                                    realm_name="shasta",
                                    client_secret_key=password,
                                    verify=False)

            token = keycloak_openid.token(grant_type="client_credentials")
        except:
            raise AuthException("Unable to obtain token from Keycloak")

        return token["access_token"]

    @property
    def token(self):
        if not self._token:
            username, password = self.get_secrets()
            self._token = self.get_token(username, password)

        return self._token


