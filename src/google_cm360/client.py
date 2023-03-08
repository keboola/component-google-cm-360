from google_auth_oauthlib.flow import Flow
from googleapiclient import discovery
from keboola.component.exceptions import UserException


class GoogleDV360ClientException(UserException):
    pass


class GoogleCM360Client:
    def __init__(self, oauth_credentials):
        self.service = None
        token_response = oauth_credentials.data
        token_response['expires_at'] = 22222
        client_secrets = {
            "web": {
                "client_id": oauth_credentials.appKey,
                "client_secret": oauth_credentials.appSecret,
                "redirect_uris": ["https://www.example.com/oauth2callback"],
                "auth_uri": "https://oauth2.googleapis.com/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            }
        }
        scopes = ['https://www.googleapis.com/auth/dfareporting']
        credentials = Flow.from_client_config(client_secrets, scopes=scopes, token=token_response).credentials
        discovery_url = 'https://dfareporting.googleapis.com/$discovery/rest?version=v4'
        # Build the API service.
        self.service = discovery.build(
            'dfareporting', 'v4',
            discoveryServiceUrl=discovery_url,
            credentials=credentials)

    def list_profiles_test(self) -> list[(str, str)]:
        request = self.service.userProfiles().list()
        # Execute request and print response.
        response = request.execute()

        for profile in response['items']:
            print('Found user profile with ID %s and user name "%s".' %
                  (profile['profileId'], profile['userName']))
