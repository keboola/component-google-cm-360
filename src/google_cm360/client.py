# import http
import io

from google_auth_oauthlib.flow import Flow
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from keboola.component.exceptions import UserException


class GoogleDV360ClientException(UserException):
    pass


class GoogleCM360Client:
    def __init__(self, client_id: str, app_secret: str, token_data: dict):
        self.service = None
        token_response = token_data
        token_response['expires_at'] = 22222
        client_secrets = {
            "web": {
                "client_id": client_id,
                "client_secret": app_secret,
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

    def list_profiles(self) -> list[(str, str)]:
        """Call API to retrieve available profiles

        Returns: List of tuples - (profileId, userName)

        """
        request = self.service.userProfiles().list()
        response = request.execute()
        list_of_ids_names = [(p['profileId'], p['userName']) for p in response['items']]
        return list_of_ids_names

    def list_reports(self, profile_id: str = '8467304'):
        if not profile_id:
            profile_id = self.service.userProfiles().list().execute()['items'][0]['profileId']

        request = self.service.reports().list(profileId=profile_id)
        response = request.execute()
        return response

    def list_compatible_fields(self, report_type: str = "STANDARD", compat_fields: str = "reportCompatibleFields",
                               attribute: str = "dimensions", profile_id: str = None):
        if not profile_id:
            profile_id = self.service.userProfiles().list().execute()['items'][0]['profileId']

        request = self.service.reports().compatibleFields().query(profileId=profile_id, body={"type": report_type})
        response = request.execute()

        return [item['name'] for item in response[compat_fields][attribute]]

    def list_dimension_values(self, dimension_name: str, start_date: str, end_date: str, profile_id: str = None):
        body = {
            "dimensionName": dimension_name,
            # "filters": [
            #   {
            #     object (DimensionFilter)
            #   }
            # ],
            "startDate": start_date,  # "yyyy-MM-dd",
            "endDate": end_date  # "yyyy-MM-dd"
        }
        # doc: https://developers.google.com/doubleclick-advertisers/rest/v4/dimensionValues/query
        response = self.service.dimensionValues().query(profileId=profile_id, body=body).execute()
        return response

    def create_report(self, report: dict, profile_id: str = None):
        inserted_report = self.service.reports().insert(profileId=profile_id, body=report).execute()
        return inserted_report

    def run_report(self, report_id: str, profile_id: str):
        report_file = self.service.reports().run(profileId=profile_id, reportId=report_id).execute()
        return report_file

    def report_status(self, report_id: str, file_id: str):
        report_file = self.service.files().get(reportId=report_id, fileId=file_id).execute()
        return report_file

    def get_report_file(self, report_id: str, file_id: str, report_file: dict = None):
        if not report_file:
            report_file = self.service.files().get(reportId=report_id, fileId=file_id).execute()
        out_file = io.FileIO('muj_report.csv', mode='wb')
        request = self.service.files().get_media(reportId=report_id, fileId=file_id)
        CHUNK_SIZE = 8192
        downloader = MediaIoBaseDownload(
            out_file, request, chunksize=CHUNK_SIZE)
        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()
