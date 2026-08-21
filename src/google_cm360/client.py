# import http
import io
import logging
import time
from datetime import datetime

from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import Flow
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from keboola.component.exceptions import UserException


class GoogleDV360ClientException(UserException):
    pass


# HTTP statuses worth retrying: transient server-side errors and rate limiting.
# Any other status (e.g. 401/403/404) is re-raised immediately so those failures
# keep failing exactly as before.
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
# Total attempts, i.e. the initial call plus (MAX_API_ATTEMPTS - 1) retries.
MAX_API_ATTEMPTS = 5


def _user_error_for_rejected_report(ex: HttpError, action: str) -> UserException | None:
    """Map an HTTP 400 from the CM360 API onto a ``UserException`` the customer can act on.

    The CM360 Reporting API answers an unacceptable report definition with HTTP 400 and a
    self-explanatory message (e.g. "You cannot save or run a report for a date over 2 years
    in the past."). Without this mapping that message never reaches the user: the raw
    ``HttpError`` propagates to the entrypoint's generic handler and the job dies with an
    opaque internal error (exit 2).

    Returns ``None`` for any other status so the caller re-raises it untouched -- only the
    exit code and the message of an already-failing job change, never whether it fails.
    """
    if getattr(ex.resp, "status", None) != 400:
        return None
    return UserException(
        f"Campaign Manager 360 rejected the report definition while {action}: {ex.reason} "
        "Please review the report configuration (for example the selected date range) and run the job again."
    )


def _execute_with_retry(request_factory, attempts: int = MAX_API_ATTEMPTS):
    """Execute a googleapiclient request, retrying only transient HTTP errors.

    ``request_factory`` is a no-argument callable that builds a fresh request; it is
    invoked once per attempt. Behaviour on requests that already succeeded is unchanged:
    the request is executed once and its result returned. Only a transient server-side
    error (HTTP 429/5xx) triggers a bounded exponential backoff; any other ``HttpError``
    is re-raised immediately, and a still-failing transient error is re-raised after the
    final attempt so the job still fails loudly.
    """
    delay = 2
    for attempt in range(1, attempts + 1):
        try:
            return request_factory().execute()
        except HttpError as ex:
            status = getattr(ex.resp, "status", None)
            if status not in RETRYABLE_HTTP_STATUSES or attempt == attempts:
                raise
            logging.warning(
                f"Transient error from CM360 API (HTTP {status}); retry {attempt}/{attempts - 1} in {delay}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)


class GoogleCM360Client:
    def __init__(self, client_id: str, app_secret: str, token_data: dict, scopes: list):
        self.service = None
        token_response = token_data
        token_response["expires_at"] = 22222
        client_secrets = {
            "web": {
                "client_id": client_id,
                "client_secret": app_secret,
                "redirect_uris": ["https://www.example.com/oauth2callback"],
                "auth_uri": "https://oauth2.googleapis.com/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }

        credentials = Flow.from_client_config(client_secrets, scopes=scopes, token=token_response).credentials
        discovery_url = "https://dfareporting.googleapis.com/$discovery/rest?version=v5"
        # Build the API service.
        self.service = discovery.build("dfareporting", "v5", discoveryServiceUrl=discovery_url, credentials=credentials)
        logging.info(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Google DV360 client initialized")

    def list_profiles(self) -> dict:
        """Call API to retrieve available profiles

        Returns: mapping of profileId -> userName

        """
        try:
            request = self.service.userProfiles().list()
            response = request.execute()
        except RefreshError as ex:
            raise UserException(
                "The OAuth token has expired or been revoked. Please reauthorize the application."
            ) from ex
        except HttpError as ex:
            raise UserException(f"Failed to load CM360 profiles: {ex.reason}") from ex
        items = response.get("items", [])
        if not items:
            raise UserException(
                "No Campaign Manager 360 profiles found for the authorized account. "
                "Please verify the account has access to at least one CM360 profile."
            )
        id_2_name = dict([(p["profileId"], p["userName"]) for p in items])
        return id_2_name

    def list_metadata(self, profile_id: str = None, endpoint_name: str = None):
        """Call API to retrieve available profiles

        Returns: mapping of profileId -> userName

        """
        try:
            next_page = None
            while True:
                request_args = {"profileId": profile_id}
                if next_page is not None:
                    request_args["pageToken"] = next_page

                response = getattr(self.service, endpoint_name)().list(**request_args).execute()

                if endpoint_name in response:
                    yield from response[endpoint_name]

                next_page = response.get("nextPageToken")
                if not next_page:
                    break

        except HttpError as ex:
            if ex.resp.status == 403:
                raise UserException(f"{ex.reason} Reauthorize the component to enable new scopes for listing metadata")

        except Exception as ex:
            logging.warning(f"Listing metadata for {endpoint_name}: {ex}")

    def list_reports(self, profile_id: str = None):
        if not profile_id:
            profiles_response = self.service.userProfiles().list().execute()
            items = profiles_response.get("items", [])
            if not items:
                raise UserException(
                    "No Campaign Manager 360 profiles found for the authorized account. "
                    "Please verify the account has access to at least one CM360 profile."
                )
            profile_id = items[0]["profileId"]

        request = self.service.reports().list(profileId=profile_id)
        response = request.execute()
        return response.get("items", [])

    def get_report(self, report_id: str, profile_id: str = None, ignore_error: bool = False):
        if not profile_id:
            profiles_response = self.service.userProfiles().list().execute()
            items = profiles_response.get("items", [])
            if not items:
                raise UserException(
                    "No Campaign Manager 360 profiles found for the authorized account. "
                    "Please verify the account has access to at least one CM360 profile."
                )
            profile_id = items[0]["profileId"]
        request = self.service.reports().get(profileId=profile_id, reportId=report_id)
        try:
            response = request.execute()
        except HttpError as ex:
            if ignore_error:
                return None
            else:
                raise UserException(f"Get report {report_id} for {profile_id}: {ex.reason}")
        return response

    def delete_report(self, report_id: str, profile_id: str, ignore_error: bool = False):
        request = self.service.reports().delete(profileId=profile_id, reportId=report_id)
        try:
            response = request.execute()
        except HttpError as ex:
            if ignore_error:
                return None
            else:
                raise UserException(f"Error deleting report {report_id} for {profile_id}: {ex.reason}")
        return response

    def patch_report(self, report: dict, report_id: str, profile_id: str):
        response = self.service.reports().delete(profileId=profile_id, reportId=report_id, body=report).execute()
        return response

    def update_report(self, report: dict, report_id: str, profile_id: str):
        try:
            response = self.service.reports().update(profileId=profile_id, reportId=report_id, body=report).execute()
        except HttpError as ex:
            user_error = _user_error_for_rejected_report(ex, f"updating report {report_id} in profile {profile_id}")
            if user_error is None:
                raise
            raise user_error from ex
        return response

    def list_compatible_fields(
        self,
        report_type: str = "STANDARD",
        compat_fields: str = "reportCompatibleFields",
        attribute: str = "dimensions",
        profile_id: str = None,
    ):
        if not profile_id:
            profiles_response = self.service.userProfiles().list().execute()
            items = profiles_response.get("items", [])
            if not items:
                raise UserException(
                    "No Campaign Manager 360 profiles found for the authorized account. "
                    "Please verify the account has access to at least one CM360 profile."
                )
            profile_id = items[0]["profileId"]

        request = self.service.reports().compatibleFields().query(profileId=profile_id, body={"type": report_type})
        response = request.execute()

        return [item["name"] for item in response[compat_fields][attribute]]

    def create_report(self, report: dict, profile_id: str = None):
        try:
            inserted_report = self.service.reports().insert(profileId=profile_id, body=report).execute()
        except HttpError as ex:
            user_error = _user_error_for_rejected_report(ex, f"creating a report in profile {profile_id}")
            if user_error is None:
                raise
            raise user_error from ex
        return inserted_report

    def run_report(self, report_id: str, profile_id: str):
        report_file = _execute_with_retry(lambda: self.service.reports().run(profileId=profile_id, reportId=report_id))
        return report_file

    def report_status(self, report_id: str, file_id: str):
        report_file = self.service.files().get(reportId=report_id, fileId=file_id).execute()
        return report_file

    def get_report_file(self, report_id: str, file_id: str, local_file_name: str):
        out_file = io.FileIO(local_file_name, mode="wb")
        request = self.service.files().get_media(reportId=report_id, fileId=file_id)
        CHUNK_SIZE = 8192
        downloader = MediaIoBaseDownload(out_file, request, chunksize=CHUNK_SIZE)
        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()
