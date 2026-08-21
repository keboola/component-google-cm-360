"""Regression tests for the defensive error handling in GoogleCM360Client.

Each test class guards one defensive fix and asserts the same two things: the newly
handled failure now behaves better, and the success path plus every other failure behave
exactly as before.
"""

import json
import unittest
from unittest import mock

import httplib2
from googleapiclient.errors import HttpError
from keboola.component.exceptions import UserException

from google_cm360.client import GoogleCM360Client


def _http_error(status: int) -> HttpError:
    resp = httplib2.Response({"status": status})
    resp.reason = "Server Error"
    return HttpError(resp, b'{"error": {"message": "Unknown Error"}}')


class TestRunReportRetry(unittest.TestCase):
    def _make_client(self) -> GoogleCM360Client:
        # Bypass __init__ (which builds a real OAuth-backed Google service) and inject a mock.
        client = GoogleCM360Client.__new__(GoogleCM360Client)
        client.service = mock.MagicMock()
        return client

    @staticmethod
    def _execute_mock(client: GoogleCM360Client):
        return client.service.reports.return_value.run.return_value.execute

    @mock.patch("google_cm360.client.time.sleep", return_value=None)
    def test_success_executes_once_and_returns_result(self, mock_sleep):
        client = self._make_client()
        execute = self._execute_mock(client)
        execute.return_value = {"id": "file-xyz"}

        result = client.run_report(report_id="r1", profile_id="p1")

        self.assertEqual(result, {"id": "file-xyz"})
        self.assertEqual(execute.call_count, 1)
        mock_sleep.assert_not_called()

    @mock.patch("google_cm360.client.time.sleep", return_value=None)
    def test_retries_transient_500_then_succeeds(self, mock_sleep):
        client = self._make_client()
        execute = self._execute_mock(client)
        execute.side_effect = [_http_error(500), _http_error(500), {"id": "file-123"}]

        result = client.run_report(report_id="r1", profile_id="p1")

        self.assertEqual(result, {"id": "file-123"})
        self.assertEqual(execute.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @mock.patch("google_cm360.client.time.sleep", return_value=None)
    def test_reraises_after_exhausting_retries(self, mock_sleep):
        client = self._make_client()
        execute = self._execute_mock(client)
        execute.side_effect = _http_error(503)

        with self.assertRaises(HttpError):
            client.run_report(report_id="r1", profile_id="p1")

        self.assertEqual(execute.call_count, 5)

    @mock.patch("google_cm360.client.time.sleep", return_value=None)
    def test_non_transient_error_is_not_retried(self, mock_sleep):
        client = self._make_client()
        execute = self._execute_mock(client)
        execute.side_effect = _http_error(404)

        with self.assertRaises(HttpError):
            client.run_report(report_id="r1", profile_id="p1")

        self.assertEqual(execute.call_count, 1)
        mock_sleep.assert_not_called()


def _api_http_error(status: int, message: str) -> HttpError:
    """Build an HttpError whose ``reason`` is the message the CM360 API returned."""
    resp = httplib2.Response({"status": status})
    resp.reason = "Bad Request" if status == 400 else "Server Error"
    body = json.dumps({"error": {"message": message, "code": status}}).encode()
    return HttpError(resp, body)


DATE_RANGE_REJECTION = "You cannot save or run a report for a date over 2 years in the past."


class TestReportSaveRejectedAsUserError(unittest.TestCase):
    """Regression tests for surfacing a CM360 HTTP 400 on report save as a UserException.

    Guards the defensive fix for the CM360 Reporting API rejecting a report definition
    (e.g. a date range starting more than two years in the past). Such a rejection used to
    propagate as a raw HttpError and kill the job with an opaque internal error (exit 2);
    it must now fail as a UserException (exit 1) carrying the API's own explanation, while
    the success path and every non-400 failure behave exactly as before.
    """

    def _make_client(self) -> GoogleCM360Client:
        # Bypass __init__ (which builds a real OAuth-backed Google service) and inject a mock.
        client = GoogleCM360Client.__new__(GoogleCM360Client)
        client.service = mock.MagicMock()
        return client

    @staticmethod
    def _update_execute(client: GoogleCM360Client):
        return client.service.reports.return_value.update.return_value.execute

    @staticmethod
    def _insert_execute(client: GoogleCM360Client):
        return client.service.reports.return_value.insert.return_value.execute

    # --- update_report -------------------------------------------------------------

    def test_update_report_success_is_unchanged(self):
        client = self._make_client()
        execute = self._update_execute(client)
        execute.return_value = {"id": "report-1"}

        result = client.update_report(report={"name": "r"}, report_id="report-1", profile_id="p1")

        self.assertEqual(result, {"id": "report-1"})
        self.assertEqual(execute.call_count, 1)

    def test_update_report_400_raises_user_exception_with_api_message(self):
        client = self._make_client()
        self._update_execute(client).side_effect = _api_http_error(400, DATE_RANGE_REJECTION)

        with self.assertRaises(UserException) as ctx:
            client.update_report(report={"name": "r"}, report_id="report-1", profile_id="p1")

        self.assertIn(DATE_RANGE_REJECTION, str(ctx.exception))
        self.assertIn("report-1", str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, HttpError)

    def test_update_report_non_400_error_still_propagates_raw(self):
        for status in (403, 404, 500, 503):
            with self.subTest(status=status):
                client = self._make_client()
                self._update_execute(client).side_effect = _api_http_error(status, "Unknown Error")

                with self.assertRaises(HttpError):
                    client.update_report(report={"name": "r"}, report_id="report-1", profile_id="p1")

    # --- create_report -------------------------------------------------------------

    def test_create_report_success_is_unchanged(self):
        client = self._make_client()
        execute = self._insert_execute(client)
        execute.return_value = {"id": "report-9"}

        result = client.create_report({"name": "r"}, profile_id="p1")

        self.assertEqual(result, {"id": "report-9"})
        self.assertEqual(execute.call_count, 1)

    def test_create_report_400_raises_user_exception_with_api_message(self):
        client = self._make_client()
        self._insert_execute(client).side_effect = _api_http_error(400, DATE_RANGE_REJECTION)

        with self.assertRaises(UserException) as ctx:
            client.create_report({"name": "r"}, profile_id="p1")

        self.assertIn(DATE_RANGE_REJECTION, str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, HttpError)

    def test_create_report_non_400_error_still_propagates_raw(self):
        for status in (403, 404, 500, 503):
            with self.subTest(status=status):
                client = self._make_client()
                self._insert_execute(client).side_effect = _api_http_error(status, "Unknown Error")

                with self.assertRaises(HttpError):
                    client.create_report({"name": "r"}, profile_id="p1")


if __name__ == "__main__":
    unittest.main()
