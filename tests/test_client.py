"""Regression tests for the transient-error retry added to GoogleCM360Client.run_report.

Guards the defensive fix for the CM360 Reporting API returning HTTP 500 ("Unknown
Error") on ``reports().run()``: a transient server-side error must be retried, while the
success path and non-transient failures must behave exactly as before.
"""

import unittest
from unittest import mock

import httplib2
from googleapiclient.errors import HttpError

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


if __name__ == "__main__":
    unittest.main()
