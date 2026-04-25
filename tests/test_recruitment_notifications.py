"""Lightweight tests for recruitment notification helpers (no SMTP required)."""
import unittest
from unittest.mock import patch


class RecruitmentNotificationsTest(unittest.TestCase):
    def test_public_base_url_reads_env(self):
        from app.plugins.recruitment_module import notifications as n

        with patch.dict("os.environ", {"RECRUITMENT_PUBLIC_BASE_URL": "https://example.com/"}, clear=False):
            self.assertEqual(n._public_base_url(), "https://example.com")

    @patch("app.plugins.recruitment_module.notifications._get_email_manager")
    @patch("app.plugins.recruitment_module.notifications._fetch_application_row")
    def test_notify_stage_skips_when_no_row(self, mock_row, mock_em):
        from app.plugins.recruitment_module import notifications as n

        mock_row.return_value = None
        n.notify_applicant_stage_change(999, "interview")
        mock_em.return_value.send_email.assert_not_called()

    @patch("app.plugins.recruitment_module.notifications._send_applicant_email")
    @patch("app.plugins.recruitment_module.notifications._fetch_application_row")
    def test_notify_stage_sends_when_configured(self, mock_row, mock_send):
        from app.plugins.recruitment_module import notifications as n

        mock_row.return_value = {
            "applicant_email": "a@example.com",
            "applicant_name": "Alex",
            "opening_title": "Engineer",
        }
        n.notify_applicant_stage_change(1, "interview")
        mock_send.assert_called_once()
        args = mock_send.call_args[0]
        self.assertEqual(args[0], "a@example.com")
        self.assertIn("Interview", args[2])


if __name__ == "__main__":
    unittest.main()
