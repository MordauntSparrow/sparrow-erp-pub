import unittest
from unittest.mock import patch

from app.public_base import (
    EMPLOYEE_PORTAL_PUBLIC_PATH,
    RECRUITMENT_VACANCIES_PATH,
    resolve_public_base_url,
)


class TestPublicPathConstants(unittest.TestCase):
    def test_recruitment_and_portal_paths(self):
        self.assertEqual(RECRUITMENT_VACANCIES_PATH, "/vacancies")
        self.assertEqual(EMPLOYEE_PORTAL_PUBLIC_PATH, "/employee-portal")


class TestResolvePublicBaseUrl(unittest.TestCase):
    def test_sparrow_url_wins(self):
        with patch.dict(
            "os.environ",
            {
                "SPARROW_PUBLIC_BASE_URL": "https://app.example.com/",
                "RAILWAY_PUBLIC_DOMAIN": "wrong.up.railway.app",
            },
            clear=False,
        ):
            self.assertEqual(resolve_public_base_url(), "https://app.example.com")

    def test_railway_domain_gets_https(self):
        with patch.dict(
            "os.environ",
            {"RAILWAY_PUBLIC_DOMAIN": "myapp-production.up.railway.app"},
            clear=False,
        ):
            self.assertEqual(
                resolve_public_base_url(),
                "https://myapp-production.up.railway.app",
            )

    def test_extra_key_first(self):
        with patch.dict(
            "os.environ",
            {
                "RECRUITMENT_PUBLIC_BASE_URL": "https://jobs.example.com",
                "SPARROW_PUBLIC_BASE_URL": "https://other.example.com",
            },
            clear=False,
        ):
            self.assertEqual(
                resolve_public_base_url(extra_env_keys=("RECRUITMENT_PUBLIC_BASE_URL",)),
                "https://jobs.example.com",
            )


if __name__ == "__main__":
    unittest.main()
