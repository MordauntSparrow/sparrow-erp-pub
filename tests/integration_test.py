#!/usr/bin/env python3
"""Integration smoke tests with optional authentication.

Set environment variables:
  BASE_URL (default http://localhost:5000)
  LOGIN_PATH (default /login)
  TEST_USER, TEST_PASS (optional) — if set the script will attempt to log in

Usage:
  python tests/integration_test.py
"""
import os
import re
import sys
import requests


def get_csrf_token(html):
    m = re.search(
        r"name=[\'\"]csrf_token[\'\"] value=[\'\"]([^\'\"]+)[\'\"]", html)
    if m:
        return m.group(1)
    # try alternate common names
    m = re.search(
        r"name=[\'\"]_csrf_token[\'\"] value=[\'\"]([^\'\"]+)[\'\"]", html)
    if m:
        return m.group(1)
    return None


def main():
    base = os.environ.get('BASE_URL', 'http://localhost:5000').rstrip('/')
    login_path = os.environ.get('LOGIN_PATH', '/login')
    test_user = os.environ.get('TEST_USER')
    test_pass = os.environ.get('TEST_PASS')

    s = requests.Session()

    print('Checking landing page...')
    r = s.get(base + '/plugin/ventus_response_module/', timeout=6)
    print('landing ->', r.status_code)

    print('Checking Socket.IO client asset...')
    r = s.get(base + '/socket.io/socket.io.js', timeout=6)
    print('socket.io asset ->', r.status_code)

    if test_user and test_pass:
        print('Attempting login...')
        r = s.get(base + login_path, timeout=6)
        token = get_csrf_token(r.text) if r.status_code == 200 else None
        payload = {'username': test_user, 'password': test_pass}
        if token:
            payload['csrf_token'] = token
        r2 = s.post(base + login_path, data=payload,
                    timeout=6, allow_redirects=True)
        print('login ->', r2.status_code)
        if r2.status_code >= 400:
            print('Login failed; aborting protected endpoint checks')
            sys.exit(3)
        # Try protected endpoint
        r3 = s.get(base + '/plugin/ventus_response_module/jobs', timeout=6)
        print('/jobs ->', r3.status_code)
        if r3.status_code != 200:
            print('Unexpected /jobs response after login')
            sys.exit(4)
    else:
        print('TEST_USER/TEST_PASS not set — skipping authenticated checks')

    print('\nIntegration smoke tests completed successfully')


if __name__ == '__main__':
    main()
