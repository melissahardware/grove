# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Zendesk Audit Log collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses
from grove.connectors.zendesk.audit_logs import Connector
from grove.models import ConnectorConfig
from tests import mocks


class ZendeskAuditLogTestCase(unittest.TestCase):
    """Implements unit tests for the Zendesk Audit Logs collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="orgname",
                key="token",
                username="username",
                name="test",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_rate_limit(self):
        """Ensure rate-limit retires are working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=429,
            content_type="application/json",
            body=bytes(),
            headers={
                "Retry-After": "66",
            },
        )

        # Succeed on the second.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/slack/audit/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # Ensure time.sleep is called with the correct value in response to a
        # rate-limit.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_with(66)

    @responses.activate
    def test_collect_pagination(self):
        # first page has a cursor to the next page
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zendesk/audit_logs/001.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # The last page returns no cursor.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zendesk/audit_logs/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 4)
        self.assertEqual(self.connector.pointer, "2023-12-07T17:50:08Z")

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        # add one entry result with no cursor
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zendesk/audit_logs/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2023-12-07T17:50:08Z")
