# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk Audit log connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.zendesk.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    NAME = "zendesk_audit_logs"
    POINTER_PATH = "created_at"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    @property
    def username(self):
        """Fetches the Zendesk username from the configuration.

        This is required as Zendesk uses basic authorization.

        :return: The "username" portion of the connector's configuration.
        """
        try:
            return self.configuration.username
        except AttributeError:
            return None

    def collect(self):
        """Collects all audit logs from the Zendesk API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, a 7 day look-back of data will be collected.
        """
        client = Client(identity=self.identity, token=self.key, username=self.username)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to 7-days ago.
        now = datetime.utcnow().strftime(DATESTAMP_FORMAT)
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=1)).strftime(
                DATESTAMP_FORMAT
            )

        while True:
            log = client.get_audit_logs(
                from_date=self.pointer, to_date=now, cursor=cursor
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break
