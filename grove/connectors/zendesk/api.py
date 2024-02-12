# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk API client.

As the Python Zendesk client does not currently support Audit Log API, 
this client has been created in the interim.
"""

import base64
import logging
import time
from typing import Any, Dict, Optional

import requests
from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://{identity}.zendesk.com"


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        username: Optional[str] = None,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param identity: The name of the Zendesk organisation.
        "param username: The Zendesk Username used for Auth.
        :param token: The Zendesk API token.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        # set basic auth value for authorisation header
        basic_auth = str(
            base64.b64encode(bytes(f"{username}/token:{token}", "utf-8")),
            "utf-8",
        )
        self.logger = logging.getLogger(__name__)
        self.retry = retry
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {basic_auth}",
        }

        # We need to push the identity into the URI, so we'll keep track of this.
        self._api_base_uri = API_BASE_URI.format(identity=identity)

    def _get(
        self, url: str, params: Optional[Dict[str, Optional[Any]]] = None
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        :param url: A URL to perform the HTTP GET against.
        :param headers: A dictionary of headers to add to the request.
        :param parameters: An optional set of HTTP parameters to add to the request.

        :return: HTTP Response object containing the headers and body of a response.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.
        """
        while True:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as err:
                # Retry on rate-limit, but only if requested.
                if err.response.status_code == 429:
                    self.logger.warning("Rate-limit was exceeded during request")
                    if self.retry:
                        time.sleep(int(err.response.headers.get("Retry-After", "1")))
                        continue
                    else:
                        raise RateLimitException(err) from err

                raise RequestFailedException(err) from err

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_audit_logs(
        self,
        cursor: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs.

        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param created_at: Filter audit logs by the time of creation.
            The API looks for a query with a range.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        url = f"{self._api_base_uri}/api/v2/audit_logs"

        # Use the cursor URL if set, otherwise construct the initial query.
        if cursor is not None:
            self.logger.debug(
                "Collecting next page with provided cursor", extra={"cursor": cursor}
            )
            result = self._get(cursor)
        else:
            self.logger.debug(
                "Collecting first page with provided",
                extra={"from_date": from_date, "to_date": to_date},
            )
            result = self._get(
                url,
                params={
                    "filter[created_at][]": from_date,
                    "filter[created_at][]": to_date,
                },
            )

        # Track the results
        cursor = result.body.get("next_page", None)
        if cursor == "":
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("audit_logs", []))
