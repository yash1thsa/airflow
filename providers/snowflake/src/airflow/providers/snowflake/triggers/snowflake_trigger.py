# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import aiohttp
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from datetime import timedelta


class CustomSnowflakeSqlApiTrigger(BaseTrigger):
    """
    Custom Trigger for the SnowflakeSqlApiOperator.
    This trigger uses pre-calculated authentication headers and API URL,
    avoiding synchronous connection fetching/header generation in the Triggerer process.
    """

    def __init__(
        self,
        poll_interval: float,
        query_ids: list[str],
        api_url: str,
        auth_headers: dict[str, str],
        snowflake_conn_id: str = "snowflake_default",
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.query_ids = query_ids
        self.api_url = api_url  # Pre-calculated base URL (e.g., https://<account>.snowflakecomputing.com)
        self.auth_headers = auth_headers  # Pre-calculated Authorization header
        self.snowflake_conn_id = snowflake_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize CustomSnowflakeSqlApiTrigger arguments and classpath."""
        return (
            "airflow.providers.snowflake.triggers.snowflake_trigger.CustomSnowflakeSqlApiTrigger",
            {
                "poll_interval": self.poll_interval,
                "query_ids": self.query_ids,
                "api_url": self.api_url,
                "auth_headers": self.auth_headers,
                "snowflake_conn_id": self.snowflake_conn_id,
            },
        )

    async def _process_response(self, status_code: int, resp: dict[str, Any]) -> dict[str, Any]:
        """
        Processes the API response synchronously with the Hook's logic.
        Copied from Hook to make Trigger fully self-contained.
        """
        self.log.info("Snowflake SQL GET statements status API response: %s", resp)
        if status_code == 202:
            return {"status": "running", "message": "Query statements are still running"}
        if status_code == 422:
            return {"status": "error", "message": resp["message"]}
        if status_code == 200:
            if resp_statement_handles := resp.get("statementHandles"):
                statement_handles = resp_statement_handles
            elif resp_statement_handle := resp.get("statementHandle"):
                statement_handles = [resp_statement_handle]
            else:
                statement_handles = []
            return {
                "status": "success",
                "message": resp["message"],
                "statement_handles": statement_handles,
            }
        # Catch-all for unexpected status codes
        return {"status": "error", "message": resp.get("message", f"Unexpected status code {status_code}")}

    async def _get_query_status(self, query_id: str) -> dict[str, Any]:
        """Async method to check query status using pre-calculated auth."""

        # The full status URL includes the API path: /api/v2/statements/
        status_url = f"{self.api_url}/api/v2/statements/{query_id}"
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id)}

        self.log.info("Checking status for query ID %s at %s", query_id, status_url)

        # Use aiohttp directly with the provided headers
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(status_url, params=params, headers=self.auth_headers) as response:
                    status_code = response.status
                    try:
                        resp = await response.json()
                    except aiohttp.ContentTypeError:
                        self.log.error("API response was not valid JSON (Status: %s)", status_code)
                        resp = {"message": "Non-JSON response received from Snowflake API."}

                    return await self._process_response(status_code, resp)
            except Exception as e:
                self.log.error("API call failed for query %s: %s", query_id, e)
                return {"status": "error", "message": f"API request failed: {e}"}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait for the query the snowflake query to complete."""

        try:
            successful_query_ids: list[str] = []
            queries_to_poll = list(self.query_ids)

            while queries_to_poll:
                next_queries_to_poll: list[str] = []

                # Check status for all running queries in parallel
                tasks = [self._get_query_status(query_id) for query_id in queries_to_poll]
                statuses = await asyncio.gather(*tasks)

                for query_id, status in zip(queries_to_poll, statuses):
                    if status["status"] == "success":
                        successful_query_ids.append(query_id)
                    elif status["status"] == "error":
                        yield TriggerEvent(status)
                        return
                    else:  # status is "running" or unknown (keep polling)
                        next_queries_to_poll.append(query_id)

                queries_to_poll = next_queries_to_poll

                if not queries_to_poll:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "statement_query_ids": successful_query_ids,
                        }
                    )
                    return

                await asyncio.sleep(self.poll_interval)

        except Exception as e:
            self.log.exception("Exception occurred while polling Snowflake SQL API.")
            yield TriggerEvent({"status": "error", "message": str(e)})

