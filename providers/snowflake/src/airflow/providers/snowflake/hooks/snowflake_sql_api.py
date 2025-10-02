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
import traceback
import uuid
import re  # <-- ADDED: Needed for account name cleaning logic
from datetime import timedelta
from pathlib import Path
from typing import Any

import aiohttp
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.sql_api_generate_jwt import JWTGenerator


class SnowflakeSqlApiHook(SnowflakeHook):
    """
    A client to interact with Snowflake using SQL API and submit multiple SQL statements in a single request.

    This Hook is optimized for both synchronous (Operator) and asynchronous (Trigger) execution
    to prevent the AsyncToSync deadlock by ensuring all connection lookups in the async path
    are run in a separate thread via asyncio.to_thread.
    """

    LIFETIME = timedelta(minutes=59)
    RENEWAL_DELTA = timedelta(minutes=54)

    def __init__(
        self,
        snowflake_conn_id: str,
        token_life_time: timedelta | float = LIFETIME,
        token_renewal_delta: timedelta | float = RENEWAL_DELTA,
        *args: Any,
        **kwargs: Any,
    ):
        self.snowflake_conn_id = snowflake_conn_id

        # Defensive type conversion for deserialized floats (from the Trigger)
        if isinstance(token_life_time, float):
            self.token_life_time = timedelta(seconds=token_life_time)
        else:
            self.token_life_time = token_life_time

        if isinstance(token_renewal_delta, float):
            self.token_renewal_delta = timedelta(seconds=token_renewal_delta)
        else:
            self.token_renewal_delta = token_renewal_delta

        # Call super().__init__ - this is the likely source of synchronous calls
        super().__init__(snowflake_conn_id, *args, **kwargs)
        self.private_key: Any = None

    @property
    def _get_cleaned_conn_params(self) -> dict[str, Any]:
        """
        Retrieves connection parameters and robustly cleans the 'account' field
        to prevent double domains/paths when reconstructing the full URL.
        """
        # This accesses the base hook's logic to retrieve the raw parameters
        conn_config = self._get_conn_params.copy()

        # NOTE: This property access is synchronous (used by Operator or via asyncio.to_thread).

        account_name = conn_config.get("account")
        if account_name:
            # 1. Strip scheme (https://)
            account_name = re.sub(r"^(https?://)", "", account_name, flags=re.IGNORECASE)
            # 2. Strip domain (.snowflakecomputing.com) and any paths (e.g., /api/v2/statements)
            account_name = re.sub(r"(\.snowflakecomputing\.com/.*|\.snowflakecomputing\.com)$", "",
                                  account_name, flags=re.IGNORECASE)
            # 3. Take only the first segment in case the user included a path like account/region
            account_name = account_name.split("/")[0].strip()

            # Update the cleaned account name in the copy
            conn_config["account"] = account_name

        return conn_config

    # --- Sync Helpers for Operator path ---

    def _get_account_identifier_from_config(self, conn_config: dict[str, Any]) -> str:
        """
        Helper to compute account identifier from conn_config for sync use.
        conn_config['account'] is now guaranteed to be clean.
        """
        account_identifier = f"https://{conn_config['account']}"

        if conn_config.get("region"):
            account_identifier += f".{conn_config['region']}"

        return account_identifier

    @property
    def account_identifier(self) -> str:
        """
        Returns snowflake account identifier. Used by synchronous Operator path.
        This access is allowed in the synchronous execution path (Operator).
        """
        # Ensure this property explicitly calls the synchronous lookup method with cleaning
        conn_config = self._get_cleaned_conn_params
        return self._get_account_identifier_from_config(conn_config)

    def _get_private_key_sync(self, conn_config: dict[str, Any]) -> None:
        """
        Get the private key from snowflake connection (Synchronous, blocking I/O/CPU).
        Requires the conn_config to be passed in to avoid internal self._get_conn_params calls.
        """
        conn = self.get_connection(self.snowflake_conn_id)

        private_key_file = conn.extra_dejson.get(
            "extra__snowflake__private_key_file"
        ) or conn.extra_dejson.get("private_key_file")
        private_key_content = conn.extra_dejson.get(
            "extra__snowflake__private_key_content"
        ) or conn.extra_dejson.get("private_key_content")

        private_key_pem = None
        if private_key_content and private_key_file:
            raise AirflowException(
                "The private_key_file and private_key_content extra fields are mutually exclusive. "
                "Please remove one."
            )
        if private_key_file:
            private_key_pem = Path(private_key_file).read_bytes()
        elif private_key_content:
            private_key_pem = private_key_content.encode()

        if private_key_pem:
            passphrase = None
            if conn.password:
                passphrase = conn.password.strip().encode()

            self.private_key = serialization.load_pem_private_key(
                private_key_pem, password=passphrase, backend=default_backend()
            )

    # NEW HELPER: Synchronous retrieval of connection parameters (required for asyncio.to_thread)
    def _get_conn_config_sync(self) -> dict[str, Any]:
        """Helper to retrieve the connection parameters synchronously for the async path."""
        # This now uses the new property that cleans the account name
        return self._get_cleaned_conn_params

    # --- Asynchronous Helpers for Trigger path ---

    async def _get_private_key_async(self, conn_config: dict[str, Any]) -> None:
        """Run synchronous private key loading in a separate thread for the async path."""
        if not self.private_key:
            # Use asyncio.to_thread to run the blocking function without freezing the event loop
            await asyncio.to_thread(self._get_private_key_sync, conn_config)

    # --- Synchronous Authentication Methods (requests - Used by Operator) ---

    def get_oauth_token(self) -> str:
        """Generate temporary OAuth access token (Synchronous via requests)."""
        # Use the cleaned parameters
        conn_config = self._get_cleaned_conn_params
        url = f"{self.account_identifier}.snowflakecomputing.com/oauth/token-request"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": conn_config["refresh_token"],
            "redirect_uri": conn_config.get("redirect_uri", "https://localhost.com"),
        }
        response = requests.post(
            url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=HTTPBasicAuth(conn_config["client_id"], conn_config["client_secret"]),
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: no cover
            msg = f"Response: {e.response.content.decode()} Status Code: {e.response.status_code}"
            raise AirflowException(msg)
        return response.json()["access_token"]

    def get_headers(self) -> dict[str, Any]:
        """Form auth headers (Synchronous)."""
        # Use the cleaned parameters
        conn_config = self._get_cleaned_conn_params

        # OAuth path
        if all(
            [conn_config.get("refresh_token"), conn_config.get("client_id"), conn_config.get("client_secret")]
        ):
            self.log.info("DEBUG: Using synchronous OAuth path for headers.")
            oauth_token = self.get_oauth_token()
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {oauth_token}",
                "Accept": "application/json",
                "User-Agent": "snowflakeSQLAPI/1.0",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            }
            return headers

        # JWT path
        self.log.info("DEBUG: Using synchronous JWT path for headers.")
        # Pass conn_config to _get_private_key_sync to avoid internal self._get_conn_params calls
        if not self.private_key:
            self._get_private_key_sync(conn_config)

        token = JWTGenerator(
            conn_config["account"],
            conn_config["user"],
            private_key=self.private_key,
            lifetime=self.token_life_time,
            renewal_delay=self.token_renewal_delta,
        ).get_token()

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "snowflakeSQLAPI/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        return headers

    # --- Asynchronous Authentication Methods (aiohttp - Used by Trigger) ---

    async def get_oauth_token_async(self, conn_config: dict[str, Any]) -> str:
        """Generate temporary OAuth access token (Asynchronous via aiohttp)."""

        # Construct account identifier for URL using the awaited conn_config
        account_identifier = self._get_account_identifier_from_config(conn_config)

        # Need to parse the URL correctly: remove the 'https://' prefix for the base URL component
        base_url = account_identifier.replace("https://", "")
        url = f"{base_url}.snowflakecomputing.com/oauth/token-request"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": conn_config["refresh_token"],
            "redirect_uri": conn_config.get("redirect_uri", "https://localhost.com"),
        }

        auth = aiohttp.BasicAuth(conn_config["client_id"], conn_config["client_secret"])

        async with aiohttp.ClientSession(auth=auth) as session:
            async with session.post(
                url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as response:
                if response.status >= 400:
                    response_content = await response.text()
                    msg = f"Response: {response_content} Status Code: {response.status}"
                    raise AirflowException(msg)

                resp_json = await response.json()
                return resp_json["access_token"]

    async def get_headers_async(self) -> dict[str, Any]:
        """Form auth headers (Asynchronous)."""

        # CRITICAL FIX: Retrieve connection config non-blockingly (which includes cleaning)
        conn_config = await asyncio.to_thread(self._get_conn_config_sync)

        # OAuth path
        if all(
            [conn_config.get("refresh_token"), conn_config.get("client_id"), conn_config.get("client_secret")]
        ):
            self.log.info("DEBUG: Using asynchronous OAuth path for headers.")
            oauth_token = await self.get_oauth_token_async(conn_config)
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {oauth_token}",
                "Accept": "application/json",
                "User-Agent": "snowflakeSQLAPI/1.0",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            }
            return headers

        # JWT path
        self.log.info("DEBUG: Using asynchronous JWT path for headers.")

        # Ensure private key loading (blocking I/O) is run non-blockingly
        if not self.private_key:
            await self._get_private_key_async(conn_config)

        # Ensure JWT generation (blocking CPU) is run non-blockingly
        def generate_token_sync():
            return JWTGenerator(
                conn_config["account"],
                conn_config["user"],
                private_key=self.private_key,
                lifetime=self.token_life_time,
                renewal_delay=self.token_renewal_delta,
            ).get_token()

        token = await asyncio.to_thread(generate_token_sync)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "snowflakeSQLAPI/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        return headers

    # --- Asynchronous Header/Param Builder (Used by Trigger) ---

    async def get_request_url_header_params_async(self, query_id: str) -> tuple[
        dict[str, Any], dict[str, Any], str]:
        """
        Build request header, URL, and params (Asynchronous).
        """
        req_id = uuid.uuid4()
        # Await the async header getter
        header = await self.get_headers_async()
        params = {"requestId": str(req_id)}

        # Get config safely again to build the URL (already cleaned)
        conn_config = await asyncio.to_thread(self._get_conn_config_sync)
        account_identifier = self._get_account_identifier_from_config(conn_config)

        # The account_identifier now contains https://account.region, so we append the domain and path
        url = f"{account_identifier}.snowflakecomputing.com/api/v2/statements/{query_id}"

        return header, params, url

    # --- Core Execution Methods (Synchronous versions retained for Operator) ---

    def execute_query(
        self, sql: str, statement_count: int, query_tag: str = "", bindings: dict[str, Any] | None = None
    ) -> list[str]:
        """Synchronous API call to execute query."""
        # Use the cleaned parameters
        conn_config = self._get_cleaned_conn_params

        req_id = uuid.uuid4()
        # Use the property for the synchronous path (which uses cleaned params)
        url = f"{self.account_identifier}.snowflakecomputing.com/api/v2/statements"
        params: dict[str, Any] | None = {"requestId": str(req_id), "async": True, "pageSize": 10}
        headers = self.get_headers()
        sql_is_multi_stmt = ";" in sql.strip()
        if not isinstance(bindings, dict) and bindings is not None:
            raise AirflowException("Bindings should be a dictionary or None.")
        if bindings and sql_is_multi_stmt:
            self.log.warning(
                "Bindings are not supported for multi-statement queries. Bindings will be ignored."
            )
        bindings = bindings or {}
        data = {
            "statement": sql,
            "resultSetMetaData": {"format": "json"},
            "database": self.database or conn_config["database"],
            "schema": self.schema or conn_config["schema"],
            "warehouse": self.warehouse or conn_config["warehouse"],
            "role": self.role or conn_config["role"],
            "bindings": bindings,
            "parameters": {
                "MULTI_STATEMENT_COUNT": statement_count,
                "query_tag": query_tag,
            },
        }
        response = requests.post(url, json=data, headers=headers, params=params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: no cover
            msg = f"Response: {e.response.content.decode()} Status Code: {e.response.status_code}"
            raise AirflowException(msg)
        json_response = response.json()
        self.log.info("Snowflake SQL POST API response: %s", json_response)
        if "statementHandles" in json_response:
            self.query_ids = json_response["statementHandles"]
        elif "statementHandle" in json_response:
            self.query_ids.append(json_response["statementHandle"])
        else:
            raise AirflowException("No statementHandle/statementHandles present in response")
        return self.query_ids

    def get_request_url_header_params(self, query_id: str) -> tuple[dict[str, Any], dict[str, Any], str]:
        """Synchronous builder for request header, URL, and params."""
        req_id = uuid.uuid4()
        header = self.get_headers()
        params = {"requestId": str(req_id)}
        url = f"{self.account_identifier}.snowflakecomputing.com/api/v2/statements/{query_id}"
        return header, params, url

    def check_query_output(self, query_ids: list[str]) -> None:
        """Synchronous API call to check query output."""
        for query_id in query_ids:
            header, params, url = self.get_request_url_header_params(query_id)
            try:
                response = requests.get(url, headers=header, params=params)
                response.raise_for_status()
                self.log.info(response.json())
            except requests.exceptions.HTTPError as e:
                msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
                raise AirflowException(msg)

    def _process_response(self, status_code, resp):
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
        return {"status": "error", "message": resp["message"]}

    def get_sql_api_query_status(self, query_id: str) -> dict[str, str | list[str]]:
        """Synchronous API call to get query status."""
        self.log.info("DEBUG: Entering synchronous query status check path.")
        self.log.info("Retrieving status for query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id)
        response = requests.get(url, params=params, headers=header)
        status_code = response.status_code
        resp = response.json()
        return self._process_response(status_code, resp)

    # --- Core Asynchronous Execution Method (Used by Trigger) ---

    async def get_sql_api_query_status_async(self, query_id: str) -> dict[str, str | list[str]]:
        """Asynchronous API call to get query status."""
        self.log.info("DEBUG: Entering ASYNCHRONOUS query status check path.")
        self.log.info("Retrieving status for query id %s", query_id)

        try:
            # Call the fully asynchronous header/param builder here
            header, params, url = await self.get_request_url_header_params_async(query_id)

            async with (
                aiohttp.ClientSession(headers=header) as session,
                session.get(url, params=params) as response,
            ):
                status_code = response.status
                resp = await response.json()
                return self._process_response(status_code, resp)
        except Exception as e:
            self.log.error("FATAL ASYNC ERROR: Failed to execute non-blocking call in Trigger.")
            self.log.error(f"Error Type: {type(e).__name__}, Message: {e}")
            self.log.error("Traceback:\n%s", traceback.format_exc())
            raise

