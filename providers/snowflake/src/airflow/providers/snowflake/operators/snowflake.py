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

import time
import logging
from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, SupportsAbs, cast, AsyncIterator

import aiohttp
import asyncio

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
    SQLIntervalCheckOperator,
    SQLValueCheckOperator,
)
from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
# We keep this import to satisfy the original file structure, but we won't use it for deferral
from airflow.providers.snowflake.triggers.snowflake_trigger import CustomSnowflakeSqlApiTrigger
from airflow.triggers.base import BaseTrigger, TriggerEvent  # Base classes needed

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context

# ----------------------------------------------------------------------
# 2. MODIFIED OPERATOR
# ----------------------------------------------------------------------

class SnowflakeSqlApiOperator(SQLExecuteQueryOperator):
    """
    Modified Snowflake SQL API Operator to fix the AsyncToSync error by pre-calculating
    authentication details on the Worker and passing them to a pure async Trigger.
    """

    LIFETIME = timedelta(minutes=59)
    RENEWAL_DELTA = timedelta(minutes=54)

    template_fields: Sequence[str] = tuple(
        set(SQLExecuteQueryOperator.template_fields) | {"snowflake_conn_id"}
    )
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict[str, Any] | None = None,
        poll_interval: int = 5,
        statement_count: int = 0,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        bindings: dict[str, Any] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        self.poll_interval = poll_interval
        self.statement_count = statement_count
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        self.bindings = bindings
        self.execute_async = False
        self.deferrable = deferrable
        if any([warehouse, database, role, schema, authenticator, session_parameters]):  # pragma: no cover
            hook_params = kwargs.pop("hook_params", {})  # pragma: no cover
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(conn_id=snowflake_conn_id, **kwargs)  # pragma: no cover

    def _get_deferred_auth_params(self, hook: SnowflakeSqlApiHook) -> tuple[str, dict[str, str]]:
        """
        Synchronously generates the URL and Authorization Headers on the Worker
        by calling the synchronous get_headers() method on the Hook.
        """
        conn = hook.get_connection(hook.snowflake_conn_id)

        try:
            # FIX 1: Call the synchronous method that generates the token for either JWT or OAuth
            full_headers = hook.get_headers()
            auth_header = full_headers.get("Authorization")

            if not auth_header:
                raise AirflowException(
                    "Authorization header not found after calling hook.get_headers(). "
                    "Check that your Snowflake connection is properly configured for Keypair or OAuth."
                )

            self.log.info("Successfully retrieved Authorization header using hook.get_headers().")

        except Exception as e:
            # Raise a specific error if authentication fails during deferral setup
            raise AirflowException(
                f"Failed to retrieve Authorization header synchronously from the hook via get_headers(). "
                f"Original error: {e}"
            )

        # 1. Build Base API URL
        # FIX 2: Use hook.account_identifier (which is available on your hook)
        # to construct the full base URL, avoiding the missing 'get_account_name()' method.
        # account_identifier returns the full URL base (e.g., 'https://account.region.snowflakecomputing.com')
        account_identifier = hook.account_identifier.rstrip("/")
        api_url = f"{account_identifier}.snowflakecomputing.com"


        # 2. Build the Auth Headers for the Trigger
        auth_headers = {
            "Authorization": auth_header,
            "Accept": "application/json",
            "Content-Type": "application/json",
            # Pass the currently used role, extracted from the hook or connection
            "sf-role": hook.role or conn.extra_dejson.get("role", ""),
        }

        return api_url, auth_headers

    def execute(self, context: Context) -> None:
        """
        Submits the query and defers using the SnowflakeSqlApiTrigger
        with pre-calculated headers if deferrable=True.
        """
        self.log.info("Executing: %s", self.sql)
        self._hook = SnowflakeSqlApiHook(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
            deferrable=self.deferrable,
            **self.hook_params,
        )
        self.query_ids = self._hook.execute_query(
            self.sql,  # type: ignore[arg-type]
            statement_count=self.statement_count,
            bindings=self.bindings,
        )
        self.log.info("List of query ids %s", self.query_ids)

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        succeeded_query_ids = []
        for query_id in self.query_ids:
            self.log.info("Retrieving status for query id %s", query_id)
            statement_status = self._hook.get_sql_api_query_status(query_id)
            if statement_status.get("status") == "running":
                break
            if statement_status.get("status") == "success":
                succeeded_query_ids.append(query_id)
            else:
                raise AirflowException(f"{statement_status.get('status')}: {statement_status.get('message')}")

        if len(self.query_ids) == len(succeeded_query_ids):
            self.log.info("%s completed successfully.", self.task_id)
            return

        if self.deferrable:
            self.log.info("Deferring task using CustomSnowflakeSqlApiTrigger to avoid AsyncToSync error.")

            # 1. Synchronously get auth parameters on the Worker
            api_url, auth_headers = self._get_deferred_auth_params(self._hook)

            # 2. Defer using the custom trigger and static parameters
            self.defer(
                timeout=self.execution_timeout,
                trigger=CustomSnowflakeSqlApiTrigger(
                    poll_interval=self.poll_interval,
                    query_ids=self.query_ids,
                    api_url=api_url,
                    auth_headers=auth_headers,
                ),
                method_name="execute_complete",
            )
        else:
            while True:
                statement_status = self.poll_on_queries()
                if statement_status["error"]:
                    raise AirflowException(statement_status["error"])
                if not statement_status["running"]:
                    break

            self._hook.check_query_output(self.query_ids)

    def poll_on_queries(self):
        """Poll on requested queries."""
        queries_in_progress = set(self.query_ids)
        statement_success_status = {}
        statement_error_status = {}
        statement_running_status = {}
        for query_id in self.query_ids:
            if not len(queries_in_progress):
                break
            self.log.info("checking : %s", query_id)
            try:
                statement_status = self._hook.get_sql_api_query_status(query_id)
            except Exception as e:
                raise ValueError({"status": "error", "message": str(e)})
            if statement_status.get("status") == "error":
                queries_in_progress.remove(query_id)
                statement_error_status[query_id] = statement_status
            if statement_status.get("status") == "success":
                statement_success_status[query_id] = statement_status
                queries_in_progress.remove(query_id)
            if statement_status.get("status") == "running":
                statement_running_status[query_id] = statement_status
            time.sleep(self.poll_interval)
        return {
            "success": statement_success_status,
            "error": statement_error_status,
            "running": statement_running_status,
        }

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> None:
        """
        Execute callback when the trigger fires; returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = f"{event['status']}: {event['message']}"
                raise AirflowException(msg)
            if "status" in event and event["status"] == "success":
                hook = SnowflakeSqlApiHook(snowflake_conn_id=self.snowflake_conn_id)
                query_ids = cast("list[str]", event["statement_query_ids"])
                hook.check_query_output(query_ids)
                self.log.info("%s completed successfully.", self.task_id)
        else:
            self.log.info("%s completed successfully.", self.task_id)


class SnowflakeCheckOperator(SQLCheckOperator):
    template_fields: Sequence[str] = tuple(set(SQLCheckOperator.template_fields) | {"snowflake_conn_id"})
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        sql: str,
        snowflake_conn_id: str = "snowflake_default",
        parameters: Iterable | Mapping[str, Any] | None = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        if any([warehouse, database, role, schema, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(sql=sql, parameters=parameters, conn_id=snowflake_conn_id, **kwargs)
        self.query_ids: list[str] = []


class SnowflakeValueCheckOperator(SQLValueCheckOperator):
    template_fields: Sequence[str] = tuple(set(SQLValueCheckOperator.template_fields) | {"snowflake_conn_id"})
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        snowflake_conn_id: str = "snowflake_default",
        parameters: Iterable | Mapping[str, Any] | None = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        if any([warehouse, database, role, schema, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(
            sql=sql, pass_value=pass_value, tolerance=tolerance, conn_id=snowflake_conn_id, **kwargs
        )
        self.query_ids: list[str] = []


class SnowflakeIntervalCheckOperator(SQLIntervalCheckOperator):
    template_fields: Sequence[str] = tuple(
        set(SQLIntervalCheckOperator.template_fields) | {"snowflake_conn_id"}
    )
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict,
        date_filter_column: str = "ds",
        days_back: SupportsAbs[int] = -7,
        snowflake_conn_id: str = "snowflake_default",
        parameters: Iterable | Mapping[str, Any] | None = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        if any([warehouse, database, role, schema, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(
            table=table,
            metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column,
            days_back=days_back,
            conn_id=snowflake_conn_id,
            **kwargs,
        )
        self.query_ids: list[str] = []









