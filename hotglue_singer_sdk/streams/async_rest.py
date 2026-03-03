from __future__ import annotations

import concurrent.futures
import time
from typing import Any, Iterable

import requests

from hotglue_singer_sdk.streams.rest import RESTStream
from hotglue_singer_sdk.typing import AsyncJobStatus


class AsyncRESTStream(RESTStream):

    parallelization_limit: int = 1

    def _get_records_for_window(self, window_context: dict) -> list[dict]:
        """Run one async export job for a single paging window context."""
        job_metadata = self.create_async_job(window_context)
        polling_attempt = 0
        while True:
            job_status = self.get_async_job_status(job_metadata)
            if job_status == AsyncJobStatus.COMPLETED:
                break
            time.sleep(self.get_polling_interval_seconds(window_context, polling_attempt))
            polling_attempt += 1

        job_response = self.get_async_job_results(job_metadata)
        records = []
        for record in self.generate_records_from_job_response(job_response):
            transformed_record = self.post_process(record, window_context)
            if transformed_record is not None:
                records.append(transformed_record)
        return records

    def make_request(
        self,
        method: str,
        url: str,
        context: dict | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
    ) -> requests.Response:
        """Prepare and send an HTTP request using RESTStream retry behavior."""
        prepared_request = self.build_prepared_request(
            method=method,
            url=url,
            params=params or {},
            headers=headers or self.http_headers,
            json=json,
            data=data,
        )
        decorated_request = self.request_decorator(self._request)
        response = decorated_request(prepared_request, context)
        return response

    def create_async_job(self, context: dict | None = None) -> dict:
        pass

    def get_async_job_status(self, job_metadata: dict) -> AsyncJobStatus:
        pass

    def get_async_job_results(self, job_metadata: dict) -> dict:
        pass

    def generate_records_from_job_response(self, job_response: dict) -> Iterable[dict]:
        pass

    def get_polling_interval_seconds(
        self,
        context: dict | None,
        polling_attempt: int
    ) -> int:
        return 5

    def get_records(self, context: dict | None) -> Iterable[dict]:
        context = context or {}
        paging_windows = self.get_paging_windows(context) or [{}]
        window_contexts = []
        for paging_window in paging_windows:
            window_context = context.copy()
            window_context.update(paging_window)
            window_contexts.append(window_context)

        max_workers = min(len(window_contexts), max(1, self.parallelization_limit))

        if max_workers <= 1:
            for window_context in window_contexts:
                for record in self._get_records_for_window(window_context):
                    yield record
            return

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:
            futures = [
                executor.submit(self._get_records_for_window, window_context)
                for window_context in window_contexts
            ]
            for future in futures:
                for record in future.result():
                    yield record
