from __future__ import annotations

from typing import Any
from enum import Enum
import requests
import time
from typing import Iterable
from hotglue_singer_sdk.streams.rest import RESTStream
from hotglue_singer_sdk.typing import AsyncJobStatus



class AsyncRESTStream(RESTStream):

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
        return decorated_request(prepared_request, context)

    def create_async_job(self, context: dict | None = None) -> dict:
        pass

    def get_async_job_status(self, job_id: Any) -> AsyncJobStatus:
        pass

    def get_async_job_results(self, job_id: Any) -> dict:
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
        job_id = self.create_async_job(context)
        polling_attempt = 0
        while True:
            job_status = self.get_async_job_status(job_id)
            if job_status == AsyncJobStatus.COMPLETED:
                break
            time.sleep(self.get_polling_interval_seconds(context, polling_attempt))
            polling_attempt += 1
        yield from self.generate_records_from_job_response(job_status['results'])
