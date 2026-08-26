"""WoocommerceSink target sink class, which handles writing streams."""

import copy
import hashlib
import json
import os
from abc import abstractmethod
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from hotglue_singer_sdk.target_sdk.rest import Rest
from hotglue_singer_sdk.target_sdk.auth import Authenticator
from hotglue_singer_sdk.target_sdk.common import HGJSONEncoder
from hotglue_singer_sdk.plugin_base import PluginBase
from hotglue_singer_sdk.sinks import RecordSink, BatchSink
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError

class HotglueBaseSink(Rest):
    summary_init = False
    TARGET_STATE_FIELD_VALUES_CONTEXT_KEY = "_target_state_field_values"
    supports_target_state_fields = True
    # include any stream names if externalId needs to be passed in the payload
    allows_externalid = []
    previous_state = None
    processed_hashes = []

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def endpoint(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def base_url(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def unified_schema(self) -> BaseModel:
        raise NotImplementedError()

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        self._state = dict(target._state)
        self._target = target
        self._target_state_fields: List[str] = []
        self._target_state_include_hash = False
        super().__init__(target, stream_name, schema, key_properties)

    def configure_target_state_custom_data(self, x_hotglue: Optional[dict]) -> None:
        """Read ``x-hotglue`` SCHEMA metadata for bookmark ``customData`` enrichment.

        ``target_state_fields`` is only honored on per-record sinks (see
        ``supports_target_state_fields``); a batch sink configured with it will
        log a one-time warning and skip source-field capture. 
        ``target_state_include_hash`` works for any sink.
        """
        settings = x_hotglue if isinstance(x_hotglue, dict) else {}
        raw_fields = settings.get("target_state_fields")
        if isinstance(raw_fields, list):
            self._target_state_fields = [
                name for name in raw_fields if isinstance(name, str)
            ]
        else:
            self._target_state_fields = []
        self._target_state_include_hash = (
            settings.get("target_state_include_hash") is True
        )
        if self._target_state_fields and not self.supports_target_state_fields:
            self.logger.warning(
                f"Stream '{self.name}' configured target_state_fields "
                f"{self._target_state_fields}, but {type(self).__name__} does not "
                "support capturing source fields into bookmark customData for "
                "batch sinks. These fields will be ignored; "
                "target_state_include_hash is unaffected."
            )
            self._target_state_fields = []

    def custom_target_state_data_enabled(self) -> bool:
        """Return whether SCHEMA ``x-hotglue`` requests SDK enrichment of bookmark ``customData``."""
        return bool(self._target_state_fields or self._target_state_include_hash)

    def capture_target_state_field_values(self, record: dict) -> dict:
        """Capture configured ETL field values before preprocess can mutate them."""
        captured = {}
        for field_name in self._target_state_fields:
            if field_name not in record:
                continue
            value = record[field_name]
            if value is not None:
                captured[field_name] = copy.deepcopy(value)
        return captured

    def prepare_target_state_field_context(self, record: dict, context: dict) -> None:
        """Stash configured field values in context before external preprocess.

        Only meaningful for per-record sinks: batch sinks share one context
        across every record in the batch, so a value stashed here would be
        overwritten by the next record and is never read back in
        ``process_batch``.
        """
        if self._target_state_fields:
            context[self.TARGET_STATE_FIELD_VALUES_CONTEXT_KEY] = (
                self.capture_target_state_field_values(record)
            )

    def url(self, endpoint=None):
        if not endpoint:
            endpoint = self.endpoint
        return f"{self.base_url}{endpoint}"

    def validate_input(self, record: dict):
        raise NotImplementedError()

    def validate_output(self, mapping):
        return mapping

    def get_previous_state(self):
        if not self.previous_state:
            previous_state_path = self._target.incremental_target_state_path
            if os.path.exists(previous_state_path):
                with open(previous_state_path, "r") as f:
                    self.previous_state = json.load(f)
            else:
                self.previous_state = {}

        # remove failed records from the previous state so retrigger retries those records
        if self.previous_state:
            if not self.previous_state.get("bookmarks"):
                self.previous_state["bookmarks"] = {}
            if not self.previous_state.get("summary"):
                self.previous_state["summary"] = {}

            for stream in self.previous_state["bookmarks"]:
                self.previous_state["bookmarks"][stream] = [record for record in self.previous_state["bookmarks"][stream] if record.get("success")]
            for stream in self.previous_state["summary"]:
                self.previous_state["summary"][stream]["fail"] = 0
        return self.previous_state

    def init_state(self):
        # on first run, initialize state with the previous job state if it exists
        if self.previous_state is None:
            previous_state = self.get_previous_state()
            if previous_state:
                self._target._latest_state = previous_state
        
        # if previous state exists, add the hashes to the processed_hashes
        if self.previous_state:
            self.processed_hashes.extend([record["hash"] for record in self.previous_state.get("bookmarks", {}).get(self.name, []) if record.get("hash")])

        # get the full target state
        target_state = self._target._latest_state

        # If there is data for the stream name in target_state use that to initialize the state
        if target_state:
            if not self._state and target_state.get("bookmarks", {}).get(self.name) and target_state.get("summary", {}).get(self.name):
                self.latest_state = target_state
        # If not init sink state latest_state
        if not self.latest_state:
            self.latest_state = self._state or {"bookmarks": {}, "summary": {}}

        if self.name not in self.latest_state["bookmarks"]:
            if not self.latest_state["bookmarks"].get(self.name):
                self.latest_state["bookmarks"][self.name] = []

        if not self.summary_init:
            if not self.latest_state.get("summary"):
                self.latest_state["summary"] = {}
            if not self.latest_state["summary"].get(self.name):
                self.latest_state["summary"][self.name] = {"success": 0, "fail": 0, "existing": 0, "updated": 0}

            self.summary_init = True
    
    def error_to_string(self, error: Any):
        return str(error)
    
    def process_error_state(self, state: dict):
        # log full error
        self.logger.error(f"Error processing record of type {self.name}: {state.get('error')}")
        # clean error for state
        state["error"] = self.error_to_string(state.get("error"))
        return state

    def _enrich_custom_data(
        self, state: dict, snapshot_field_values: Optional[dict] = None
    ) -> None:
        """Merge ETL field values into ``customData``; target-provided values win."""
        custom_data = dict(snapshot_field_values or {})
        if self._target_state_include_hash:
            record_hash = state.get("hash")
            if record_hash is not None:
                custom_data["hash"] = record_hash
        target_custom_data = state.get("customData")
        if isinstance(target_custom_data, dict):
            custom_data.update(target_custom_data)
        if custom_data:
            state["customData"] = custom_data

    def update_state(
        self,
        state: dict,
        is_duplicate: bool = False,
        record: Optional[dict] = None,
        snapshot_field_values: Optional[dict] = None,
    ) -> None:
        if is_duplicate:
            self.logger.info(f"Record of type {self.name} already exists with id: {state.get('id')}")
            self.latest_state["summary"][self.name]["existing"] += 1

        elif not state.get("success", False):
            self.latest_state["summary"][self.name]["fail"] += 1
            self.process_error_state(state)
        elif state.get("is_updated", False):
            self.latest_state["summary"][self.name]["updated"] += 1
            state.pop("is_updated", None)
        else:
            self.latest_state["summary"][self.name]["success"] += 1
        
        # add the mapped record to the state if it exists and env var OUTPUT_MAPPED_RECORD is set to true
        if record and os.getenv("OUTPUT_MAPPED_RECORD", "false").lower() == "true":
            state["mapped_record"] = record

        if (
            not is_duplicate
            and state.get("success", False)
            and self.custom_target_state_data_enabled()
        ):
            self._enrich_custom_data(state, snapshot_field_values)

        self.latest_state["bookmarks"][self.name].append(state)

        # If "authenticator" exists and if it's an instance of "Authenticator" class,
        # update "self.latest_state" with the the "authenticator" state
        if self.authenticator and isinstance(self.authenticator, Authenticator):
            self.latest_state.update(self.authenticator.state)

    def build_record_hash(self, record: dict) -> str:
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def _get_error_classification_metadata(self, error: Exception) -> dict:
        if isinstance(error, InvalidCredentialsError):
            return {"hg_error_class": InvalidCredentialsError.__name__}
        if isinstance(error, InvalidPayloadError):
            return {"hg_error_class": InvalidPayloadError.__name__}
        return {}

    def _record_identifiers(self, record: Optional[dict]) -> dict:
        """Extract id and externalId fields from a record for state bookmarks."""
        if not record:
            return {}
        identifiers = {}
        if record.get("id") not in (None, ""):
            identifiers["id"] = str(record["id"])
        external_id_key = self._target.EXTERNAL_ID_KEY
        external_id = record.get(external_id_key) or record.get(external_id_key.lower())
        if external_id not in (None, ""):
            identifiers["externalId"] = str(external_id)
        return identifiers

    def _build_record_error_state(
        self,
        error: Exception,
        *,
        record: Optional[dict] = None,
        external_id: Optional[str] = None,
        record_hash: Optional[str] = None,
        identifiers: Optional[dict] = None,
    ) -> dict:
        """Build a complete failed-record state dict from an exception."""
        state = {
            "success": False,
            "error": str(error),
        }
        state.update(self._get_error_classification_metadata(error))
        state.update(self._record_identifiers(record))
        if identifiers:
            for key, value in identifiers.items():
                if value not in (None, ""):
                    state[key] = value
        if record_hash:
            state["hash"] = record_hash
        elif record:
            try:
                state["hash"] = self.build_record_hash(record)
            except Exception:
                pass
        if external_id and "externalId" not in state:
            state["externalId"] = str(external_id)
        return state


class HotglueSink(HotglueBaseSink, RecordSink):
    """Hotglue target sink class."""
    def upsert_record(self, record: dict, context: dict):
        response = self.request_api("POST", request_data=record)
        id = response.json().get("id")
        return id, response.ok, dict()

    def get_existing_state(self, hash: str):
        """
        Returns the existing state if it exists
        """
        states = self.latest_state["bookmarks"][self.name]

        existing_state = next((s for s in states if hash==s.get("hash") and s.get("success")), None)

        return existing_state

    @abstractmethod
    def preprocess_record(self, record: dict, context: dict) -> dict:
        raise NotImplementedError()

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if not self.latest_state:
            self.init_state()

        snapshot_field_values = context.pop(
            self.TARGET_STATE_FIELD_VALUES_CONTEXT_KEY, None
        )
        if snapshot_field_values is None and self._target_state_fields:
            snapshot_field_values = self.capture_target_state_field_values(record)
        id = None
        external_id = None
        state_updates = dict()
        external_id_key = self._target.EXTERNAL_ID_KEY

        try:
            if self.name not in self.allows_externalid and (record.get(external_id_key) or record.get(external_id_key.lower())):
                external_id = record.pop(external_id_key, None) or record.pop(external_id_key.lower(), None)

            record = self.preprocess_record(record, context)

            if record and external_id:
                record[self._target.EXTERNAL_ID_KEY] = external_id
        except Exception as e:
            self.logger.exception(f"Preprocess record error {str(e)}")
            self.update_state(
                self._build_record_error_state(
                    e,
                    record=record,
                    external_id=external_id,
                ),
                record=record,
            )
            return

        record_hash = self.build_record_hash(record)

        if record_hash in self.processed_hashes:
            self.logger.info(f"Record of type {self.name} already exists with hash: {record_hash}")
            return

        existing_state = self.get_existing_state(record_hash)

        if self.name in self.allows_externalid:
            external_id = record.get(external_id_key) or record.get(external_id_key.lower())
        else:
            external_id = record.pop(external_id_key, None) or record.pop(external_id_key.lower(), None)

        if existing_state:
            return self.update_state(
                existing_state,
                is_duplicate=True,
                record=record,
            )

        try:
            id, success, state_updates = self.upsert_record(record, context)
        except Exception as e:
            self.logger.exception(f"Upsert record error {str(e)}")
            self.update_state(
                self._build_record_error_state(
                    e,
                    record=record,
                    external_id=external_id,
                    record_hash=record_hash,
                ),
                record=record,
            )
            return

        if success:
            self.logger.info(f"{self.name} processed id: {id}")

        state = {"success": success, "hash": record_hash}

        if id:
            state["id"] = id

        if external_id:
            state["externalId"] = external_id

        # if is_duplicate is in state_updates, set is_duplicate to True
        is_duplicate = False
        if state_updates.pop("existing", False):
            is_duplicate = True

        if state_updates and isinstance(state_updates, dict):
            state = dict(state, **state_updates)

        self.update_state(
            state,
            is_duplicate=is_duplicate,
            record=record,
            snapshot_field_values=snapshot_field_values,
        )


class HotglueBatchSink(HotglueBaseSink, BatchSink):
    """Hotglue target sink class."""

    supports_target_state_fields = False

    def process_batch_record(self, record: dict, index: int) -> dict:
        return record

    @abstractmethod
    def make_batch_request(self, records: List[dict]):
        raise NotImplementedError()

    def handle_batch_response(self, response) -> dict:
        """
        This method should return a dict.
        It's recommended that you return a key named "state_updates".
        This key should be an array of all state updates
        """
        return dict()

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]
        staged_records = []
        error_states = []

        for index, raw_record in enumerate(raw_records):
            record_identifiers = self._record_identifiers(raw_record)
            try:
                normalized = self.process_batch_record(raw_record, index)
                staged_records.append((record_identifiers, normalized))
            except Exception as e:
                self.logger.exception("Batch record preprocess error %s", e)
                error_states.append(
                    self._build_record_error_state(
                        e,
                        record=raw_record,
                        identifiers=record_identifiers,
                    )
                )

        response = None
        batch_request_failed = False
        if staged_records:
            records = [record for _, record in staged_records]
            try:
                response = self.make_batch_request(records)
            except Exception as e:
                batch_request_failed = True
                self.logger.exception("Batch request error %s", e)
                for record_identifiers, normalized in staged_records:
                    error_states.append(
                        self._build_record_error_state(
                            e,
                            record=normalized,
                            identifiers=record_identifiers,
                        )
                    )

        try:
            if staged_records and not batch_request_failed:
                result = self.handle_batch_response(response)
                for state in result.get("state_updates", []):
                    self.update_state(state)
        finally:
            for state in error_states:
                self.update_state(state)
