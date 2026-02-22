"""Tap abstract class."""

import abc
import copy
import json
import sys
import threading
import traceback
from enum import Enum
from pathlib import Path, PurePath
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast

import click

from hotglue_singer_sdk.cli import common_options
from hotglue_singer_sdk.exceptions import MaxRecordsLimitException
from hotglue_singer_sdk.helpers import _state
from hotglue_singer_sdk.helpers._classproperty import classproperty
from hotglue_singer_sdk.helpers._compat import final
from hotglue_singer_sdk.helpers._singer import Catalog
from hotglue_singer_sdk.helpers._state import write_stream_state
from hotglue_singer_sdk.helpers._util import read_json_file
from hotglue_singer_sdk.helpers.capabilities import (
    CapabilitiesEnum,
    PluginCapabilities,
    TapCapabilities,
)
from hotglue_singer_sdk.mapper import PluginMapper
from hotglue_singer_sdk.plugin_base import PluginBase
from hotglue_singer_sdk.streams import RESTStream, SQLStream, Stream
from hotglue_etl_exceptions import InvalidCredentialsError

STREAM_MAPS_CONFIG = "stream_maps"


class CliTestOptionValue(Enum):
    """Values for CLI option --test."""

    All = "all"
    Schema = "schema"
    Disabled = False


class Tap(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps.

    The Tap class governs configuration, validation, and stream discovery for tap
    plugins.
    """

    # Constructor

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, Catalog, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the tap.

        Args:
            config: Tap configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            catalog: Tap catalog. Can be a dictionary or a path to the catalog file.
            state: Tap state. Can be dictionary or a path to the state file.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        # Declare private members
        self._streams: Optional[Dict[str, Stream]] = None
        self._input_catalog: Optional[Catalog] = None
        self._state: Dict[str, Stream] = {}
        self._catalog: Optional[Catalog] = None  # Tap's working catalog
        self.config_file = config[0] if config else None

    def register_streams_from_catalog(self, catalog):
        if isinstance(catalog, Catalog):
            self._input_catalog = catalog
        elif isinstance(catalog, dict):
            self._input_catalog = Catalog.from_dict(catalog)
        elif catalog is not None:
            self._input_catalog = Catalog.from_dict(read_json_file(catalog))

        # Initialize mapper
        self.mapper: PluginMapper
        self.mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )

        self.mapper.register_raw_streams_from_catalog(self.catalog)


    def register_state_from_file(self, state):
        state_dict: dict = {}
        if isinstance(state, dict):
            state_dict = state
        elif state:
            state_dict = read_json_file(state)
        self.load_state(state_dict)


    # Class properties

    @property
    def streams(self) -> Dict[str, Stream]:
        """Get streams discovered or catalogued for this tap.

        Results will be cached after first execution.

        Returns:
            A mapping of names to streams, using discovery or a provided catalog.
        """
        input_catalog = self.input_catalog

        if self._streams is None:
            self._streams = {}
            for stream in self.load_streams():
                if input_catalog is not None:
                    stream.apply_catalog(input_catalog)
                self._streams[stream.name] = stream
        return self._streams

    @property
    def state(self) -> dict:
        """Get tap state.

        Returns:
            The tap's state dictionary

        Raises:
            RuntimeError: If state has not been initialized.
        """
        if self._state is None:
            raise RuntimeError("Could not read from uninitialized state.")
        return self._state

    @property
    def input_catalog(self) -> Optional[Catalog]:
        """Get the catalog passed to the tap.

        Returns:
            Catalog dictionary input, or None if not provided.
        """
        return self._input_catalog

    @property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Returns:
            A Singer catalog object.
        """
        if self._catalog is None:
            self._catalog = self.input_catalog or self._singer_catalog

        return self._catalog

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get tap capabilities.

        Returns:
            A list of capabilities supported by this tap.
        """
        capabilities = [
            TapCapabilities.CATALOG,
            TapCapabilities.STATE,
            TapCapabilities.DISCOVER,
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
            PluginCapabilities.HOTGLUE_EXCEPTIONS_CLASSES,
        ]

        if self.confirm_fetch_access_token_support():
            capabilities.append(PluginCapabilities.ALLOWS_FETCH_ACCESS_TOKEN)
        return capabilities

    # Connection test:

    @final
    def run_connection_test(self) -> bool:
        """Run connection test.

        Returns:
            True if the test succeeded.
        """
        for stream in self.streams.values():
            # Initialize streams' record limits before beginning the sync test.
            stream._MAX_RECORDS_LIMIT = 1

        for stream in self.streams.values():
            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' should be called by "
                    f"parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue
            try:
                stream.sync()
            except MaxRecordsLimitException:
                pass
        return True

    def _resolve_test_stream(self, stream_id: str) -> RESTStream:
        """Look up a stream by name or tap_stream_id and assert it is a REST stream.

        Args:
            stream_id: The name or tap_stream_id of the stream.

        Returns:
            The matching RESTStream instance.

        Raises:
            ValueError: If no stream matching stream_id is found.
            click.UsageError: If the stream is not a REST stream.
        """
        stream = self.streams.get(stream_id) or next(
            (s for s in self.streams.values() if s.tap_stream_id == stream_id),
            None,
        )
        if stream is None:
            raise ValueError(
                f"Stream '{stream_id}' was not found. "
                f"Available streams: {list(self.streams.keys())}"
            )
        if not isinstance(stream, RESTStream):
            raise click.UsageError(
                f"Stream '{stream_id}' is not a REST stream. "
                "Only REST streams are supported by --test-stream."
            )
        return stream

    @final
    def run_test_stream(
        self,
        stream_id: str,
        request_only: bool = False,
        pages: int = 1,
    ) -> bool:
        """Entry point for --test-stream. Routes to the appropriate sub-function.

        Args:
            stream_id: The name or tap_stream_id of the stream to test.
            request_only: If True, delegate to ``run_test_stream_request`` to return
                the raw HTTP response payload. Otherwise delegate to
                ``run_test_stream_pages``.
            pages: Number of pages to fetch. Passed through to
                ``run_test_stream_pages``; ignored when ``request_only`` is True.

        Returns:
            True if the operation completed successfully.
        """
        if request_only:
            return self.run_test_stream_request(stream_id)
        return self.run_test_stream_pages(stream_id, pages=pages)

    @final
    def run_test_stream_request(self, stream_id: str) -> bool:
        """Make a single raw HTTP request for a stream and print the response to STDOUT.

        Prints a JSON object containing ``status_code``, ``headers``, and ``body``.
        On a network-level error the object will contain an ``error`` key instead.

        Args:
            stream_id: The name or tap_stream_id of the stream to test.

        Returns:
            True if the method completed (regardless of HTTP status).
        """
        stream = self._resolve_test_stream(stream_id)
        prepared_request = stream.prepare_request(context=None, next_page_token=None)
        try:
            response = stream.requests_session.send(
                prepared_request, timeout=stream.timeout
            )
            try:
                body: Any = response.json()
            except Exception:
                body = response.text
            print(
                json.dumps(
                    {
                        "status_code": response.status_code,
                        "headers": dict(response.headers),
                        "body": body,
                    },
                    indent=2,
                    default=str,
                )
            )
        except Exception as exc:
            print(json.dumps({"error": str(exc)}, indent=2))
        return True

    @final
    def run_test_stream_pages(self, stream_id: str, pages: int = 1) -> bool:
        """Fetch one or more pages of parsed records for a stream and print to STDOUT.

        Uses the stream's own request machinery (authentication, backoff, parsing, and
        post-processing) and prints all collected records as a JSON array.

        Args:
            stream_id: The name or tap_stream_id of the stream to test.
            pages: Maximum number of pages to fetch (default 1).

        Returns:
            True if the request succeeded.
        """
        stream = self._resolve_test_stream(stream_id)
        decorated_request = stream.request_decorator(stream._request)
        next_page_token: Any = None
        all_records: List[dict] = []
        for _ in range(pages):
            prepared_request = stream.prepare_request(
                context=None, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, None)
            for raw_record in stream.parse_response(resp):
                processed = stream.post_process(raw_record, None)
                if processed is not None:
                    all_records.append(processed)
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = stream.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if not next_page_token:
                break
        print(json.dumps(all_records, indent=2, default=str))
        return True

    @final
    def write_schemas(self) -> None:
        """Write a SCHEMA message for all known streams to STDOUT."""
        for stream in self.streams.values():
            stream._write_schema_message()

    # Stream detection:

    def run_discovery(self) -> str:
        """Write the catalog json to STDOUT and return as a string.

        Returns:
            The catalog as a string of JSON.
        """
        catalog_text = self.catalog_json_text
        print(catalog_text)
        return catalog_text

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        return cast(dict, self._singer_catalog.to_dict())

    @property
    def catalog_json_text(self) -> str:
        """Get catalog JSON.

        Returns:
            The tap's catalog as formatted JSON text.
        """
        return json.dumps(self.catalog_dict, indent=2)

    @property
    def _singer_catalog(self) -> Catalog:
        """Return a Catalog object.

        Returns:
            :class:`hotglue_singer_sdk.helpers._singer.Catalog`.
        """
        return Catalog(
            (stream.tap_stream_id, stream._singer_catalog_entry)
            for stream in self.streams.values()
        )

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list.

        Return:
            List of discovered Stream objects.

        Raises:
            NotImplementedError: If the tap implementation does not override this
                method.
        """
        raise NotImplementedError(
            f"Tap '{self.name}' does not support discovery. "
            "Please set the '--catalog' command line argument and try again."
        )

    @classmethod
    def update_access_token(cls, authenticator, auth_endpoint, tap) -> None:
        """Update the access token.

        Returns:
            None
        """

        # If the tap has a use_auth_dummy_stream method, use it to create a dummy stream
        # normally used for taps with dynamic catalogs
        class DummyStream:
            def __init__(self, tap):
                self._tap = tap
                self.logger = tap.logger
                self.tap_name = tap.name
                self.config = tap.config

        stream = DummyStream(tap)
        auth = authenticator(
            stream=stream,
            config_file=tap.config_file,
            auth_endpoint=auth_endpoint,
        )

        # Update the access token
        if not auth.is_token_valid():
            auth.update_access_token()

    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        streams = [stream for streams in streams_by_type.values() for stream in streams]
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )

    # Bookmarks and state management

    def load_state(self, state: Dict[str, Any]) -> None:
        """Merge or initialize stream state with the provided state dictionary input.

        Override this method to perform validation and backwards-compatibility patches
        on self.state. If overriding, we recommend first running
        `super().load_state(state)` to ensure compatibility with the SDK.

        Args:
            state: Initialize the tap's state with this value.

        Raises:
            ValueError: If the tap's own state is None, meaning it has not been
                initialized.
        """
        if self.state is None:
            raise ValueError("Cannot write to uninitialized state dictionary.")

        for stream_name, stream_state in state.get("bookmarks", {}).items():
            for key, val in stream_state.items():
                write_stream_state(
                    self.state,
                    stream_name,
                    key,
                    val,
                )

    # State handling

    def _reset_state_progress_markers(self) -> None:
        """Clear prior jobs' progress markers at beginning of sync."""
        for _, state in self.state.get("bookmarks", {}).items():
            _state.reset_state_progress_markers(state)
            for partition_state in state.get("partitions", []):
                _state.reset_state_progress_markers(partition_state)

    # Fix sync replication method incompatibilities

    def _set_compatible_replication_methods(self) -> None:
        stream: Stream
        for stream in self.streams.values():
            for descendent in stream.descendent_streams:
                if descendent.selected and descendent.ignore_parent_replication_key:
                    self.logger.warning(
                        f"Stream descendent '{descendent.name}' is selected and "
                        f"its parent '{stream.name}' does not use inclusive "
                        f"replication keys. "
                        f"Forcing full table replication for '{stream.name}'."
                    )
                    stream.replication_key = None
                    stream.forced_replication_method = "FULL_TABLE"

    # Sync methods

    @final
    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()

    @classproperty
    def cli(cls) -> Callable:
        """Execute standard CLI handler for taps.

        Returns:
            A callable CLI object.
        """

        @common_options.PLUGIN_VERSION
        @common_options.PLUGIN_ABOUT
        @common_options.PLUGIN_ABOUT_FORMAT
        @common_options.PLUGIN_CONFIG
        @click.option(
            "--discover",
            is_flag=True,
            help="Run the tap in discovery mode.",
        )
        @click.option(
            "--test",
            is_flag=False,
            flag_value=CliTestOptionValue.All.value,
            default=CliTestOptionValue.Disabled,
            help=(
                "Use --test to sync a single record for each stream. "
                + "Use --test=schema to test schema output without syncing "
                + "records."
            ),
        )
        @click.option(
            "--catalog",
            help="Use a Singer catalog file with the tap.",
            type=click.Path(),
        )
        @click.option(
            "--state",
            help="Use a bookmarks file for incremental replication.",
            type=click.Path(),
        )
        @click.option(
            "--access-token",
            "access_token",
            is_flag=True,
            help="Refresh the OAuth access token and update the config file.",
        )
        @click.option(
            "--test-stream",
            "test_stream",
            is_flag=True,
            help="Fetch records for the stream specified by --stream.",
        )
        @click.option(
            "--stream",
            "request_stream_id",
            default=None,
            type=click.STRING,
            help="The stream name or tap_stream_id to request when using --test-stream.",
        )
        @click.option(
            "--request-only",
            "request_only",
            is_flag=True,
            help=(
                "With --test-stream: return the raw HTTP response payload (or error "
                "info) instead of parsed Singer records. REST streams only."
            ),
        )
        @click.option(
            "--pages",
            "pages",
            default=1,
            type=click.INT,
            help=(
                "With --test-stream: number of pages to fetch and return as parsed "
                "Singer records (default: 1)."
            ),
        )
        @click.command(
            help="Execute the Singer tap.",
            context_settings={"help_option_names": ["--help"]},
        )
        def cli(
            version: bool = False,
            about: bool = False,
            discover: bool = False,
            test: CliTestOptionValue = CliTestOptionValue.Disabled,
            config: Tuple[str, ...] = (),
            state: str = None,
            catalog: str = None,
            format: str = None,
            access_token: bool = False,
            test_stream: bool = False,
            request_stream_id: Optional[str] = None,
            request_only: bool = False,
            pages: int = 1,
        ) -> None:
            """Handle command line execution.

            Args:
                version: Display the package version.
                about: Display package metadata and settings.
                discover: Run the tap in discovery mode.
                test: Test connectivity by syncing a single record and exiting.
                format: Specify output style for `--about`.
                config: Configuration file location or 'ENV' to use environment
                    variables. Accepts multiple inputs as a tuple.
                catalog: Use a Singer catalog file with the tap.",
                state: Use a bookmarks file for incremental replication.
                access_token: Refresh the OAuth access token and update the config.
                test_stream: Activate stream test mode (requires --stream).
                request_stream_id: The stream name or tap_stream_id to request when
                    using --test-stream.
                request_only: Return raw HTTP response payload instead of parsed
                    records (REST streams only, used with --test-stream).
                pages: Number of pages to fetch when using --test-stream
                    (default: 1).

            Raises:
                FileNotFoundError: If the config file does not exist.
                click.UsageError: If --test-stream is used without --stream.
            """
            if version:
                cls.print_version()
                return

            if not about:
                cls.print_version(print_fn=cls.logger.info)
            else:
                cls.print_about(format=format)
                return

            validate_config: bool = True
            if discover:
                # Don't abort on validation failures
                validate_config = False

            parse_env_config = False
            config_files: List[PurePath] = []
            for config_path in config:
                if config_path == "ENV":
                    # Allow parse from env vars:
                    parse_env_config = True
                    continue

                # Validate config file paths before adding to list
                if not Path(config_path).is_file():
                    raise FileNotFoundError(
                        f"Could not locate config file at '{config_path}'."
                        "Please check that the file exists."
                    )

                config_files.append(Path(config_path))

            tap = cls(  # type: ignore  # Ignore 'type not callable'
                config=config_files or None,
                state=state,
                catalog=catalog,
                parse_env_config=parse_env_config,
                validate_config=validate_config,
            )

            if access_token:
                return cls.fetch_access_token(connector=tap)

            if test_stream:
                if not request_stream_id:
                    raise click.UsageError(
                        "--test-stream requires --stream <stream_id>."
                    )
                tap.run_test_stream(
                    request_stream_id,
                    request_only=request_only,
                    pages=pages,
                )
            elif discover:
                tap.register_streams_from_catalog(catalog)
                tap.register_state_from_file(state)
                tap.run_discovery()
                if test == CliTestOptionValue.All.value:
                    tap.run_connection_test()
            elif test == CliTestOptionValue.All.value:
                tap.run_connection_test()
            elif test == CliTestOptionValue.Schema.value:
                tap.write_schemas()
            else:
                tap.register_streams_from_catalog(catalog)
                tap.register_state_from_file(state)
                tap.sync_all()

        return cli


class SQLTap(Tap):
    """A specialized Tap for extracting from SQL streams."""

    # Stream class used to initialize new SQL streams from their catalog declarations.
    default_stream_class: Type[SQLStream]

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the SQL tap.

        The SQLTap initializer additionally creates a cache variable for _catalog_dict.

        Args:
            config: Tap configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            catalog: Tap catalog. Can be a dictionary or a path to the catalog file.
            state: Tap state. Can be dictionary or a path to the state file.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        self._catalog_dict: Optional[dict] = None
        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.default_stream_class.connector_class(dict(self.config))

        result: Dict[str, List[dict]] = {"streams": []}
        result["streams"].extend(connector.discover_catalog_entries())

        self._catalog_dict = result
        return self._catalog_dict

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: List[Stream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            result.append(self.default_stream_class(self, catalog_entry))

        return result


def custom_hotglue_tap_exception_handling(exc_type, exc_value, exc_traceback):
    """
    Global handler for all unhandled exceptions.
    """
    # if the exception is not derived from Exception we don't wanna log it
    # example: KeyboardInterrupt, SystemExit, etc.
    if not issubclass(exc_type, Exception):
        # use the default hook to print to stderr
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    try:
        exc_type_to_log = exc_type
        if issubclass(exc_type, InvalidCredentialsError):
            exc_type_to_log = InvalidCredentialsError

        exc_type_name = exc_type_to_log.__name__
        exc_message = str(exc_value) if exc_value else None
        formatted_traceback = "".join(traceback.format_tb(exc_traceback)) if exc_traceback else None

        exc_json = {
            "exception_name": exc_type_name,
            "exception_message": exc_message,
            "exception_traceback": formatted_traceback,
        }

        current_thread_id = threading.get_ident()

        with open(f"hg-tap-exception-{current_thread_id}.json", "w") as f:
            f.write(json.dumps(exc_json, indent=2))
    except Exception:
        # we don't want to raise exceptions from this hook
        pass

    # use the default hook to print to stderr
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


def install_thread_excepthook():
    """
    Workaround for uncaught exceptions in threads.
    """
    run_old = threading.Thread.run
    def run(*args, **kwargs):
        try:
            run_old(*args, **kwargs)
        except Exception:
            sys.excepthook(*sys.exc_info())
            raise
    threading.Thread.run = run


# only install the custom exception handler if we are running a tap
if len(sys.argv) > 0 and "tap" in sys.argv[0]:
    # Install the custom exception handler for main thread
    sys.excepthook = custom_hotglue_tap_exception_handling
    # Install the custom exception handler for threads
    install_thread_excepthook()
