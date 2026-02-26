"""Sample tap that reads from a SQLite database."""

from typing import Any, Dict, List

import sqlalchemy
from hotglue_singer_sdk.streams import SQLStream
from hotglue_singer_sdk.streams.sql import SQLConnector
from hotglue_singer_sdk.tap_base import SQLTap


class SQLiteConnector(SQLConnector):
    """SQLite connector using path_to_db config."""

    def get_sqlalchemy_url(self, config: Dict[str, Any]) -> str:
        """Return SQLite URL from path_to_db."""
        path = config.get("path_to_db")
        if not path:
            raise ValueError("config must contain 'path_to_db'")
        return f"sqlite:///{path}"

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Create SQLite engine with check_same_thread=False for target threading."""
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
            connect_args={"check_same_thread": False},
        )


class SQLiteStream(SQLStream):
    """SQLite stream."""

    connector_class = SQLiteConnector


class SQLiteTap(SQLTap):
    """Sample tap that discovers and syncs SQLite tables."""

    name = "sample-tap-sqlite"
    config_jsonschema = {
        "type": "object",
        "properties": {"path_to_db": {"type": "string"}},
        "required": ["path_to_db"],
    }
    default_stream_class = SQLiteStream
