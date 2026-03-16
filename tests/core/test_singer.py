from typing import List, Optional

import pytest

from hotglue_singer_sdk.helpers._singer import Catalog, CatalogEntry, Metadata, MetadataMapping, StreamMetadata, SchemaPlus


def test_catalog_parsing():
    """Validate parsing works for a catalog and its stream entries."""
    catalog_dict = {
        "streams": [
            {
                "tap_stream_id": "test",
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "inclusion": "available",
                        },
                    },
                    {
                        "breadcrumb": ["properties", "a"],
                        "metadata": {
                            "inclusion": "unsupported",
                        },
                    },
                ],
                "schema": {
                    "type": "object",
                },
            },
        ],
    }
    catalog = Catalog.from_dict(catalog_dict)

    assert catalog.streams[0].tap_stream_id == "test"
    assert catalog.get_stream("test").tap_stream_id == "test"
    assert catalog["test"].metadata.to_list() == catalog_dict["streams"][0]["metadata"]
    assert catalog["test"].tap_stream_id == catalog_dict["streams"][0]["tap_stream_id"]
    assert catalog["test"].schema.to_dict() == {"type": "object"}
    assert catalog.to_dict() == catalog_dict

    new = {
        "tap_stream_id": "new",
        "metadata": [],
        "schema": {},
    }
    entry = CatalogEntry.from_dict(new)
    catalog.add_stream(entry)
    assert catalog.get_stream("new") == entry


@pytest.mark.parametrize(
    "schema,key_properties,replication_method,valid_replication_keys",
    [
        (
            {"properties": {"id": {"type": "integer"}}, "type": "object"},
            ["id"],
            "FULL_TABLE",
            None,
        ),
        (
            {
                "properties": {
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                },
                "type": "object",
            },
            ["first_name", "last_name"],
            "INCREMENTAL",
            ["updated_at"],
        ),
        (
            {
                "properties": {
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                },
                "type": "object",
            },
            ["first_name", "last_name"],
            "FULL_TABLE",
            None,
        ),
        (
            {},
            [],
            None,
            None,
        ),
    ],
)
def test_standard_metadata(
    schema: dict,
    key_properties: List[str],
    replication_method: Optional[str],
    valid_replication_keys: Optional[List[str]],
):
    """Validate generated metadata."""
    metadata = MetadataMapping.get_standard_metadata(
        schema=schema,
        schema_name="test",
        key_properties=key_properties,
        replication_method=replication_method,
        valid_replication_keys=valid_replication_keys,
    )

    stream_metadata = metadata[()]
    assert stream_metadata.table_key_properties == key_properties
    assert stream_metadata.forced_replication_method == replication_method
    assert stream_metadata.valid_replication_keys == valid_replication_keys
    assert stream_metadata.selected is None

    for pk in key_properties:
        pk_metadata = metadata[("properties", pk)]
        assert pk_metadata.inclusion == Metadata.InclusionType.AUTOMATIC
        assert pk_metadata.selected is None

def test_metadata_from_dict_preserves_extra_keys():
    """Extra keys in metadata (e.g. visible) are preserved by from_dict (HGI-9926)."""
    data = {"inclusion": "available", "visible": False}
    meta = Metadata.from_dict(data)
    assert getattr(meta, "visible", None) is False

    data = {"inclusion": "available", "visible": True}
    meta = Metadata.from_dict(data)
    assert getattr(meta, "visible", None) is True
    
    stream_data = {"inclusion": "available", "visible": False}
    stream_meta = StreamMetadata.from_dict(stream_data)
    assert getattr(stream_meta, "visible", None) is False

    stream_data = {"inclusion": "available", "visible": True}
    stream_meta = StreamMetadata.from_dict(stream_data)
    assert getattr(stream_meta, "visible", None) is True

def test_metadata_to_dict_includes_extra_attributes():
    """to_dict includes extra attributes so round-trip preserves them (HGI-9926)."""
    meta = Metadata(inclusion=Metadata.InclusionType.AVAILABLE)
    setattr(meta, "visible", False)
    out = meta.to_dict()
    assert "visible" in out
    assert out["visible"] is False

    # Build catalog entry with root metadata that has extra "visible"
    root_meta = StreamMetadata(inclusion=Metadata.InclusionType.AVAILABLE)
    setattr(root_meta, "visible", False)
    metadata = MetadataMapping()
    metadata[()] = root_meta

    entry = CatalogEntry(
        tap_stream_id="test",
        metadata=metadata,
        schema=SchemaPlus.from_dict({"type": "object"}),
    )
    catalog = Catalog()
    catalog["test"] = entry

    out = catalog.to_dict()
    assert out["streams"][0]["metadata"][0]["metadata"]["visible"] is False
