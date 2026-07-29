# Hotglue SDK for Taps

This is a fork of Melanto's SingerSDK for special use in [hotglue](https://hotglue.com), an embedded integration platform for running Singer Taps and Targets.

Taps and targets built on the SDK are automatically compliant with the
[Singer Spec](https://hub.meltano.com/singer/spec), the
de-facto open source standard for extract and load pipelines.

## OAuth Access Token Support

Taps can implement the `--access-token` CLI flag to refresh OAuth access tokens without running the tap directly.

### Implementing Access Token Support

To enable this feature in your tap, override the `access_token_support` class method to return a tuple of `(authenticator_class, auth_endpoint)`:

```python
from hotglue_singer_sdk import Tap
from my_tap.auth import MyOAuthAuthenticator

class MyTap(Tap):
    name = "tap-myservice"

    @classmethod
    def access_token_support(cls, connector=None):
        """Return the authenticator class and auth endpoint for token refresh.

        Returns:
            A tuple of (authenticator_class, auth_endpoint).
        """
        default_url = "https://api.myservice.com/oauth/token"
        # ommit if token url is not dynamic
        dynamic_url = connector.config.get("auth_url")
        url = dynamic_url or default_url
        return (MyOAuthAuthenticator, url)
```

### Authenticator Requirements

The authenticator class must implement the following methods:

- `is_token_valid()` - Returns `True` if the current access token is still valid
- `update_access_token()` - Refreshes the access token and updates the config file

The authenticator will be instantiated with these parameters:
- `stream` - A dummy stream object with `logger`, `tap_name`, and `config` attributes
- `config_file` - Path to the config file for writing updated tokens
- `auth_endpoint` - The OAuth token endpoint URL

### Usage

Once implemented, users can refresh the access token using:

```bash
tap-myservice --config config.json --access-token
```

This will output the new access token as JSON:

```json
{
  "access_token": "new_token_value"
}
```

**Note:** The `--access-token` flag requires a config file path. It will not work with `--config ENV` or when omitting the config.

## Stream Schema Resolution

During a sync the tap is handed a catalog which already contains each stream's schema, recorded when discovery last ran. The SDK serves that schema instead of resolving it again, so streams which detect their schema from the API do not re-detect it on every sync — including streams the user has deselected.

`Stream.schema` resolves once per stream, in this order:

1. A static schema, if the stream was given one (the `schema=` argument or `schema_filepath`).
2. The schema catalogued for the stream in the tap's input catalog. An entry whose schema has no properties is skipped, since it cannot be used as a schema.
3. `get_schema()`, for streams which detect their schema at runtime.

### Implementing Dynamic Schemas

Streams whose schema is only known at runtime should override `get_schema()` rather than the `schema` property:

```python
class MyStream(RESTStream):
    name = "my_stream"

    def get_schema(self) -> dict:
        """Detect the schema for this stream."""
        records = self.request_records({})
        return infer_schema(records)
```

The SDK calls this only when the catalog holds nothing usable for the stream — a discovery run, or a stream which is new since the last discovery. The result is cached for the life of the stream.

### Opting Out

Set `use_input_catalog` to `false` in the tap config to ignore the catalogued schema and always resolve it from the stream:

```json
{
  "use_input_catalog": false
}
```

**Note:** A stream which overrides the `schema` property directly bypasses all of the above — the catalog is never consulted and `get_schema()` is never called. Such taps keep working unchanged, but to pick up catalog reuse they must remove the `schema` override and expose their detection logic as `get_schema()` instead.