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