from hotglue_singer_sdk.exceptions import StreamMapConfigError
import logging


def _build_filter_fn(self, filter_rule):
    if filter_rule is None:
        return lambda record: True
    if not isinstance(filter_rule, str):

        def _filter(record):
            filter_result = self._eval(expr=filter_rule, record=record, property_name=None)
            logging.debug(
                f"Filter result for '{filter_rule}' in '{{self.name}}' stream: {{filter_result}}"
            )
            if not filter_result:
                logging.debug("Excluding record due to filter.")
                return False

            return True

        return _filter
    raise StreamMapConfigError(
        f"Unexpected filter rule type '{type(filter_rule).__name__}' in "
        f"expression {str(filter_rule)}. Expected 'str' or 'None'."
    )


def _build_transform_fn(self, stream_map, include_by_default):
    def transform(record):
        if not self.get_filter_result(record):
            return None

        result = (
            record.copy()
            if include_by_default
            else {k: record[k] for k in self.transformed_key_properties or [] if k in record}
        )

        for prop_key, prop_def in list(stream_map.items()):
            if prop_def is None:
                # Remove property from result
                result.pop(prop_key, None)
                continue

            if isinstance(prop_def, str):
                # Apply property transform
                result[prop_key] = self._eval(expr=prop_def, record=record, property_name=prop_key)
                continue

            raise StreamMapConfigError(
                f"Unexpected mapping type '{type(prop_def).__name__}' in "
                f"map expression '{prop_def}'. Expected 'str' or 'None'."
            )

    return transform
