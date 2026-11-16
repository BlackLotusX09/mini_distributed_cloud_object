"""Custom-metadata and pagination validation. Every violation is an ApiError."""
import json
from typing import Dict, Optional, Tuple

from errors import ApiError

RESERVED_KEYS = frozenset({
    "id", "object_id", "name", "size", "content_type", "checksum",
    "chunk_count", "status", "created_at", "modified_at",
})
MAX_KEY_LEN = 256
DEFAULT_LIMIT, MAX_LIMIT = 50, 200


def _invalid(msg: str) -> ApiError:
    return ApiError(400, "INVALID_METADATA", msg)


def serialized_size(d: dict) -> int:
    return len(json.dumps(d, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _check_keys(d: dict, allow_none: bool, max_bytes: int) -> None:
    reserved = sorted(k for k in d if isinstance(k, str) and k.lower() in RESERVED_KEYS)
    if reserved:
        raise _invalid(f"reserved metadata keys not allowed: {', '.join(reserved)}")
    for k, v in d.items():
        if not isinstance(k, str) or not k.strip():
            raise _invalid("metadata keys must be non-empty strings")
        if len(k) > MAX_KEY_LEN:
            raise _invalid(f"metadata key too long (max {MAX_KEY_LEN} chars)")
        if v is None and allow_none:
            continue
        if not isinstance(v, str):
            raise _invalid(f"metadata value for '{k}' must be a string"
                           + (" or null" if allow_none else ""))
    if serialized_size(d) > max_bytes:
        raise _invalid(f"metadata exceeds {max_bytes} bytes")


def parse_upload_metadata(raw, max_bytes: int) -> Dict[str, str]:
    """`metadata` form field: absent/empty => {}; otherwise a JSON object of str -> str."""
    if raw is None or raw == "":
        return {}
    if not isinstance(raw, str):
        raise _invalid("metadata must be a JSON string field")
    try:
        value = json.loads(raw)
    except ValueError:
        raise _invalid("metadata is not valid JSON") from None
    if not isinstance(value, dict):
        raise _invalid("metadata must be a JSON object")
    _check_keys(value, allow_none=False, max_bytes=max_bytes)
    return value


def validate_patch(body, max_bytes: int) -> Dict[str, Optional[str]]:
    if not isinstance(body, dict):
        raise _invalid("request body must be a JSON object")
    _check_keys(body, allow_none=True, max_bytes=max_bytes)
    return body


def parse_pagination(params) -> Tuple[int, int]:
    def parse(name, default):
        raw = params.get(name)
        if raw is None:
            return default
        if isinstance(raw, bool):
            raise ApiError(400, "INVALID_PAGINATION", f"'{name}' must be an integer")
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ApiError(400, "INVALID_PAGINATION", f"'{name}' must be an integer") from None
        if value < 0:
            raise ApiError(400, "INVALID_PAGINATION", f"'{name}' must not be negative")
        return value

    limit = parse("limit", DEFAULT_LIMIT)
    if limit < 1 or limit > MAX_LIMIT:
        raise ApiError(400, "INVALID_PAGINATION", f"'limit' must be between 1 and {MAX_LIMIT}")
    return limit, parse("offset", 0)

