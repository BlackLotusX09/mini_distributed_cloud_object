
import hashlib
import pytest

import validation
import objects_api


def test_metadata_empty_patch_is_valid():
    assert validation.validate_patch({}, 16384) == {}


def test_metadata_reserved_keys_are_case_insensitive():
    for key in ["NAME", "Object_ID", "CHECKSUM", "Created_At"]:
        with pytest.raises(Exception):
            validation.validate_patch({key: "value"}, 16384)


def test_metadata_unicode_size_uses_utf8_bytes():
    value = "測" * 100
    metadata = {"description": value}

    assert len(value) == 100
    assert len(value.encode("utf-8")) == 300
    assert validation.serialized_size(metadata) > 300


def test_metadata_whitespace_only_key_rejected():
    with pytest.raises(Exception):
        validation.validate_patch({"   ": "value"}, 16384)


def test_metadata_none_allowed_only_for_patch():
    assert validation.validate_patch({"remove_me": None}, 16384) == {
        "remove_me": None
    }

    with pytest.raises(Exception):
        validation.parse_upload_metadata(
            '{"remove_me": null}', 16384
        )


def test_pagination_very_large_offset():
    limit, offset = validation.parse_pagination({
        "limit": "10",
        "offset": "999999999999"
    })

    assert limit == 10
    assert offset == 999999999999


def test_pagination_decimal_values_rejected():
    for params in [
        {"limit": "1.5"},
        {"offset": "2.5"},
    ]:
        with pytest.raises(Exception):
            validation.parse_pagination(params)


def test_content_disposition_unicode_filename():
    filename = "café résumé 测试.txt"
    header = objects_api.content_disposition(filename)

    assert "filename*=UTF-8''" in header
    assert "caf%C3%A9" in header
    assert "r%C3%A9sum%C3%A9" in header


#def test_content_disposition_newline_is_sanitized():
   # filename = 'report.txt\r\nX-Injected: true'
  #  header = objects_api.content_disposition(filename)
#
 #   assert "\r" not in header
    #assert "\n" not in header
    #//assert "X-Injected: true" not in header.split("filename=")[1].split(";")[0]
def test_content_disposition_newline_is_sanitized():
    filename = 'report.txt\r\nX-Injected: true'
    header = objects_api.content_disposition(filename)

    assert "\r" not in header
    assert "\n" not in header
    assert "%0D" not in header.upper()
    assert "%0A" not in header.upper()

def test_content_disposition_quotes_and_backslashes():
    filename = 'a"b\\c.txt'
    header = objects_api.content_disposition(filename)

    assert 'filename="a_b_c.txt"' in header


def test_object_id_validation():
    assert objects_api.valid_object_id(
        "123e4567-e89b-42d3-a456-426614174000"
    )

    assert not objects_api.valid_object_id("invalid-id")
    assert not objects_api.valid_object_id(
        "123e4567-e89b-12d3-a456-426614174000"
    )
    assert not objects_api.valid_object_id(
        "123e4567-e89b-42d3-a456-426614174000-extra"
    )
