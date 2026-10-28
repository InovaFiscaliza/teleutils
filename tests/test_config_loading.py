import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from teleutils.core.orchestrators import load_config


def test_load_example_configuration():
    path = (
        Path(__file__).resolve().parents[1]
        / "src/teleutils/core/orchestrators/etl.config.example.json"
    )
    configs = load_config(str(path))
    assert configs
    assert all("output_file_prefix" in config for config in configs)


def test_load_local_configuration_without_minio_environment(tmp_path, monkeypatch):
    for name in ("MINIO_ENDPOINT", "MINIO_USER", "MINIO_SECRET"):
        monkeypatch.delenv(name, raising=False)
    configs = [{"output_file_prefix": "claro"}, {"output_file_prefix": "tim"}]
    path = tmp_path / "etl.json"
    path.write_text(json.dumps(configs), encoding="utf-8")

    assert load_config(str(path)) == configs


@pytest.mark.parametrize(
    "content, error, message",
    [
        ("not json", json.JSONDecodeError, "Expecting value"),
        ('{"output_file_prefix": "a"}', TypeError, "lista"),
        ("[{}, null]", TypeError, "índice 1"),
        (
            '[{"output_file_prefix": "a"}, {"output_file_prefix": "a"}]',
            ValueError,
            r"output_file_prefix.*\[0, 1\]",
        ),
    ],
)
def test_invalid_local_configuration_is_rejected(tmp_path, content, error, message):
    path = tmp_path / "etl.json"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(error, match=message):
        load_config(str(path))


@pytest.mark.parametrize(
    "path",
    [
        "relative.json",
        "file:///data/etl.json",
        "s3a://bucket",
        "s3a://bucket/",
        "s3a:///key",
    ],
)
def test_invalid_paths_are_rejected(path):
    with pytest.raises(ValueError, match="path"):
        load_config(path)


def test_missing_local_file_is_reported(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(str(tmp_path / "missing.json"))


def test_invalid_path_type_is_rejected():
    with pytest.raises(TypeError, match="path"):
        load_config(None)


def test_minio_reads_uri_uses_environment_and_releases_response(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "minio.example:9000")
    monkeypatch.setenv("MINIO_USER", "user")
    monkeypatch.setenv("MINIO_SECRET", "secret")
    response = Mock()
    response.read.return_value = b'[{"output_file_prefix":"item"}]'
    client = Mock()
    client.get_object.return_value = response
    factory = Mock(return_value=client)
    monkeypatch.setattr("minio.Minio", factory)

    result = load_config("s3a://bucket/cdr/sub/file.json", secure=True)

    assert result == [{"output_file_prefix": "item"}]
    factory.assert_called_once_with(
        endpoint="minio.example:9000",
        access_key="user",
        secret_key="secret",
        secure=True,
    )
    client.get_object.assert_called_once_with("bucket", "cdr/sub/file.json")
    response.close.assert_called_once_with()
    response.release_conn.assert_called_once_with()


def test_minio_explicit_options_override_environment(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "ignored")
    monkeypatch.setenv("MINIO_USER", "ignored")
    monkeypatch.setenv("MINIO_SECRET", "ignored")
    response = Mock()
    response.read.return_value = b"[]"
    client = Mock()
    client.get_object.return_value = response
    factory = Mock(return_value=client)
    monkeypatch.setattr("minio.Minio", factory)

    assert (
        load_config(
            "s3a://bucket/file.json",
            endpoint="server:9000",
            access_key="key",
            secret_key="secret",
        )
        == []
    )
    factory.assert_called_once_with(
        endpoint="server:9000",
        access_key="key",
        secret_key="secret",
        secure=False,
    )


@pytest.mark.parametrize(
    "content", [b"invalid", b'[{"output_file_prefix":"x"},{"output_file_prefix":"x"}]']
)
def test_minio_releases_response_on_invalid_json_or_duplicate(monkeypatch, content):
    response = Mock()
    response.read.return_value = content
    client = Mock()
    client.get_object.return_value = response
    monkeypatch.setattr("minio.Minio", Mock(return_value=client))
    with pytest.raises(ValueError):
        load_config(
            "s3a://bucket/file",
            endpoint="server",
            access_key="key",
            secret_key="secret",
        )
    response.close.assert_called_once_with()
    response.release_conn.assert_called_once_with()


def test_minio_releases_response_on_read_failure(monkeypatch):
    response = Mock()
    response.read.side_effect = OSError("read failure")
    client = Mock()
    client.get_object.return_value = response
    monkeypatch.setattr("minio.Minio", Mock(return_value=client))
    with pytest.raises(OSError, match="read failure"):
        load_config(
            "s3a://bucket/file",
            endpoint="server",
            access_key="key",
            secret_key="secret",
        )
    response.close.assert_called_once_with()
    response.release_conn.assert_called_once_with()


@pytest.mark.parametrize("variable", ["MINIO_ENDPOINT", "MINIO_USER", "MINIO_SECRET"])
def test_missing_minio_setting_is_explicit(monkeypatch, variable):
    for name in ("MINIO_ENDPOINT", "MINIO_USER", "MINIO_SECRET"):
        monkeypatch.setenv(name, "value")
    monkeypatch.delenv(variable)
    with pytest.raises(ValueError, match=variable):
        load_config("s3a://bucket/file")


def test_minio_get_error_is_propagated(monkeypatch):
    client = Mock()
    client.get_object.side_effect = RuntimeError("object unavailable")
    monkeypatch.setattr("minio.Minio", Mock(return_value=client))
    with pytest.raises(RuntimeError, match="object unavailable"):
        load_config(
            "s3a://bucket/file",
            endpoint="server",
            access_key="key",
            secret_key="secret",
        )


def test_minio_secure_must_be_boolean():
    with pytest.raises(TypeError, match="secure"):
        load_config("s3a://bucket/file", secure="false")
