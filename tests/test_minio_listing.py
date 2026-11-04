import subprocess
import sys
from unittest.mock import Mock, call

import pytest
from minio.datatypes import Object

from teleutils.core.orchestrators import list_files


@pytest.fixture
def minio(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "minio.example:9000")
    monkeypatch.setenv("MINIO_USER", "user")
    monkeypatch.setenv("MINIO_SECRET", "secret")
    client = Mock()
    client.list_objects.return_value = iter(())
    factory = Mock(return_value=client)
    monkeypatch.setattr("minio.Minio", factory)
    return client, factory


def objects(*names):
    return (Object("bucket", name) for name in names)


@pytest.mark.parametrize("recursive", [False, True])
@pytest.mark.parametrize("protocol", ["", "s3a://"])
def test_partitioned_dataset_is_one_logical_file(minio, protocol, recursive):
    client, _ = minio
    prefix = "cdr/processed/transformed/"
    dataset = prefix + "semana110_algar_smp_nokia_transformed.parquet"
    client.list_objects.return_value = objects(dataset + "/")

    assert list_files(f"{protocol}sandbox-sfi/{prefix}", "*.parquet", recursive) == [
        f"s3a://sandbox-sfi/{dataset}"
    ]
    client.list_objects.assert_called_once_with(
        "sandbox-sfi", prefix=prefix, recursive=False
    )


@pytest.mark.parametrize("recursive", [False, True])
def test_mixed_files_and_virtual_prefixes(minio, recursive):
    client, _ = minio
    tree = {
        "data/": [
            "data/z.parquet/",
            "data/a.parquet",
            "data/_SUCCESS",
            "data/notes.txt",
            "data/ordinary/",
            "data/empty/",
            "data/",
            "data/z.parquet/",
            "data/a.parquet",
            "data/ordinary/",
        ],
        "data/ordinary/": [
            "data/ordinary/deeper/",
            "data/ordinary/b.parquet/",
            "data/ordinary/readme.txt",
        ],
        "data/ordinary/deeper/": ["data/ordinary/deeper/c.parquet"],
        "data/empty/": ["data/empty/"],
    }

    def list_level(bucket, *, prefix, recursive):
        assert bucket == "bucket"
        assert recursive is False
        return objects(*tree[prefix])

    client.list_objects.side_effect = list_level
    expected = ["_SUCCESS", "a.parquet", "notes.txt", "z.parquet"]
    if recursive:
        expected += [
            "ordinary/b.parquet",
            "ordinary/deeper/c.parquet",
            "ordinary/readme.txt",
        ]
    assert list_files("bucket/data", recursive=recursive) == sorted(
        f"s3a://bucket/data/{name}" for name in expected
    )
    expected_prefixes = set(tree) if recursive else {"data/"}
    assert {c.kwargs["prefix"] for c in client.list_objects.call_args_list} == (
        expected_prefixes
    )
    assert client.list_objects.call_count == len(expected_prefixes)


def test_pattern_does_not_prune_common_folders_or_enter_rejected_datasets(minio):
    client, _ = minio
    levels = {
        "data/": ["data/rejected.parquet/", "data/common/"],
        "data/common/": ["data/common/wanted.parquet/"],
    }
    client.list_objects.side_effect = lambda bucket, *, prefix, recursive: objects(
        *levels[prefix]
    )

    assert list_files("bucket/data", "wanted*", True) == [
        "s3a://bucket/data/common/wanted.parquet"
    ]
    assert client.list_objects.call_args_list == [
        call("bucket", prefix="data/", recursive=False),
        call("bucket", prefix="data/common/", recursive=False),
    ]


@pytest.mark.parametrize(
    "pattern, expected",
    [
        ("", ["DATA.PARQUET", "_SUCCESS", "a1.parquet", "a2.parquet", "notes.txt"]),
        ("*.parquet", ["a1.parquet", "a2.parquet"]),
        ("a?.parquet", ["a1.parquet", "a2.parquet"]),
        ("a[12].parquet", ["a1.parquet", "a2.parquet"]),
        ("a[!2].parquet", ["a1.parquet"]),
        ("*.PARQUET", ["DATA.PARQUET"]),
        ("{a1,a2}.parquet", []),
        ("data*", []),
        ("missing*", []),
    ],
)
def test_glob_matches_only_basename_case_sensitively(minio, pattern, expected):
    client, _ = minio
    client.list_objects.return_value = objects(
        "data/a2.parquet/",
        "data/notes.txt",
        "data/a1.parquet",
        "data/_SUCCESS",
        "data/DATA.PARQUET",
    )
    assert list_files("bucket/data", pattern) == [
        f"s3a://bucket/data/{name}" for name in expected
    ]


@pytest.mark.parametrize(
    "base, bucket, prefix",
    [
        ("bucket", "bucket", ""),
        ("s3a://bucket///", "bucket", ""),
        ("bucket/data///", "bucket", "data/"),
        ("s3a://bucket/data", "bucket", "data/"),
        ("bucket.parquet", "bucket.parquet", ""),
        ("bucket/a b/+@%25//sub/", "bucket", "a b/+@%25//sub/"),
    ],
)
def test_base_normalization_and_prefix_boundary(minio, base, bucket, prefix):
    client, _ = minio
    client.list_objects.return_value = objects(prefix + "file.txt")
    assert list_files(base) == [f"s3a://{bucket}/{prefix}file.txt"]
    client.list_objects.assert_called_once_with(bucket, prefix=prefix, recursive=False)


@pytest.mark.parametrize(
    "kwargs, error, field",
    [
        ({"base_folder": None}, TypeError, "base_folder"),
        ({"base_folder": 1}, TypeError, "base_folder"),
        ({"pattern": None}, TypeError, "pattern"),
        ({"pattern": []}, TypeError, "pattern"),
        ({"recursive": "false"}, TypeError, "recursive"),
        ({"recursive": 1}, TypeError, "recursive"),
        ({"secure": "false"}, TypeError, "secure"),
        ({"secure": 0}, TypeError, "secure"),
        ({"base_folder": ""}, ValueError, "base_folder"),
        ({"base_folder": " "}, ValueError, "base_folder"),
        ({"base_folder": "s3a://"}, ValueError, "base_folder"),
        ({"base_folder": "/bucket/data"}, ValueError, "base_folder"),
        ({"base_folder": "s3://bucket/data"}, ValueError, "base_folder"),
        ({"base_folder": "bucket/../data"}, ValueError, "base_folder"),
        ({"base_folder": "bucket/%2e%2e/data"}, ValueError, "base_folder"),
        ({"base_folder": "bucket/*"}, ValueError, "base_folder"),
        ({"base_folder": "bucket/data.parquet/"}, ValueError, "contêiner"),
        ({"base_folder": "bucket/data.parquet/p=1"}, ValueError, "contêiner"),
        ({"pattern": "sub/*.parquet"}, ValueError, "pattern"),
        ({"pattern": "s3a://bucket/*"}, ValueError, "pattern"),
        ({"pattern": r"sub\*.parquet"}, ValueError, "pattern"),
        ({"pattern": "sub%2F*.parquet"}, ValueError, "pattern"),
        ({"pattern": "\n"}, ValueError, "pattern"),
    ],
)
def test_invalid_arguments_fail_before_client_creation(minio, kwargs, error, field):
    _, factory = minio
    options = {"base_folder": "bucket/data", **kwargs}
    with pytest.raises(error, match=field):
        list_files(**options)
    factory.assert_not_called()


def test_empty_prefix_returns_empty_list(minio):
    assert list_files("bucket/missing") == []


@pytest.mark.parametrize("secure", [False, True])
def test_environment_and_transport(minio, secure):
    _, factory = minio
    assert list_files("bucket", secure=secure) == []
    factory.assert_called_once_with(
        endpoint="minio.example:9000",
        access_key="user",
        secret_key="secret",
        secure=secure,
    )


def test_explicit_options_override_environment_and_default_to_http(minio):
    _, factory = minio
    list_files("bucket", endpoint="server:9000", access_key="key", secret_key="s")
    factory.assert_called_once_with(
        endpoint="server:9000", access_key="key", secret_key="s", secure=False
    )


@pytest.mark.parametrize("variable", ["MINIO_ENDPOINT", "MINIO_USER", "MINIO_SECRET"])
def test_missing_connection_setting_is_explicit(minio, monkeypatch, variable):
    _, factory = minio
    monkeypatch.delenv(variable)
    with pytest.raises(ValueError, match=variable):
        list_files("bucket")
    factory.assert_not_called()


@pytest.mark.parametrize("option", ["endpoint", "access_key", "secret_key"])
@pytest.mark.parametrize("value", ["", " ", 1])
def test_invalid_explicit_setting_does_not_fall_back_to_environment(
    minio, option, value
):
    _, factory = minio
    with pytest.raises(ValueError, match=option):
        list_files("bucket", **{option: value})
    factory.assert_not_called()


@pytest.mark.parametrize("stage", ["client", "listing", "iteration"])
def test_errors_are_propagated_without_partial_results(minio, stage):
    client, factory = minio
    error = RuntimeError("MinIO unavailable")

    def failing_pages():
        yield Object("bucket", "data/first.parquet/")
        raise error

    if stage == "client":
        factory.side_effect = error
    elif stage == "listing":
        client.list_objects.side_effect = error
    else:
        client.list_objects.return_value = failing_pages()
    with pytest.raises(RuntimeError) as caught:
        list_files("bucket/data")
    assert caught.value is error


def test_all_iterator_batches_are_consumed(minio):
    client, _ = minio
    batches = [
        ["data/z.parquet/", "data/a.parquet"],
        ["data/b.parquet/", "data/a.parquet"],
    ]

    def pages():
        for batch in batches:
            yield from objects(*batch)

    client.list_objects.return_value = pages()
    assert list_files("bucket/data") == [
        "s3a://bucket/data/a.parquet",
        "s3a://bucket/data/b.parquet",
        "s3a://bucket/data/z.parquet",
    ]


@pytest.mark.parametrize("name", [None, ""])
def test_unnamed_sdk_object_is_reported(minio, name):
    client, _ = minio
    client.list_objects.return_value = objects(name)
    with pytest.raises(ValueError, match="sem nome"):
        list_files("bucket/data")


def test_public_import_does_not_load_spark_or_minio():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; from teleutils.core.orchestrators import list_files; "
                "assert callable(list_files); "
                "assert 'pyspark' not in sys.modules; "
                "assert 'minio' not in sys.modules"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
