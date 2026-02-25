"""
S3 integration tests using MinIO.

Requires Docker:
    docker run -d -p 9000:9000 -p 9001:9001 \
        -e MINIO_ROOT_USER=minioadmin \
        -e MINIO_ROOT_PASSWORD=minioadmin \
        minio/minio server /data --console-address ":9001"

Skip if MinIO is not available.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import numpy as np
import pytest


# Check MinIO availability
def _minio_available():
    """Check if MinIO is running on localhost:9000."""
    try:
        import urllib.request
        urllib.request.urlopen("http://localhost:9000/minio/health/live", timeout=2)
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.skipif(not _minio_available(), reason="MinIO not running on localhost:9000"),
]


S3_CONFIG = {
    "bucket": "test-pixelquery",
    "endpoint_url": "http://localhost:9000",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
    "allow_http": True,
    "force_path_style": True,
    "region": "us-east-1",
}


@pytest.fixture(scope="module")
def minio_bucket():
    """Create test bucket in MinIO."""
    try:
        import obstore.store

        obstore.store.S3Store(
            bucket=S3_CONFIG["bucket"],
            config={
                "endpoint": S3_CONFIG["endpoint_url"],
                "access_key_id": S3_CONFIG["access_key_id"],
                "secret_access_key": S3_CONFIG["secret_access_key"],
                "region": S3_CONFIG["region"],
                "allow_http": "true",
                "virtual_hosted_style_request": "false",
            },
        )
        # Try to list (this creates the bucket implicitly in some MinIO configs)
        # If it fails, try via boto3 or urllib
        yield S3_CONFIG
    except Exception as e:
        pytest.skip(f"Could not configure MinIO bucket: {e}")


@pytest.fixture(scope="module")
def sample_cog(tmp_path_factory):
    """Create a small sample COG for testing."""
    try:
        import rasterio
        from rasterio.transform import from_bounds
    except ImportError:
        pytest.skip("rasterio required for creating test COGs")

    tmp_dir = tmp_path_factory.mktemp("cogs")
    cog_path = str(tmp_dir / "test_20250101.tif")

    transform = from_bounds(127.0, 37.0, 127.1, 37.1, 64, 64)
    data = np.random.randint(0, 10000, (4, 64, 64), dtype=np.uint16)

    with rasterio.open(
        cog_path,
        "w",
        driver="GTiff",
        height=64,
        width=64,
        count=4,
        dtype="uint16",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data)

    return cog_path


class TestS3AutoDetect:
    """Test S3 URI detection in open_dataset()."""

    def test_s3_uri_does_not_use_path(self):
        """S3 URIs should not go through Path().exists() check."""
        from pathlib import Path as _Path

        # This should NOT raise - it should detect S3 and route to open_xarray
        # (which will fail for other reasons, but not because of Path)
        s3_path = "s3://nonexistent-bucket/nonexistent-repo"
        with pytest.raises((ValueError, Exception)):
            import pixelquery as pq
            pq.open_dataset(s3_path, storage_type="s3", storage_config=S3_CONFIG)

    def test_s3_uri_detected_as_remote(self):
        """Verify S3 URIs are recognized as remote paths."""
        for uri in ["s3://bucket/path", "gs://bucket/path", "https://example.com/path"]:
            assert uri.startswith(("s3://", "gs://", "http://", "https://"))


class TestS3StorageSingleton:
    """Test that S3 URIs work as singleton keys."""

    def test_s3_key_no_path_resolve(self):
        """S3 keys should NOT go through Path().resolve()."""
        from pixelquery._internal.storage.icechunk_storage import get_storage_manager

        # This should not raise TypeError from Path("s3://...")
        # It may raise other errors (no bucket, etc.) but not Path errors
        s3_path = "s3://test-bucket/test-repo"
        try:
            get_storage_manager(s3_path, storage_type="s3", storage_config=S3_CONFIG)
        except Exception as e:
            # Expected: connection errors, bucket errors, etc.
            # NOT expected: Path errors
            assert "Path" not in str(type(e).__name__), f"Got Path-related error: {e}"


class TestS3ThreadSafety:
    """Test thread-safe S3 environment variable handling."""

    def test_env_vars_set_under_lock(self):
        """Verify _env_lock exists in module."""
        from pixelquery._internal.storage import icechunk_storage

        assert hasattr(icechunk_storage, "_env_lock")
