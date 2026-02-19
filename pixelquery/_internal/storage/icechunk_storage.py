"""
Icechunk Storage Manager

Manages an Icechunk repository for virtual zarr storage of COG references.
Replaces IcebergStorageManager for the virtual dataset backend.

Thread-safety: Use ``get_storage_manager()`` to obtain a singleton per
warehouse path.  Each manager exposes a ``write_lock`` for serializing
concurrent writes (e.g. parallel Kafka consumers).
"""

import logging
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---- Singleton registry (one manager per resolved warehouse path) ----------
_managers: dict[str, "IcechunkStorageManager"] = {}
_managers_lock = threading.Lock()


def get_storage_manager(
    repo_path: str,
    storage_type: str = "local",
    storage_config: dict[str, Any] | None = None,
    vcc_prefix: str | None = None,
    vcc_data_path: str | None = None,
) -> "IcechunkStorageManager":
    """Get or create a singleton :class:`IcechunkStorageManager` for *repo_path*.

    Concurrent calls with the same resolved path will receive the same
    already-initialised instance, avoiding repeated ``Repository.open()``
    round-trips and potential creation race-conditions.
    """
    key = str(Path(repo_path).resolve())
    if key not in _managers:
        with _managers_lock:
            if key not in _managers:  # double-checked locking
                mgr = IcechunkStorageManager(
                    repo_path,
                    storage_type=storage_type,
                    storage_config=storage_config,
                    vcc_prefix=vcc_prefix,
                    vcc_data_path=vcc_data_path,
                )
                mgr.initialize()
                _managers[key] = mgr
    return _managers[key]


class IcechunkStorageManager:
    """
    Manages an Icechunk repository with Virtual Chunk Container support.

    Handles:
    - Repository creation / opening (local filesystem, S3, GCS)
    - VCC configuration for both write and read paths
    - Session management (writable / readonly)
    - Snapshot history (Time Travel)
    - ObjectStoreRegistry for VirtualiZarr
    """

    def __init__(
        self,
        repo_path: str,
        storage_type: str = "local",
        storage_config: dict[str, Any] | None = None,
        vcc_prefix: str | None = None,
        vcc_data_path: str | None = None,
    ):
        """
        Args:
            repo_path: Path to the Icechunk repository root directory
            storage_type: "local", "s3", or "gcs"
            storage_config: Cloud storage config (bucket, prefix, region, etc.)
            vcc_prefix: Virtual Chunk Container URL prefix (e.g. "file:///path/to/data/")
                        If None, auto-derived from repo_path parent for local storage.
            vcc_data_path: Local filesystem path for VCC store.
                          If None, auto-derived from vcc_prefix.
        """
        self.repo_path = str(repo_path)
        self.storage_type = storage_type
        self.storage_config = storage_config or {}
        self._repo = None
        self._registry = None
        self._initialized = False
        self._write_lock = threading.Lock()

        # Derive VCC settings
        # icechunk requires file:// prefix to include a path beyond root "/"
        if vcc_prefix is None and storage_type == "local":
            # Default: cover the repo's parent tree
            import os

            home = os.path.expanduser("~")
            # Use parent of home dir to cover most local paths
            base = str(Path(home).parent)
            if not base.endswith("/"):
                base += "/"
            self.vcc_prefix = f"file://{base}"
            self.vcc_data_path = base
        elif vcc_prefix is None and storage_type == "s3":
            # Auto-derive VCC prefix from S3 bucket in storage_config
            bucket = (storage_config or {}).get("bucket", "")
            if bucket:
                self.vcc_prefix = f"s3://{bucket}/"
                self.vcc_data_path = None  # type: ignore[assignment]
            else:
                self.vcc_prefix = "file:///tmp/"
                self.vcc_data_path = "/tmp/"
        else:
            self.vcc_prefix = vcc_prefix or "file:///tmp/"
            self.vcc_data_path = vcc_data_path or "/tmp/"

    def initialize(self) -> None:
        """Create or open the Icechunk repository."""
        if self._initialized:
            return

        import icechunk

        # Ensure repo directory exists (local only)
        if self.storage_type == "local":
            repo_dir = Path(self.repo_path)
            repo_dir.mkdir(parents=True, exist_ok=True)
            icechunk_dir = str(repo_dir / ".icechunk")
        else:
            icechunk_dir = self.repo_path  # S3/GCS: path used as prefix

        # Build repository config with VCC
        config = icechunk.RepositoryConfig.default()

        if self.storage_type == "local":
            vcc_store = icechunk.local_filesystem_store(self.vcc_data_path)
            container = icechunk.VirtualChunkContainer(self.vcc_prefix, vcc_store)
            config.set_virtual_chunk_container(container)

            storage = icechunk.local_filesystem_storage(icechunk_dir)
        elif self.storage_type == "s3":
            storage = icechunk.s3_storage(
                bucket=self.storage_config["bucket"],
                prefix=self.storage_config.get("prefix", ""),
                region=self.storage_config.get("region"),
                endpoint_url=self.storage_config.get("endpoint_url"),
                allow_http=bool(self.storage_config.get("allow_http", False)),
                access_key_id=self.storage_config.get("access_key_id"),
                secret_access_key=self.storage_config.get("secret_access_key"),
                force_path_style=bool(self.storage_config.get("force_path_style", False)),
            )
            if self.vcc_prefix:
                if self.vcc_prefix.startswith("s3://"):
                    # S3 VCC: use S3 store with same endpoint/credentials
                    vcc_store = icechunk.s3_store(  # type: ignore[assignment]
                        endpoint_url=self.storage_config.get("endpoint_url"),
                        allow_http=bool(self.storage_config.get("allow_http", False)),
                        force_path_style=bool(self.storage_config.get("force_path_style", False)),
                    )
                elif self.vcc_data_path:
                    vcc_store = icechunk.local_filesystem_store(self.vcc_data_path)
                else:
                    vcc_store = None
                if vcc_store is not None:
                    container = icechunk.VirtualChunkContainer(self.vcc_prefix, vcc_store)
                    config.set_virtual_chunk_container(container)
        elif self.storage_type == "gcs":
            storage = icechunk.gcs_storage(
                bucket=self.storage_config["bucket"],
                prefix=self.storage_config.get("prefix", ""),
            )
            if self.vcc_prefix and self.vcc_data_path:
                vcc_store = icechunk.local_filesystem_store(self.vcc_data_path)
                container = icechunk.VirtualChunkContainer(self.vcc_prefix, vcc_store)
                config.set_virtual_chunk_container(container)
        else:
            raise ValueError(f"Unknown storage type: {self.storage_type}")

        # Build VCC access credentials using containers_credentials() wrapper
        # which converts provider-specific creds to the Credentials type expected
        # by authorize_virtual_chunk_access
        if self.vcc_prefix and self.vcc_prefix.startswith("s3://"):
            ak = self.storage_config.get("access_key_id")
            sk = self.storage_config.get("secret_access_key")
            raw_cred = (
                icechunk.s3_credentials(access_key_id=ak, secret_access_key=sk)
                if ak and sk
                else None
            )
            vcc_auth = icechunk.containers_credentials({self.vcc_prefix: raw_cred})
        elif self.vcc_prefix:
            # Local / other VCC: authorize with None credentials
            vcc_auth = icechunk.containers_credentials({self.vcc_prefix: None})

        # Create or open repository (with retry for concurrent access)
        open_kwargs: dict[str, Any] = {"storage": storage, "config": config}
        if vcc_auth:
            open_kwargs["authorize_virtual_chunk_access"] = vcc_auth

        try:
            self._repo = icechunk.Repository.open(**open_kwargs)  # type: ignore[assignment]
            logger.info("Opened existing Icechunk repo at %s", self.repo_path)
        except Exception:
            try:
                self._repo = icechunk.Repository.create(  # type: ignore[assignment]
                    storage=storage,
                    config=config,
                )
                logger.info("Created new Icechunk repo at %s", self.repo_path)
            except Exception:
                # Another thread/process may have created the repo concurrently
                self._repo = icechunk.Repository.open(**open_kwargs)  # type: ignore[assignment]
                logger.info(
                    "Opened Icechunk repo at %s (created by another session)", self.repo_path
                )

        self._initialized = True

    @property
    def repo(self):
        """Get the Icechunk Repository, initializing if needed."""
        if not self._initialized:
            self.initialize()
        return self._repo

    @property
    def registry(self):
        """Get the ObjectStoreRegistry for VirtualiZarr."""
        if self._registry is None:
            import obstore
            from obspec_utils.registry import ObjectStoreRegistry

            self._registry = ObjectStoreRegistry()  # type: ignore[assignment]
            self._registry.register("file://", obstore.store.LocalStore())  # type: ignore[attr-defined]
            if self.storage_type == "s3":
                # Extract bucket from VCC prefix or storage_config
                if self.vcc_prefix and self.vcc_prefix.startswith("s3://"):
                    bucket = self.vcc_prefix.replace("s3://", "").strip("/").split("/")[0]
                else:
                    bucket = self.storage_config.get("bucket", "")
                if bucket:
                    # Set AWS env vars so S3Store reads them automatically.
                    # This avoids pyo3-object_store Rust panic when virtual_tiff
                    # serializes the store via __getnewargs_ex__().
                    import os

                    for env_key, cfg_key in [
                        ("AWS_ENDPOINT_URL", "endpoint_url"),
                        ("AWS_ACCESS_KEY_ID", "access_key_id"),
                        ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
                        ("AWS_REGION", "region"),
                    ]:
                        val = self.storage_config.get(cfg_key)
                        if val:
                            os.environ.setdefault(env_key, str(val))
                    if self.storage_config.get("allow_http"):
                        os.environ.setdefault("AWS_ALLOW_HTTP", "true")

                    s3_store = obstore.store.S3Store(bucket=bucket)
                    self._registry.register(f"s3://{bucket}/", s3_store)  # type: ignore[attr-defined]
        return self._registry

    @property
    def write_lock(self) -> threading.Lock:
        """Lock for serializing writes to this repository."""
        return self._write_lock

    def writable_session(self, branch: str = "main"):
        """Get a writable session."""
        return self.repo.writable_session(branch)

    def readonly_session(
        self,
        snapshot_id: str | None = None,
        branch: str = "main",
    ):
        """Get a readonly session, optionally at a specific snapshot."""
        if snapshot_id:
            return self.repo.readonly_session(snapshot_id=snapshot_id)
        return self.repo.readonly_session(branch=branch)

    def commit(self, session, message: str) -> str:
        """Commit a session and return the snapshot ID."""
        return session.commit(message)  # type: ignore[no-any-return]

    def get_snapshot_history(self) -> list[dict[str, Any]]:
        """Get snapshot ancestry for the main branch."""
        snapshots = []
        for snap in self.repo.ancestry(branch="main"):
            snapshots.append(
                {
                    "snapshot_id": snap.id,
                    "timestamp": str(snap.written_at) if hasattr(snap, "written_at") else None,
                    "message": snap.message,
                }
            )
        return snapshots
