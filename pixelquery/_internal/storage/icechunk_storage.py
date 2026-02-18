"""
Icechunk Storage Manager

Manages an Icechunk repository for virtual zarr storage of COG references.
Replaces IcebergStorageManager for the virtual dataset backend.
"""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


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

        # Build VCC access credentials
        vcc_credentials = None
        if self.vcc_prefix and self.vcc_prefix.startswith("s3://"):
            ak = self.storage_config.get("access_key_id")
            sk = self.storage_config.get("secret_access_key")
            if ak and sk:
                vcc_credentials = icechunk.s3_static_credentials(
                    access_key_id=ak,
                    secret_access_key=sk,
                )

        # Create or open repository
        try:
            self._repo = icechunk.Repository.open(  # type: ignore[assignment]
                storage=storage,
                config=config,
                authorize_virtual_chunk_access={self.vcc_prefix: vcc_credentials},  # type: ignore[dict-item]
            )
            logger.info("Opened existing Icechunk repo at %s", self.repo_path)
        except Exception:
            self._repo = icechunk.Repository.create(  # type: ignore[assignment]
                storage=storage,
                config=config,
            )
            logger.info("Created new Icechunk repo at %s", self.repo_path)

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
        return self._registry

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
