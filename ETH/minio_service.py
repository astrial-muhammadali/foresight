"""JPEG storage with bounded requests and safe, deterministic download filenames."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

from minio import Minio
from minio.error import MinioException, S3Error
from urllib3 import PoolManager, Retry, Timeout
from urllib3.exceptions import HTTPError

from config import Settings

LOGGER = logging.getLogger(__name__)
STORAGE_ERRORS = (MinioException, HTTPError, OSError, ValueError)


class MinioServiceError(RuntimeError):
    """A bucket, upload or download operation failed."""


def check_jpeg(path: Path) -> None:
    """Check JPEG framing without adding an image-decoding dependency."""
    with path.open("rb") as image:
        if image.read(3) != b"\xff\xd8\xff":
            raise ValueError(f"{path.name} does not have a JPEG signature")
        image.seek(-2, 2)
        if image.read(2) != b"\xff\xd9":
            raise ValueError(f"{path.name} does not have a JPEG end marker")


class MinioService:
    def __init__(self, settings: Settings):
        self.bucket = settings.minio_bucket
        self._http = PoolManager(
            timeout=Timeout(connect=5, read=settings.minio_timeout_seconds),
            retries=Retry(total=2, backoff_factor=0.3),
        )
        self._client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
            http_client=self._http,
        )

    def bucket_exists(self, bucket: str | None = None) -> bool:
        try:
            return self._client.bucket_exists(bucket_name=bucket or self.bucket)
        except STORAGE_ERRORS as exc:
            raise MinioServiceError(
                "Cannot check MinIO bucket; check endpoint and credentials"
            ) from exc

    def create_bucket_if_required(self, bucket: str | None = None) -> None:
        name = bucket or self.bucket
        if self.bucket_exists(name):
            return
        try:
            self._client.make_bucket(bucket_name=name)
        except S3Error as exc:
            # Another producer using this account may have created the bucket.
            if exc.code != "BucketAlreadyOwnedByYou":
                raise MinioServiceError(
                    f"Cannot create MinIO bucket {name}: {exc.code}"
                ) from exc
        except STORAGE_ERRORS as exc:
            raise MinioServiceError(f"Cannot create MinIO bucket {name}") from exc

    def upload_image(self, image_path: Path | str, object_key: str) -> dict[str, str]:
        path = Path(image_path)
        try:
            check_jpeg(path)
            self.create_bucket_if_required()
            self._client.fput_object(
                bucket_name=self.bucket,
                object_name=object_key,
                file_path=str(path),
                content_type="image/jpeg",
            )
        except STORAGE_ERRORS as exc:
            raise MinioServiceError(
                f"JPEG upload failed for {self.bucket}/{object_key}: {exc}"
            ) from exc
        LOGGER.info("Uploaded image bucket=%s object_key=%s", self.bucket, object_key)
        return {
            "bucket": self.bucket,
            "object_key": object_key,
            "content_type": "image/jpeg",
        }

    def download_image(
        self, bucket: str, object_key: str, destination_dir: Path | str
    ) -> Path:
        if bucket != self.bucket:
            raise MinioServiceError(
                f"Observation bucket must match configured bucket {self.bucket}"
            )
        # Never turn a remote object key directly into a local filesystem path.
        filename = (
            hashlib.sha256(f"{bucket}\0{object_key}".encode()).hexdigest() + ".jpg"
        )
        destination = Path(destination_dir).resolve()
        path = destination / filename
        if path.is_symlink():
            raise MinioServiceError("Image download target must not be a symbolic link")
        try:
            destination.mkdir(parents=True, exist_ok=True)
            # Isolate SDK partial files, preserve an existing JPEG on failure,
            # and clean up temporary data even if the connection is interrupted.
            with TemporaryDirectory(prefix=".download-", dir=destination) as work:
                temporary = Path(work) / "image.jpg"
                self._client.fget_object(
                    bucket_name=bucket,
                    object_name=object_key,
                    file_path=str(temporary),
                )
                check_jpeg(temporary)
                temporary.replace(path)
        except STORAGE_ERRORS as exc:
            raise MinioServiceError(
                f"JPEG download failed for {bucket}/{object_key}: {exc}"
            ) from exc
        LOGGER.info("Saved image to %s", path)
        return path

    def close(self) -> None:
        self._http.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
