from __future__ import annotations
from configs import *
from dataclasses import dataclass
from io import BytesIO
from typing import Optional
from google.cloud import storage

@dataclass(frozen=True)
class GCPStorageConfig:
    bucket_name: str
    credentials_json: Optional[str] = None
    project_id: Optional[str] = None

def get_storage_client(config: Optional[GCPStorageConfig] = None) -> storage.Client:
    if config is None:
        config = GCPStorageConfig(
            bucket_name=DEFAULT_GCP_BUCKET,
            credentials_json=DEFAULT_GCP_CREDENTIALS_JSON,
            project_id=DEFAULT_GCP_PROJECT_ID,
        )

    if config.credentials_json:
        return storage.Client.from_service_account_json(
            config.credentials_json, project=config.project_id
        )
    
    return storage.Client(project=config.project_id)


def get_bucket(config: Optional[GCPStorageConfig] = None) -> storage.Bucket:
    if config is None:
        config = GCPStorageConfig(
            bucket_name=DEFAULT_GCP_BUCKET,
            credentials_json=DEFAULT_GCP_CREDENTIALS_JSON,
            project_id=DEFAULT_GCP_PROJECT_ID,
        )

    if not config.bucket_name:
        raise ValueError("GCP bucket name is not configured.")

    client = get_storage_client(config)
    return client.bucket(config.bucket_name)


def upload_pdf(
    *,
    file_bytes: bytes,
    destination_path: str,
    content_type: str = "application/pdf",
    config: Optional[GCPStorageConfig] = None,
) -> str:
    bucket = get_bucket(config)
    blob = bucket.blob(destination_path)
    blob.upload_from_string(file_bytes, content_type=content_type)
    return blob.public_url


def upload_pdf_fileobj(
    *,
    file_obj: BytesIO,
    destination_path: str,
    content_type: str = "application/pdf",
    config: Optional[GCPStorageConfig] = None,
) -> str:
    bucket = get_bucket(config)
    blob = bucket.blob(destination_path)
    blob.upload_from_file(file_obj, content_type=content_type)
    return blob.public_url


def download_pdf(
    *,
    source_path: str,
    config: Optional[GCPStorageConfig] = None,
) -> bytes:
    bucket = get_bucket(config)
    blob = bucket.blob(source_path)
    return blob.download_as_bytes()


def download_pdf_to_fileobj(
    *,
    source_path: str,
    file_obj: BytesIO,
    config: Optional[GCPStorageConfig] = None,
) -> BytesIO:
    bucket = get_bucket(config)
    blob = bucket.blob(source_path)
    blob.download_to_file(file_obj)
    file_obj.seek(0)
    return file_obj
