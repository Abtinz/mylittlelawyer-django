from django.conf import settings

DEFAULT_GCP_BUCKET = getattr(settings, "GCP_BUCKET_NAME", "")
DEFAULT_GCP_CREDENTIALS_JSON = getattr(settings, "GCP_CREDENTIALS_JSON", None)
DEFAULT_GCP_PROJECT_ID = getattr(settings, "GCP_PROJECT_ID", None)