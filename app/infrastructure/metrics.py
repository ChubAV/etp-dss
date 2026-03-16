from prometheus_client import Counter, Histogram

UPLOAD_REQUESTS = Counter(
    "dss_upload_requests_total",
    "Total upload requests",
    ["status", "content_type", "owner_type"],
)
UPLOAD_DURATION = Histogram(
    "dss_upload_duration_seconds",
    "Upload duration",
    ["content_type"],
)
UPLOAD_SIZE = Histogram(
    "dss_upload_size_bytes",
    "Upload file sizes",
    ["content_type", "owner_type"],
    buckets=[1024, 10240, 102400, 1048576, 5242880, 10485760, 20971520],
)
DOWNLOAD_REQUESTS = Counter(
    "dss_download_requests_total",
    "Total download requests",
    ["status", "visibility"],
)
PRESIGNED_URL_DURATION = Histogram(
    "dss_presigned_url_duration_seconds",
    "Presigned URL generation duration",
    ["visibility"],
)
S3_OPERATIONS = Counter(
    "dss_s3_operations_total",
    "S3 operations",
    ["operation", "bucket", "status"],
)
