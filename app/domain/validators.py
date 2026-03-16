from app.domain.exceptions import FileSizeExceededError, InvalidContentTypeError


def validate_content_type(content_type: str, allowed: list[str]) -> None:
    if content_type not in allowed:
        raise InvalidContentTypeError(
            f"Content type '{content_type}' is not allowed. Allowed: {allowed}"
        )


def validate_file_size(size_bytes: int, max_size_mb: int) -> None:
    max_bytes = max_size_mb * 1024 * 1024
    if size_bytes > max_bytes:
        raise FileSizeExceededError(
            f"File size {size_bytes} bytes exceeds limit of {max_size_mb} MB"
        )
