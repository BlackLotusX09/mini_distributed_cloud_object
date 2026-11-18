"""Shared API error types and the single, centralized exception -> JSON mapping.

Every non-2xx response is {"error": CODE, "message": text}.
"""
import logging

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger("controller")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class ObjectNotFound(ApiError):
    def __init__(self, object_id: str):
        super().__init__(404, "OBJECT_NOT_FOUND", f"object '{object_id}' not found")


class ObjectBusy(ApiError):
    def __init__(self, object_id: str):
        super().__init__(409, "OBJECT_BUSY", f"object '{object_id}' is still being uploaded")


class StorageUnavailable(ApiError):
    def __init__(self, message: str):
        super().__init__(503, "STORAGE_UNAVAILABLE", message)


class DataUnavailable(ApiError):
    def __init__(self, message: str):
        super().__init__(503, "DATA_UNAVAILABLE", message)


class DataCorrupt(ApiError):
    def __init__(self, message: str):
        super().__init__(500, "DATA_CORRUPT", message)


class InvalidMetadata(ApiError):
    def __init__(self, message: str):
        super().__init__(400, "INVALID_METADATA", message)


class InvalidPagination(ApiError):
    def __init__(self, message: str):
        super().__init__(400, "INVALID_PAGINATION", message)


class InvalidInput(ApiError):
    def __init__(self, message: str):
        super().__init__(400, "INVALID_INPUT", message)


def error_response(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message})


HTTP_CODES = {400: "INVALID_REQUEST", 404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED",
              413: "PAYLOAD_TOO_LARGE"}


def install_handlers(app) -> None:
    """The one registry mapping every exception type to a JSON error."""
    from placement import InsufficientWorkersError  # local: keeps placement import-free

    @app.exception_handler(ApiError)  # covers every typed domain exception above
    async def _api_error(_req, exc: ApiError):
        return error_response(exc.status, exc.code, exc.message)

    @app.exception_handler(InsufficientWorkersError)
    async def _insufficient(_req, exc):
        return error_response(
            503, "INSUFFICIENT_WORKERS", f"need {exc.needed} healthy workers, have {exc.available}")

    @app.exception_handler(RequestValidationError)
    async def _validation(_req, exc):
        return error_response(400, "INVALID_REQUEST", "malformed request")

    @app.exception_handler(StarletteHTTPException)
    async def _http(_req, exc):
        return error_response(exc.status_code, HTTP_CODES.get(exc.status_code, "HTTP_ERROR"), str(exc.detail))

    @app.exception_handler(Exception)
    async def _unexpected(_req, exc):
        log.exception("unhandled error")
        return error_response(500, "INTERNAL_ERROR", "Internal server error")
