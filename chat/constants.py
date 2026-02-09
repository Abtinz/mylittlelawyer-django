"""Constants for chat application."""

# MongoDB Collections
CHATS_COLLECTION = "chats"
MESSAGES_COLLECTION = "messages"

# FastAPI Configuration
FASTAPI_CHAT_ENDPOINT = "http://localhost:8000/ai/chat"
FASTAPI_FORM_ENDPOINT = "http://localhost:8000/ai/file"
FASTAPI_CONSULTANT_ENDPOINT = "http://localhost:8000/ai/consultant"
FASTAPI_TIMEOUT = 30.0

# Chat Status
CHAT_STATUS_DRAFT = "draft"

# Message Fields
FIELD_CHAT_ID = "chat_id"
FIELD_USER = "user"
FIELD_TITLE = "title"
FIELD_STATUS = "status"
FIELD_CREATED_AT = "created_at"
FIELD_UPDATED_AT = "updated_at"
FIELD_ROLE = "role"
FIELD_CONTENT = "content"
FIELD_ID = "_id"
FIELD_RESPONSE = "response"
FIELD_RESPONSE_FILE_URL = "response_file_url"
FIELD_FILE = "file"
FIELD_DATA = "data"
FIELD_FILENAME = "filename"
FIELD_FORM = "form"

# FastAPI Payload Fields
FIELD_SESSION_ID = "session_id"
FIELD_MESSAGE = "message" 
FIELD_CHAT_HISTORY = "chat_history"
FIELD_FORM = "form"

# WebSocket Response Types
RESPONSE_TYPE_CHAT_CREATED = "chat.created"
RESPONSE_OK = "ok"
RESPONSE_ERRORS = "errors"
FIELD_MESSAGE = "message"

# Error Messages
ERROR_INVALID_JSON = "invalid_json"
ERROR_INVALID_PAYLOAD = "invalid_payload"
ERROR_CHAT_INIT_FAILED = "chat_init_failed"
ERROR_MESSAGE_INSERT_FAILED = "message_insert_failed"

# File Paths
FILE_PATH_PREFIX = "chat"
FILE_PATH_MESSAGES = "messages"
FILE_PATH_RESPONSE_PREFIX = "response_"
DEFAULT_FILENAME = "response.pdf"

# HTTP Status
HTTP_OK = 200
HTTP_ERROR = 500
