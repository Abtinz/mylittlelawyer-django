"""FastAPI client for chat AI service."""

import logging
from typing import Optional, Dict, Any, List

import httpx
from django.conf import settings

from .constants import (
    FASTAPI_TIMEOUT,
    FIELD_CHAT_ID,
    FIELD_NEW_MESSAGE, FIELD_CHAT_HISTORY, FIELD_FORM,
    FIELD_SESSION_ID,
    HTTP_ERROR
)

logger = logging.getLogger(__name__)


class ErrorResponse:
    """Mock response object for FastAPI errors."""
    
    def __init__(self, error_message: str):
        self.status_code = HTTP_ERROR
        self.text = error_message
    
    def json(self) -> Dict[str, str]:
        return {"error": self.text}


class FastAPIClient:
    """Client for communicating with FastAPI chat service."""
    
    @staticmethod
    async def send_chat_request(
        endpoint: str,
        new_message: Dict[str, Any], 
        session_id: str,
        chat_history: Optional[List[Dict[str, Any]]] = None,
        # form: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        """
        Send message and chat history to FastAPI service and get AI response.
        
        Args:
            new_message: Current message document
            chat_history: List of previous messages (optional)
            form: Form data (optional)
            
        Returns:
            httpx.Response object (or ErrorResponse mock on failure)
        """
        payload = {
            FIELD_SESSION_ID: session_id,
            FIELD_NEW_MESSAGE: new_message,
            FIELD_CHAT_HISTORY: chat_history if chat_history else None,
            # FIELD_FORM: form
        }

        try:
            async with httpx.AsyncClient(timeout=FASTAPI_TIMEOUT) as client:
                return await client.post(endpoint, json=payload)
        except httpx.RequestError as exc:
            logger.exception("Failed to send request to FastAPI service")
            return ErrorResponse(str(exc))

    
