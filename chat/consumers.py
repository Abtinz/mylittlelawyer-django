"""WebSocket consumer for chat functionality."""

import json
import uuid
import asyncio
import logging
from typing import Optional, Dict, Any, Tuple

from channels.generic.websocket import AsyncWebsocketConsumer

from .serializers import MessageSerializer
from .data import ChatCollection, MessageCollection
from .fastapi_client import FastAPIClient
from .constants import (
    FASTAPI_CHAT_ENDPOINT,
    FASTAPI_CONSULTANT_ENDPOINT,
    # FASTAPI_FORM_ENDPOINT,
    # FIELD_FORM,
    RESPONSE_TYPE_CHAT_CREATED, RESPONSE_OK, RESPONSE_ERRORS,
    ERROR_INVALID_JSON, ERROR_INVALID_PAYLOAD, HTTP_OK,
    FIELD_ID, FIELD_CHAT_ID, FIELD_MESSAGE, FIELD_RESPONSE, FIELD_FILE
)

logger = logging.getLogger(__name__)


class ChatConsumer(AsyncWebsocketConsumer):
    """WebSocket consumer for handling chat connections and messages."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.chat_id: Optional[str] = None
    
    async def connect(self):
        """Handle WebSocket connection: create chat document in MongoDB."""
        await self.accept()
        try:
            chat_doc = await ChatCollection.create_chat(user=self.scope.get("user"))
            self.chat_id = str(chat_doc[FIELD_ID])
            await self._send_json({
                "type": RESPONSE_TYPE_CHAT_CREATED,
                FIELD_CHAT_ID: self.chat_id
            })
        except Exception:
            logger.exception("Failed to create chat document on connection")
            await self._send_json({RESPONSE_ERRORS: "chat_init_failed"})
    
    async def receive(self, text_data: str):
        """Handle incoming WebSocket message."""
        payload, error = self._parse_json(text_data)
        if error:
            return await self._send_json({RESPONSE_ERRORS: error})
        
        chat_id = self._resolve_chat_id(payload)
        serializer = MessageSerializer(data=payload)
        if not serializer.is_valid():
            print(f"Validation errors: {serializer.errors}")
            return await self._send_json({RESPONSE_ERRORS: serializer.errors})
        
        # Create and persist message
        message_doc = MessageCollection.create_message_document(serializer.validated_data, chat_id)
        try:
            message_id = await MessageCollection.insert_message(message_doc)
        except Exception:
            logger.exception("Failed to insert message")
            return await self._send_json({RESPONSE_ERRORS: "message_insert_failed"})
        
        message_doc[FIELD_ID] = str(message_id)
        
        # Fetch history and get form data
        history = await MessageCollection.get_chat_history(chat_id, exclude_message_id=message_id)
        chat_history = history if history else None
        session_id = chat_id
        new_message = message_doc["content"]
        # form_data = payload.get("form")

        # Get AI response from FastAPI
        fastapi_response = await FastAPIClient.send_chat_request(
            endpoint=FASTAPI_CONSULTANT_ENDPOINT,
            message=new_message,
            chat_history=chat_history,
            refresh_index=True
            # form=form_data
        )

        print(f"FastAPI raw response: {fastapi_response}")
        
        if fastapi_response.status_code != HTTP_OK:
             return await self._send_json({RESPONSE_ERRORS: fastapi_response})

        # Create and persist message from the FastAPI response
        message_doc = MessageCollection.create_message_document(fastapi_response.json(), chat_id)
        try:
            message_id = await MessageCollection.insert_message(message_doc)
        except Exception:
            logger.exception("Failed to insert message")
            return await self._send_json({RESPONSE_ERRORS: "message_insert_failed"})    
       
        response_data = fastapi_response.json()
        message_doc[FIELD_RESPONSE] = response_data
        
        # Handle file upload if present
        # if response_data.get(FIELD_FILE):
        #     asyncio.create_task(
        #         MessageCollection.upload_response_file(response_data[FIELD_FILE], message_id, chat_id)
        #     )
        # # Send the file and also persist the form in mongodb and key to it in postgres
        # message_doc = await MessageCollection.upload_response_file(message_doc, message_doc[FIELD_ID], chat_id)
        # fastapi_response = await FastAPIClient.send_chat_request(
        #     endpoint=FASTAPI_FORM_ENDPOINT,
        #     new_message=message_doc,
        #     chat_history=chat_history,
        #     form=form_data
        # )
        
        await self._send_json({RESPONSE_OK: True, FIELD_MESSAGE: message_doc})
    
    async def chat_messages(self, event):
        """Handle channel layer events for chat messages."""
        await self._send_json({FIELD_MESSAGE: event.get(FIELD_MESSAGE)})
    
    # utility functions

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        """Send JSON payload to WebSocket client, converting ObjectIds to strings."""
        message = payload.get(FIELD_MESSAGE)
        if isinstance(message, dict) and FIELD_ID in message:
            payload = {**payload, FIELD_MESSAGE: {**message, FIELD_ID: str(message[FIELD_ID])}}
        await self.send(json.dumps(payload))
    
    def _parse_json(self, text_data: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Parse JSON string and validate it's a dictionary."""
        try:
            payload = json.loads(text_data)
            return (payload, None) if isinstance(payload, dict) else (None, ERROR_INVALID_PAYLOAD)
        except json.JSONDecodeError:
            return None, ERROR_INVALID_JSON
    
    def _resolve_chat_id(self, payload: Dict[str, Any]) -> str:
        """Resolve chat_id from payload or use existing session chat_id."""
        if provided_chat_id := payload.get(FIELD_CHAT_ID):
            chat_id = str(provided_chat_id)
            self.chat_id = chat_id
            payload[FIELD_CHAT_ID] = chat_id
            return chat_id
        if self.chat_id:
            payload[FIELD_CHAT_ID] = self.chat_id
            return self.chat_id
        return self.chat_id or str(uuid.uuid4())
