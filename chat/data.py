"""MongoDB operations for chat and messages."""

import uuid
import asyncio
import logging
import base64
from typing import Optional, Dict, Any, List
from bson import ObjectId

from django.utils import timezone

from config.mongo import get_mongo_db
from forms import gcp_storage
from .constants import (
    CHATS_COLLECTION, MESSAGES_COLLECTION,
    CHAT_STATUS_DRAFT, FIELD_CHAT_ID, FIELD_USER, FIELD_TITLE, FIELD_STATUS,
    FIELD_CREATED_AT, FIELD_UPDATED_AT, FIELD_ROLE, FIELD_CONTENT, FIELD_ID,
    FIELD_RESPONSE_FILE_URL, FIELD_DATA, FIELD_FILENAME,
    DEFAULT_FILENAME, FILE_PATH_PREFIX, FILE_PATH_MESSAGES, FILE_PATH_RESPONSE_PREFIX
)

logger = logging.getLogger(__name__)


class ChatCollection:
    """MongoDB operations for the chats collection."""
    
    @staticmethod
    async def create_chat(user=None, title: str = "", status: str = CHAT_STATUS_DRAFT) -> Dict[str, Any]:
        """Create a new chat document in MongoDB."""
        collection = get_mongo_db()[CHATS_COLLECTION]
        chat_id = str(uuid.uuid4())
        user_id = str(user.id) if user and getattr(user, "is_authenticated", False) else None
        
        chat_doc = {
            FIELD_ID: chat_id,
            FIELD_USER: user_id,
            FIELD_TITLE: title,
            FIELD_STATUS: status,
            FIELD_CREATED_AT: timezone.now().isoformat(),
            FIELD_UPDATED_AT: timezone.now().isoformat(),
        }
        await asyncio.to_thread(collection.insert_one, chat_doc)
        logger.debug("Created chat document: _id=%s", chat_id)
        return chat_doc


class MessageCollection:
    """MongoDB operations for the messages collection."""
    
    @staticmethod
    def create_message_document(validated_data: Dict[str, Any], chat_id: str) -> Dict[str, Any]:
        """Create a message document ready for MongoDB insertion."""
        return {
            FIELD_CHAT_ID: str(chat_id),
            FIELD_ROLE: validated_data.get(FIELD_ROLE),
            FIELD_CONTENT: validated_data.get(FIELD_CONTENT),
            FIELD_CREATED_AT: timezone.now().isoformat(),
        }
    
    @staticmethod
    async def insert_message(message_doc: Dict[str, Any]) -> ObjectId:
        """Insert a message document into MongoDB."""
        collection = get_mongo_db()[MESSAGES_COLLECTION]
        result = await asyncio.to_thread(collection.insert_one, message_doc)
        logger.debug("Message inserted: _id=%s", result.inserted_id)
        return result.inserted_id
    
    @staticmethod
    async def get_chat_history(chat_id: str, exclude_message_id: Optional[ObjectId] = None) -> List[Dict[str, Any]]:
        """Retrieve chat history (all previous messages) for a given chat_id."""
        collection = get_mongo_db()[MESSAGES_COLLECTION]
        query = {FIELD_CHAT_ID: str(chat_id)}
        if exclude_message_id:
            query[FIELD_ID] = {"$ne": exclude_message_id}
        
        messages = await asyncio.to_thread(
            lambda: list(collection.find(query).sort(FIELD_CREATED_AT, 1))
        )
        
        # Convert ObjectIds to strings and remove internal fields
        history = []
        for msg in messages:
            msg_copy = {**msg, FIELD_ID: str(msg[FIELD_ID])}
            msg_copy.pop(FIELD_RESPONSE_FILE_URL, None)
            history.append(msg_copy)
        return history
    
    @staticmethod
    async def upload_response_file(response_file: Dict[str, Any], message_id: ObjectId, chat_id: str) -> None:
        """Upload response file from FastAPI to GCP storage and update message document."""
        try:
            base64_data = response_file.get(FIELD_DATA) if isinstance(response_file, dict) else response_file
            filename = response_file.get(FIELD_FILENAME, DEFAULT_FILENAME) if isinstance(response_file, dict) else DEFAULT_FILENAME
            
            file_bytes = base64.b64decode(base64_data)
            destination_path = f"{FILE_PATH_PREFIX}/{chat_id}/{FILE_PATH_MESSAGES}/{message_id}/{FILE_PATH_RESPONSE_PREFIX}{filename}"
            pdf_url = await asyncio.to_thread(
                gcp_storage.upload_pdf, file_bytes=file_bytes, destination_path=destination_path
            )
            
            collection = get_mongo_db()[MESSAGES_COLLECTION]
            message_obj_id = message_id if isinstance(message_id, ObjectId) else ObjectId(str(message_id))
            await asyncio.to_thread(
                collection.update_one, {FIELD_ID: message_obj_id}, {"$set": {FIELD_RESPONSE_FILE_URL: pdf_url}}
            )
            logger.debug("Response file uploaded: message_id=%s", message_id)
        except Exception as exc:
            logger.exception("Failed to upload response file: message_id=%s", message_id)
