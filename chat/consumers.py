import json
import uuid
import urllib.parse
import asyncio
import logging
from channels.generic.websocket import AsyncWebsocketConsumer
from django.utils import timezone

logger = logging.getLogger(__name__) 

from config.mongo import get_mongo_db
from .serializers import MessageSerializer


class ChatConsumer(AsyncWebsocketConsumer):
    async def write(self, payload):
        """Send a JSON payload to the client.

        Ensure any Mongo ObjectId in `message["_id"]` is stringified for JSON.
        """
        message = payload.get("message")
        if isinstance(message, dict) and "_id" in message:
            m = dict(message)
            m["_id"] = str(m["_id"])
            payload = dict(payload)
            payload["message"] = m
        return await self.send(json.dumps(payload))

    async def connect(self):
        # Chat id will be provided in the message payload; initialize empty session value
        self.chat_id = None
        await self.accept()
        resp = {"type": "chat.created"}
        await self.write(resp) 

    async def receive(self, text_data):
        """Handle incoming websocket messages: validate, persist, and respond.

        Chat id is read from the payload's `chat_id` field. If missing, use an
        existing session `self.chat_id` or create a new UUID and attach it to
        the payload and session.
        """
        payload, err = self._parse_json_payload(text_data)
        if err:
            return await self.write({"errors": err})

        chat_id = self._get_or_create_chat_id(payload)

        serializer = MessageSerializer(data=payload)
        if not serializer.is_valid():
            logger.debug("Message validation failed: %s", serializer.errors)
            return await self.write({"errors": serializer.errors})

        v = serializer.validated_data
        doc = self._create_message_doc(v, chat_id)

        try:
            inserted_id = await self._insert_message(doc)
        except Exception as exc:
            logger.exception("Failed to insert message into Mongo")
            return await self.write({"errors": str(exc)})

        # Ensure the message's `_id` is JSON serializable
        doc["_id"] = str(inserted_id)

        return await self.write({"ok": True, "message": doc })

    async def chat_messages(self, event):
        return await self.write({"message": event.get("message")})



    def _parse_chat_id(self):
        qs = self.scope.get("query_string", b"").decode()
        params = urllib.parse.parse_qs(qs)
        val = (params.get("chat") or params.get("chat_id") or params.get("chatId") or [None])[0]
        try:
            return uuid.UUID(val) if val else uuid.uuid4()
        except Exception:
            return uuid.uuid4()

    def _parse_json_payload(self, text_data):
        try:
            payload = json.loads(text_data)
        except json.JSONDecodeError:
            return None, "invalid_json"

        if not isinstance(payload, dict):
            return None, "invalid_payload"

        return payload, None

    def _get_or_create_chat_id(self, payload):
        """Return chat_id from payload or session; create and persist in session when missing."""
        chat = payload.get("chat_id")
        if chat:
            chat_id = str(chat)
            self.chat_id = chat_id
            payload["chat_id"] = chat_id
            logger.debug("Using provided chat_id: %s", chat_id)
            return chat_id

        if getattr(self, "chat_id", None):
            payload["chat_id"] = str(self.chat_id)
            logger.debug("Using existing session chat_id: %s", self.chat_id)
            return str(self.chat_id)

        new_chat = str(uuid.uuid4())
        self.chat_id = new_chat
        payload["chat_id"] = new_chat
        logger.debug("Created new chat_id: %s", new_chat)
        return new_chat

    def _create_message_doc(self, validated_data, chat_id):
        """Create a document ready for Mongo insertion from validated serializer data."""
        return {
            "chat_id": str(chat_id),
            "role": validated_data.get("role"),
            "content": validated_data.get("content"),
            "created_at": timezone.now().isoformat(),
        }

    async def _insert_message(self, doc):
        """Insert doc into Mongo in a thread and return the inserted_id."""
        coll = get_mongo_db()["messages"]
        logger.debug("Inserting message into Mongo for chat %s: %s", doc.get("chat_id"), doc)
        message_result = await asyncio.to_thread(coll.insert_one, doc)
        logger.debug("Inserted message with _id=%s", message_result.inserted_id)
        return message_result.inserted_id