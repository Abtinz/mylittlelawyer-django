import uuid
from django.db import models


class Chat(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey("accounts.User", on_delete=models.CASCADE, related_name="chats")
    title = models.CharField(max_length=255, blank=True)
    status = models.CharField(max_length=32, default="draft")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "chats"

    def __str__(self) -> str:
        return self.title or f"Chat {self.id}"


class Message(models.Model):
    ROLE_CHOICES = [
        ("user", "User"),
        ("chatbot", "Chatbot"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    chat = models.ForeignKey("chat.Chat", on_delete=models.CASCADE, related_name="messages")
    role = models.CharField(max_length=16, choices=ROLE_CHOICES)
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "chat_messages"

    def __str__(self) -> str:
        return f"{self.role}: {self.content[:50]}"
