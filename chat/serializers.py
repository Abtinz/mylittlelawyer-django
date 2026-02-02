from rest_framework import serializers
import uuid
from django.utils import timezone

ROLE_CHOICES = [
    ("user", "User"),
    ("chatbot", "Chatbot"),
]


class ChatSerializer(serializers.Serializer):
    id = serializers.UUIDField(read_only=True)
    user = serializers.UUIDField()
    title = serializers.CharField(max_length=255, allow_blank=True, required=False)
    status = serializers.CharField(max_length=32, default="draft", required=False)
    created_at = serializers.DateTimeField(read_only=True)
    updated_at = serializers.DateTimeField(read_only=True)

    def create(self, validated_data):
        """Return a plain dict representing a Chat (no DB persistence)."""
        data = dict(validated_data)
        data.setdefault("id", uuid.uuid4())
        data.setdefault("title", "")
        data.setdefault("status", "draft")
        now = timezone.now()
        data.setdefault("created_at", now)
        data.setdefault("updated_at", now)
        return data

    def update(self, instance, validated_data):
        """Update the dict-like instance and return it."""
        if isinstance(instance, dict):
            instance.update(validated_data)
            instance["updated_at"] = timezone.now()
            return instance
        raise TypeError("Expected a dict-like instance for update")


class MessageSerializer(serializers.Serializer):
    ROLE_CHOICES = ROLE_CHOICES

    id = serializers.UUIDField(read_only=True)
    # chat = serializers.UUIDField()
    role = serializers.CharField(max_length=16, default="user")
    content = serializers.CharField(max_length=1000)
    created_at = serializers.DateTimeField(read_only=True)

    def validate_role(self, value):
        if value not in dict(self.ROLE_CHOICES):
            raise serializers.ValidationError("Invalid role")
        return value

    def create(self, validated_data):
        """Return a plain dict representing a Message (no DB persistence)."""
        data = dict(validated_data)
        data.setdefault("id", uuid.uuid4())
        data.setdefault("created_at", timezone.now())
        return data

    def to_representation(self, instance):
        """Ensure UUIDs and datetimes are serialized to strings."""
        if isinstance(instance, dict):
            out = dict(instance)
            if isinstance(out.get("id"), uuid.UUID):
                out["id"] = str(out["id"])
            if hasattr(out.get("created_at"), "isoformat"):
                out["created_at"] = out["created_at"].isoformat()
            return out
        return super().to_representation(instance)
