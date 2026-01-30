import re

from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from rest_framework import serializers


class RegisterSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField(min_length=8, write_only=True)
    phone = serializers.CharField(required=False, allow_blank=True, write_only=True)

    def validate_email(self, value: str) -> str:
        from .models import User

        if User.objects.filter(email=value).exists():
            raise serializers.ValidationError("Email is already registered.")
        return value

    def validate_password(self, value: str) -> str:
        try:
            validate_password(value)
        except ValidationError as exc:
            raise serializers.ValidationError(exc.messages)
        return value

    def validate_phone(self, value: str) -> str:
        if not value:
            return value

        pattern = re.compile(r"^\+?[0-9][0-9\s().-]{6,}$")
        if not pattern.match(value):
            raise serializers.ValidationError("Invalid phone number format.")
        return value


class LoginSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField(write_only=True)
