from rest_framework import serializers

from .models import Form


class FormSerializer(serializers.ModelSerializer):
    class Meta:
        model = Form
        fields = ("id", "user", "pdf_bucket_url", "created_at", "updated_at")
        read_only_fields = ("id", "created_at", "updated_at")
