from django.contrib import admin

from .models import Form

@admin.register(Form)
class FormAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "pdf_bucket_url", "created_at", "updated_at")
    search_fields = ("id", "pdf_bucket_url", "user__email")
    list_filter = ("created_at",)
    ordering = ("-created_at",)
