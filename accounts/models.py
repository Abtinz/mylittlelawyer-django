import uuid

from django.db import models
from django.db.models.functions import Lower

class User(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    full_name = models.TextField()
    email = models.EmailField()
    password_hash = models.TextField(blank=True, null=True)
    phone = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "users"
        constraints = [
            models.UniqueConstraint(Lower("email"), name="users_email_ci_unique"),
        ]

    def __str__(self) -> str:
                    return self.full_name
