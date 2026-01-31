from django.urls import path

from .views import SaveFormView, UpdateFormView, UserFormsListView

urlpatterns = [
    path("", UserFormsListView.as_view(), name="user-forms-list"),
    path("save/", SaveFormView.as_view(), name="forms-save"),
    path("<uuid:form_id>/update/", UpdateFormView.as_view(), name="forms-update"),
]
