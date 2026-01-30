from django.contrib.auth.hashers import check_password, make_password
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .authentication import UserJWTAuthentication
from .models import User
from .serializers import LoginSerializer, RegisterSerializer


class RegisterView(APIView):
    def post(self, request):
        serializer = RegisterSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        user = User.objects.create(
            email=serializer.validated_data["email"],
            password_hash=make_password(serializer.validated_data["password"]),
        )
        access_token = UserJWTAuthentication.create_access_token(user_id=str(user.id))
        return Response(
            {
                "id": str(user.id),
                "email": user.email,
                "token": str(access_token)
            },
            status=status.HTTP_201_CREATED,
        )


class LoginView(APIView):
    def post(self, request):
        serializer = LoginSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        user = User.objects.filter(email=serializer.validated_data["email"]).first()
        if not user or not user.password_hash:
            return Response({"detail": "Invalid credentials."}, status=status.HTTP_401_UNAUTHORIZED)

        if not check_password(serializer.validated_data["password"], user.password_hash):
            return Response({"detail": "Invalid credentials."}, status=status.HTTP_401_UNAUTHORIZED)

        access_token = UserJWTAuthentication.create_access_token(user_id=str(user.id))
        return Response(
            {
                "token": str(access_token), 
                "user_id": str(user.id), 
                "email": user.email
            }
        )


class LogoutView(APIView):
    authentication_classes = [UserJWTAuthentication]

    def post(self, request):
        return Response(status=status.HTTP_204_NO_CONTENT)


class DeleteAccountView(APIView):
    authentication_classes = [UserJWTAuthentication]

    def delete(self, request):
        user: User = request.user
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
