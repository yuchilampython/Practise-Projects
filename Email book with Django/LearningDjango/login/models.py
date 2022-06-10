from django.db import models

# Create your models here.
class UserInfo(models.Model):
    username = models.CharField(max_length=32)
    firstname = models.CharField(max_length=32)
    lastname = models.CharField(max_length=32)
    email = models.EmailField(max_length=255)
    password = models.CharField(max_length=64)
