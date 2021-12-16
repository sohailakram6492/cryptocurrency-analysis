from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User

from RuntimeGraphs import models

from .models import *

# Create your forms here.

class NewUserForm(UserCreationForm):
	username = forms.EmailField(required=True)
	class Meta:
		model = User
		fields = ("username", "password1", "password2" )


  

	def save(self, commit=True):
		user = super(NewUserForm, self).save(commit=False)
		user.username = self.cleaned_data['username']
		if commit:
			user.save()
		return user


class PriceSearchForm(forms.Form):
        date_from = forms.DateField(widget=forms.DateInput(attrs={'type': 'date'}))
        date_to = forms.DateField(widget=forms.DateInput(attrs={'type': 'date'}))


class RegisterForm(forms.ModelForm):
    class Meta:
        model = register
        fields = ['name',  'Main_Img']