from django.http import HttpResponse
from django.shortcuts import render

def HomePage(request):
    return render(request, "home.html")

def DashBoard(request):
    # codigoPostal = codigoPostal if codigoPostal else 10
    codigoPostal = 10
    return render(request, "dashboard.html")