from django.http import HttpResponse
from django.shortcuts import render
from pyspark.sql import SparkSession

def HomePage(request):
    return render(request, "home.html")

def Contacto(request):
    return render(request, "contacto.html")

def DashBoard(request):
    # codigoPostal = codigoPostal if codigoPostal else 10
    codigoPostal = 10
    ##instalar java https://www.oracle.com/java/technologies/downloads/?er=221886
    ##agregar java a las variables de entorno path
    ##Descargar los binarios de spark https://spark.apache.org/downloads.html
    ##agregar spark a las variables de entorno 
    spark = SparkSession.builder.appName("GeoApp").getOrCreate()

    data_path = "GeoComercioExplorer\content\CPdescarga.csv"
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    top_5_df = df.limit(5)

    # Convertir a una lista de diccionarios para pasar a la plantilla
    top_5_list = top_5_df.collect()
    top_5_data = [row.asDict() for row in top_5_list]

    # Parar SparkSession
    spark.stop()

    # Renderizar el resultado en una plantilla
    return render(request, 'dashboard.html', {'top_5_data': top_5_data})

# def initializeSpark(name):
#     return SparkSession.builder.appName(name).getOrCreate()