from django.http import HttpResponse
from django.shortcuts import render
from pyspark.sql import SparkSession
from django.http import JsonResponse
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

def initSparkApp(name):
    return SparkSession.builder.appName(name).getOrCreate()

spark = initSparkApp("GeoApp")

data_path = "GeoComercioExplorer\content\CPdescarga.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)
    
# Corregir el tipo de dato de codigo y estado
columnas_corregir = ["d_codigo", "c_estado"]

for c in columnas_corregir:
    df = df.withColumn(c, F.col(c).cast(IntegerType()))

# agregar las transformaciones a la cache
df.cache()


def HomePage(request):
    return render(request, "home.html")

def Contacto(request):
    return render(request, "contacto.html")

def DashBoard(request):
    ##mostrar el top 5
    top_5_df = df.limit(5)

    # Convertir a una lista de diccionarios para pasar a la plantilla
    top_5_list = top_5_df.collect()
    top_5_data = [row.asDict() for row in top_5_list]

    # Renderizar el resultado en una plantilla
    return render(request, 'dashboard.html', {'top_5_data': top_5_data})

def Get_actividades(request,codigo_postal):
    codigo_buscado = df.select(F.col("d_codigo").alias("CP"),
                                F.col("Latitud"),
                                F.col("Longitud"),
                                F.col("c_estado")
                                ).filter(
                                    F.col("d_codigo") == codigo_postal
                                )
    
    codigo_buscado_list = codigo_buscado.collect()
    codigo_buscado_data = [row.asDict() for row in codigo_buscado_list]
    
    data = { 'Message': 'Operacion Exitosa!', 'code1':codigo_buscado_data}
    return JsonResponse(data)
