from django.http import HttpResponse
from django.shortcuts import render
from pyspark.sql import SparkSession
from django.http import JsonResponse
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType, StringType,StructField,StructType
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import os
import sys
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.sql.types import GeometryType

os.environ['PYSPARK_PYTHON'] = sys.executable

def initSparkApp(name):
    return SparkSession.builder \
        .appName(name). \
config("spark.serializer", KryoSerializer.getName). \
config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.2.0-incubating,'
           'org.datasyslab:geotools-wrapper:1.1.0-25.2') \
        .getOrCreate()

# negocios_radio = None
# bd_denue = None
spark = initSparkApp("GeoBigData")
SedonaRegistrator.registerAll(spark)

schema = StructType([
    StructField("id", LongType(), True),
    StructField("clee", StringType(), True),
    StructField("nom_estab", StringType(), True),
    StructField("raz_social", StringType(), True),
    StructField("codigo_act", StringType(), True),
    StructField("nombre_act", StringType(), True),
    StructField("per_ocu", StringType(), True),
    StructField("tipo_vial", StringType(), True),
    StructField("nom_vial", StringType(), True),
    StructField("tipo_v_e_1", StringType(), True),
    StructField("nom_v_e_1", StringType(), True),
    StructField("tipo_v_e_2", StringType(), True),
    StructField("nom_v_e_2", StringType(), True),
    StructField("tipo_v_e_3", StringType(), True),
    StructField("nom_v_e_3", StringType(), True),
    StructField("numero_ext", StringType(), True),
    StructField("letra_ext", StringType(), True),
    StructField("edificio", StringType(), True),
    StructField("edificio_e", StringType(), True),
    StructField("numero_int", StringType(), True),
    StructField("letra_int", StringType(), True),
    StructField("tipo_asent", StringType(), True),
    StructField("nomb_asent", StringType(), True),
    StructField("tipoCenCom", StringType(), True),
    StructField("nom_CenCom", StringType(), True),
    StructField("num_local", StringType(), True),
    StructField("cod_postal", StringType(), True),
    StructField("cve_ent", StringType(), True),
    StructField("entidad", StringType(), True),
    StructField("cve_mun", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("cve_loc", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("ageb", StringType(), True),
    StructField("manzana", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("correoelec", StringType(), True),
    StructField("www", StringType(), True),
    StructField("tipoUniEco", StringType(), True),
    StructField("latitud", DoubleType(), True),
    StructField("longitud", DoubleType(), True),
    StructField("fecha_alta", StringType(), True),
    StructField("geometry", GeometryType(), True)
])

bd_denue = spark.read.schema(schema).parquet("GeoComercioExplorer/content/DENUE_Parquets/01.parquet")

# data_path = "GeoComercioExplorer\content\CPdescarga.csv"
# df = spark.read.csv(data_path, header=True, inferSchema=True)
    
# # Corregir el tipo de dato de codigo y estado
# columnas_corregir = ["d_codigo", "c_estado"]

# for c in columnas_corregir:
#     df = df.withColumn(c, F.col(c).cast(IntegerType()))

# # agregar las transformaciones a la cache
# df.cache()


# data_testing =[("James ","","Smith","36636","M",3000),
#               ("Michael ","Rose","","40288","M",4000),
#               ("Robert ","","Williams","42114","M",4000),
#               ("Maria ","Anne","Jones","39192","F",4000),
#               ("Jen","Mary","Brown","","F",-1)]
# columns=["firstname","middlename","lastname","dob","gender","salary"]
# df_parquet=spark.createDataFrame(data_testing,columns)
# df_parquet.write.mode("overwrite").parquet("/tmp/output/p.parquet")

# df_final = spark.read.parquet("/tmp/output/p.parquet")


# Depurando
try:
    print(bd_denue.count())
except Exception as e:
    print("Error:", e)


def HomePage(request):
    return render(request, "home.html")

def Contacto(request):
    return render(request, "contacto.html")

def DashBoard(request):
    ##mostrar el top 5
    top_5_list = df.limit(5).collect()

    # Convertir a una lista de diccionarios para pasar a la plantilla
    top_5_data = [row.asDict() for row in top_5_list]

    # Renderizar el resultado en una plantilla
    return render(request, 'dashboard.html', {'top_5_data': top_5_data})

def Get_actividades(request,codigo_postal, radio):
    codigo_buscado = df.select(F.col("d_codigo").alias("CP"),
                                F.col("Latitud"),
                                F.col("Longitud"),
                                F.col("c_estado")
                                ).filter(
                                    F.col("d_codigo") == codigo_postal
                                ).toPandas()
    
    data = []

    lat1 = float(codigo_buscado.Latitud[0])
    lon1 = float(codigo_buscado.Longitud[0])
    distancia_km = float(radio)

    for row in df.toLocalIterator():
        lat2 = row["Latitud"]
        lon2 = row["Longitud"]

        resultado = geodesic((lat1, lon1), (lat2, lon2))

        if resultado <= distancia_km:
            data.append((row.d_codigo,row.Latitud,row.Longitud,row.c_estado))

    
    # Columnas del DataFrame
    columns = ["CP", "Latitud", "Longitud","Entidad_fed"]
    negocios_radio = spark.createDataFrame(data, columns)
    negocios_radio.cache()

    #seleccionamos los valores distintos de entidad federativa y los asignamos a la variable estados
    estados = negocios_radio.select(F.col("Entidad_fed")).distinct().toPandas()
    # bd_denue = load_parquetby_estado(estados)
    # bd_denue_path = f"GeoComercioExplorer/content/DENUE_Parquets/0{estados.Entidad_fed[0]}.parquet"
    bd_denue = spark.read.parquet(f"C:\tmp\output\p.parquet")

    # Depurando
    try:
        # print(bd_denue_path)
        print(bd_denue.count())
    except Exception as e:
        print("Error:", e)

    codigo_buscado_data = codigo_buscado.to_dict(orient='records')
    
    data = { 
        'Message': 'Operacion Exitosa!',
          'codigo_buscado_data':codigo_buscado_data, 
          'radio':radio
    }
    return JsonResponse(data)

def load_parquetby_estado(estados):
    if estados.Entidad_fed[0] == 15:
        BD_DENUE = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/{estados.Entidad_fed[0]}_1.parquet")
        BD_DENUE1 = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/{estados.Entidad_fed[0]}_2.parquet")
        BD_DENUE = BD_DENUE.union(BD_DENUE1)
    else:
        if estados.Entidad_fed[0]<10:
            BD_DENUE = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/0{estados.Entidad_fed[0]}.parquet")
        else:
            BD_DENUE = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/{estados.Entidad_fed[0]}.parquet")

    for estado in estados.Entidad_fed[1:]:
        if estado == 15:
            BD_DENUE1 = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/{estado}_1.parquet")
            BD_DENUE = BD_DENUE.union(BD_DENUE1)
            BD_DENUE1 = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/{estado}_2.parquet")
            BD_DENUE = BD_DENUE.union(BD_DENUE1)
        else:
            if estado<10:
                BD_DENUE1 = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/0{estado}.parquet")
                BD_DENUE = BD_DENUE.union(BD_DENUE1)
            else:
                BD_DENUE1 = spark.read.parquet(f"GeoComercioExplorer/content/DENUE_Parquets/{estado}.parquet")
                BD_DENUE = BD_DENUE.union(BD_DENUE1)
                
    return BD_DENUE