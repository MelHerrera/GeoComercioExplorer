from django.http import HttpResponse
from django.shortcuts import render
from pyspark.sql import SparkSession
from django.http import JsonResponse
from pyspark.sql import functions as F
from pyspark.sql.functions import upper,expr
from pyspark.sql.types import IntegerType, DoubleType, LongType, StringType,StructField,StructType
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import os
import sys
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.sql.types import GeometryType
from shapely import wkt
import geopandas as gpd
import folium

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

# # schema = StructType([
#     StructField("id", LongType(), True),
#     StructField("clee", StringType(), True),
#     StructField("nom_estab", StringType(), True),
#     StructField("raz_social", StringType(), True),
#     StructField("codigo_act", StringType(), True),
#     StructField("nombre_act", StringType(), True),
#     StructField("per_ocu", StringType(), True),
#     StructField("tipo_vial", StringType(), True),
#     StructField("nom_vial", StringType(), True),
#     StructField("tipo_v_e_1", StringType(), True),
#     StructField("nom_v_e_1", StringType(), True),
#     StructField("tipo_v_e_2", StringType(), True),
#     StructField("nom_v_e_2", StringType(), True),
#     StructField("tipo_v_e_3", StringType(), True),
#     StructField("nom_v_e_3", StringType(), True),
#     StructField("numero_ext", StringType(), True),
#     StructField("letra_ext", StringType(), True),
#     StructField("edificio", StringType(), True),
#     StructField("edificio_e", StringType(), True),
#     StructField("numero_int", StringType(), True),
#     StructField("letra_int", StringType(), True),
#     StructField("tipo_asent", StringType(), True),
#     StructField("nomb_asent", StringType(), True),
#     StructField("tipoCenCom", StringType(), True),
#     StructField("nom_CenCom", StringType(), True),
#     StructField("num_local", StringType(), True),
#     StructField("cod_postal", StringType(), True),
#     StructField("cve_ent", StringType(), True),
#     StructField("entidad", StringType(), True),
#     StructField("cve_mun", StringType(), True),
#     StructField("municipio", StringType(), True),
#     StructField("cve_loc", StringType(), True),
#     StructField("localidad", StringType(), True),
#     StructField("ageb", StringType(), True),
#     StructField("manzana", StringType(), True),
#     StructField("telefono", StringType(), True),
#     StructField("correoelec", StringType(), True),
#     StructField("www", StringType(), True),
#     StructField("tipoUniEco", StringType(), True),
#     StructField("latitud", DoubleType(), True),
#     StructField("longitud", DoubleType(), True),
#     StructField("fecha_alta", StringType(), True),
#     StructField("geometry", GeometryType(), True)
# ])

# bd_denue = spark.read.schema(schema).parquet("GeoComercioExplorer/content/DENUE_Parquets/01.parquet")

data_path = "GeoComercioExplorer\content\CPdescarga.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)
    
# Corregir el tipo de dato de codigo y estado
columnas_corregir = ["d_codigo", "c_estado"]

for c in columnas_corregir:
    df = df.withColumn(c, F.col(c).cast(IntegerType()))

# agregar las transformaciones a la cache
df.cache()


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
# try:
#     print(bd_denue.count())
# except Exception as e:
#     print("Error:", e)


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

def Get_actividades(request,codigo_postal, radio, actividad_to_search):
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
    bd_denue = load_parquetby_estado(estados)
    bd_denue = bd_denue.filter(bd_denue.geometry.isNotNull())
    bd_denue.cache()

    actividades = (bd_denue
                    .filter(F.upper(bd_denue.nom_estab).like(f"%{ actividad_to_search.upper() }%"))
                    .groupBy("codigo_act")
                    .agg(F.count("*").alias("n_comercios"))
                    .orderBy(F.desc("n_comercios"))
                    .toPandas())

    comercios = bd_denue.join(negocios_radio, bd_denue["cod_postal"] == negocios_radio["CP"]) \
                 .filter(upper(bd_denue["nom_estab"]).like('%' + actividad_to_search.upper() + '%')) \
                 .filter(bd_denue["codigo_act"].like('%' + actividades.codigo_act[0] + '%')) \
                 .select(
                     bd_denue["clee"].alias("CVEGEO"),
                     bd_denue["nom_estab"],
                     bd_denue["codigo_act"],
                     bd_denue["nombre_act"],
                     bd_denue["latitud"],
                     bd_denue["longitud"],
                     expr("1 as n_Comercios"),
                     bd_denue["cod_postal"],
                     expr("ST_AsText(geometry) AS geometry")
                 ) \
                 .toPandas()
    
    comercios['geometry'] = comercios['geometry'].apply(wkt.loads)
    comercios_SHP = gpd.GeoDataFrame(comercios, geometry="geometry")
    comercios_SHP = comercios_SHP.set_crs('PROJCS["Mexico_ITRF2008_LCC",GEOGCS["Mexico_ITRF2008",DATUM["Mexico_ITRF2008",SPHEROID["GRS_1980",6378137,298.257222101],TOWGS84[0,0,0,0,0,0,0]],PRIMEM["Greenwich",0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP",AUTHORITY["EPSG","9802"]],PARAMETER["Central_Meridian",-102],PARAMETER["Latitude_Of_Origin",12],PARAMETER["False_Easting",2500000],PARAMETER["False_Northing",0],PARAMETER["Standard_Parallel_1",17.5],PARAMETER["Standard_Parallel_2",29.5],PARAMETER["Scale_Factor",1],UNIT["Meter",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","6372"]]')
    comercios_SHP = comercios_SHP.to_crs("EPSG:4326")
    
    comerciosB = bd_denue.join(negocios_radio, bd_denue["cod_postal"] == negocios_radio["CP"]) \
                  .filter(~upper(bd_denue["nom_estab"]).like('%' + actividad_to_search.upper() + '%')) \
                  .filter(bd_denue["codigo_act"].like('%' + actividades.codigo_act[0] + '%')) \
                  .select(
                      bd_denue["clee"].alias("CVEGEO"),
                      bd_denue["nom_estab"],
                      bd_denue["codigo_act"],
                      bd_denue["nombre_act"],
                      bd_denue["latitud"],
                      bd_denue["longitud"],
                      expr("1 as n_Comercios"),
                      bd_denue["cod_postal"],
                      expr("ST_AsText(geometry) AS geometry")
                  ) \
                  .toPandas()
    
    comerciosB['geometry'] = comerciosB['geometry'].apply(wkt.loads)
    comerciosB_SHP = gpd.GeoDataFrame(comerciosB, geometry="geometry")
    comerciosB_SHP = comerciosB_SHP.set_crs('PROJCS["Mexico_ITRF2008_LCC",GEOGCS["Mexico_ITRF2008",DATUM["Mexico_ITRF2008",SPHEROID["GRS_1980",6378137,298.257222101],TOWGS84[0,0,0,0,0,0,0]],PRIMEM["Greenwich",0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP",AUTHORITY["EPSG","9802"]],PARAMETER["Central_Meridian",-102],PARAMETER["Latitude_Of_Origin",12],PARAMETER["False_Easting",2500000],PARAMETER["False_Northing",0],PARAMETER["Standard_Parallel_1",17.5],PARAMETER["Standard_Parallel_2",29.5],PARAMETER["Scale_Factor",1],UNIT["Meter",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","6372"]]')
    comerciosB_SHP = comerciosB_SHP.to_crs("EPSG:4326")

    # Mapa con folum
    map_html = getfolium_map(lat1, lon1, distancia_km, codigo_postal, comercios_SHP, comerciosB_SHP)

    # Mapa con google maps
    markers_comercios = "["
    for _, registro in comercios_SHP.iterrows():
        markers_comercios=markers_comercios + '{lat: ' + str(registro["latitud"]) + ", lng:" + str(registro["longitud"]) + ", title:'" + str(registro["nom_estab"]) + "', label:'" + str(registro["cod_postal"])+"'},"
    
    markers_comercios = markers_comercios[:-1]
    markers_comercios = markers_comercios + "]"

    # print(markers_comercios)

    markers_competencia = "["
    for _, registro in comerciosB_SHP.iterrows():
        markers_competencia= markers_competencia + '{lat: ' + str(registro["latitud"]) + ", lng:" + str(registro["longitud"]) + ", title:'" + str(registro["nom_estab"]) + "', label:'" + str(registro["cod_postal"])+"'},"
    
    markers_competencia = markers_competencia[:-1]
    markers_competencia = markers_competencia + "]"

    #iteramos sobre negocios_radio
    markers_comercios_radio = "["
    for row in negocios_radio.collect(): # Use .collect() to get the data as a list of Row objects
        markers_comercios_radio= markers_comercios_radio + '{lat: ' + str(row["Latitud"]) + ", lng:" + str(row["Longitud"]) + ", title:'CP " + str(row["CP"]) + "', label:'CP " + str(row["CP"])+"'},"
    
    markers_comercios_radio = markers_comercios_radio[:-1]
    markers_comercios_radio = markers_comercios_radio + "]"


    # Convertir a una lista de diccionarios para pasar a la plantilla
    codigo_buscado_data = codigo_buscado.to_dict(orient='records')
    
    data = { 
        'Message': 'Operacion Exitosa!',
          'codigo_buscado_data':codigo_buscado_data, 
          'radio':radio,
          'map_html':map_html
        #   'map_html': {
        #       'lat1':lat1,
        #       'lon1':lon1,
        #       'markers_comercios':markers_comercios,
        #       'markers_competencia':markers_competencia,
        #       'markers_comercios_radio':markers_comercios_radio
        #   }
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


def getfolium_map(lat_center,long_center,distance_km, postal_code,comercios_radio, comercios_competidores):
    m3 = folium.Map(location=[lat_center,long_center], zoom_start=13)

    folium.Choropleth(
    geo_data=comercios_radio.to_json(drop_id=True),
    name="Comercios",
    data=comercios_radio,
    columns=["CVEGEO", "n_Comercios"],
    key_on="feature.properties.CVEGEO",
    fill_color="Spectral",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Comercios",
    ).add_to(m3)

    folium.Circle(
        location=[lat_center, long_center],
        radius=distance_km * 1000,  # Radio en metros
        color='blue',
        fill=True,
        fill_color='blue'
        ).add_to(m3)

    #Icono de estrella para comercios buscados
    for _, registro in comercios_radio.iterrows():
        folium.Marker([registro["latitud"], registro["longitud"]], popup=registro["nom_estab"], icon=folium.Icon(color="blue", icon="star")).add_to(m3)

    #Icono de warning para competidores
    for _, registro in comercios_competidores.iterrows():
        folium.Marker([registro["latitud"], registro["longitud"]], popup=registro["nom_estab"], icon=folium.Icon(color="red", icon="warning-sign")).add_to(m3)

    #Icono de punto central del código postal buscado
    folium.Marker([lat_center, long_center],  popup= "C.P. " + postal_code, icon=folium.Icon(color="green", icon="info-sign")).add_to(m3)

    return m3._repr_html_()