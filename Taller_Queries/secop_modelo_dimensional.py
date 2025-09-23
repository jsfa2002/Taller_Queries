# secop_modelo_dimensional.py

# -------------------------------
# Librerías
# -------------------------------
import pandas as pd
from sodapy import Socrata
from pyspark.sql import SparkSession
from delta import *

# -------------------------------
# Conexión API
# -------------------------------
client = Socrata("www.datos.gov.co", None)

# Función para descargar datos de forma paginada
def descargar_datos(select_cols, where_clause=None, group=None, limit=50000):
    offset = 0
    resultados = []
    while True:
        batch = client.get(
            "jbjy-vk9h",
            select=select_cols,
            where=where_clause,
            group=group,
            limit=limit,
            offset=offset
        )
        if not batch:
            break
        resultados.extend(batch)
        offset += limit
    return pd.DataFrame.from_records(resultados)

# -------------------------------
# Dimensiones básicas
# -------------------------------
# dim_entidades
df_entidades = descargar_datos("nit_entidad, nombre_entidad, departamento, ciudad, orden")
df_entidades['id_entidad'] = range(1, len(df_entidades) + 1)
df_entidades = df_entidades[['id_entidad', 'nit_entidad', 'nombre_entidad', 'departamento', 'ciudad', 'orden']]

# dim_proveedores
df_proveedores = descargar_datos("documento_proveedor, proveedor_adjudicado, es_pyme")
df_proveedores['id_proveedor'] = range(1, len(df_proveedores) + 1)
df_proveedores = df_proveedores[['id_proveedor', 'documento_proveedor', 'proveedor_adjudicado', 'es_pyme']]

# dim_geografia
df_geografia = df_entidades[['departamento', 'ciudad']].drop_duplicates().reset_index(drop=True)
df_geografia['id_geografia'] = range(1, len(df_geografia) + 1)
df_geografia = df_geografia[['id_geografia', 'departamento', 'ciudad']]

# dim_tipo_contrato
df_tipo = descargar_datos("tipo_modalidad_contrato", group="tipo_modalidad_contrato")
df_tipo['id_tipo_contrato'] = range(1, len(df_tipo) + 1)
df_tipo = df_tipo[['id_tipo_contrato', 'tipo_modalidad_contrato']]

# dim_tiempo
df_tiempo = descargar_datos("fecha_de_firma", group="fecha_de_firma")
df_tiempo['fecha_completa'] = pd.to_datetime(df_tiempo['fecha_de_firma'])
df_tiempo['id_tiempo'] = df_tiempo.index + 1
df_tiempo['anio'] = df_tiempo['fecha_completa'].dt.year
df_tiempo['mes'] = df_tiempo['fecha_completa'].dt.month
df_tiempo['trimestre'] = df_tiempo['fecha_completa'].dt.quarter
df_tiempo['dia_semana'] = df_tiempo['fecha_completa'].dt.day_name()
df_tiempo = df_tiempo[['id_tiempo', 'fecha_completa', 'anio', 'mes', 'trimestre', 'dia_semana']]

# -------------------------------
# Dimensiones adicionales
# -------------------------------
# dim_estado_contrato
df_estado = descargar_datos("estado_contrato, es_liquidado, habilita_pago_adelantado, puede_ser_prorrogado", group="estado_contrato")
df_estado['id_estado_contrato'] = range(1, len(df_estado)+1)
df_estado = df_estado[['id_estado_contrato', 'estado_contrato', 'es_liquidado', 'habilita_pago_adelantado', 'puede_ser_prorrogado']]

# dim_recursos_financieros
df_recursos = descargar_datos("origen_principal, destino_gasto, flag_pgn, flag_regalias, flag_sgp, flag_otros")
df_recursos['id_recurso'] = range(1, len(df_recursos)+1)
df_recursos = df_recursos[['id_recurso', 'origen_principal', 'destino_gasto', 'flag_pgn', 'flag_regalias', 'flag_sgp', 'flag_otros']]

# dim_marcadores_especiales
df_marcadores = descargar_datos("es_postconflicto, obligacion_ambiental, obligacion_postconsumo")
df_marcadores['id_marcador'] = range(1, len(df_marcadores)+1)
df_marcadores = df_marcadores[['id_marcador', 'es_postconflicto', 'obligacion_ambiental', 'obligacion_postconsumo']]

# dim_responsables
df_responsables = descargar_datos("nombre_responsable, tipo_documento, rol_responsable")
df_responsables['id_responsable'] = range(1, len(df_responsables)+1)
df_responsables = df_responsables[['id_responsable', 'nombre_responsable', 'tipo_documento', 'rol_responsable']]

# dim_modalidad_adicional
df_modalidad = descargar_datos("modalidad_contrato, es_tecnologia, es_servicios, es_obra", group="modalidad_contrato")
df_modalidad['id_modalidad'] = range(1, len(df_modalidad)+1)
df_modalidad = df_modalidad[['id_modalidad', 'modalidad_contrato', 'es_tecnologia', 'es_servicios', 'es_obra']]

# dim_tipo_pago
df_pago = descargar_datos("tipo_pago, permite_anticipos, plazo_maximo_pago", group="tipo_pago")
df_pago['id_tipo_pago'] = range(1, len(df_pago)+1)
df_pago = df_pago[['id_tipo_pago', 'tipo_pago', 'permite_anticipos', 'plazo_maximo_pago']]

# dim_entidad_supervision
df_supervision = descargar_datos("nombre_entidad_supervision, tipo_entidad_supervision, departamento, ciudad", group="nombre_entidad_supervision")
df_supervision['id_entidad_supervision'] = range(1, len(df_supervision)+1)
df_supervision = df_supervision[['id_entidad_supervision', 'nombre_entidad_supervision', 'tipo_entidad_supervision', 'departamento', 'ciudad']]

# dim_categoria_contrato
df_categoria = descargar_datos("categoria, subcategoria, es_prioritario", group="categoria")
df_categoria['id_categoria_contrato'] = range(1, len(df_categoria)+1)
df_categoria = df_categoria[['id_categoria_contrato', 'categoria', 'subcategoria', 'es_prioritario']]

# -------------------------------
# Tabla de hechos: fact_contratos
# -------------------------------
df_fact = descargar_datos(
    "id_contrato, nit_entidad, documento_proveedor, departamento, ciudad, tipo_modalidad_contrato, fecha_de_firma, valor_contrato, valor_pagado, dias_adicionados"
)

# Unir FKs
df_fact = df_fact.merge(df_entidades[['id_entidad', 'nit_entidad']], on='nit_entidad', how='left')
df_fact = df_fact.merge(df_proveedores[['id_proveedor', 'documento_proveedor']], on='documento_proveedor', how='left')
df_fact = df_fact.merge(df_geografia, on=['departamento', 'ciudad'], how='left')
df_fact = df_fact.merge(df_tipo, on='tipo_modalidad_contrato', how='left')
df_fact = df_fact.merge(df_tiempo[['id_tiempo', 'fecha_completa']], left_on='fecha_de_firma', right_on='fecha_completa', how='left')

df_fact = df_fact[['id_contrato', 'id_entidad', 'id_proveedor', 'id_geografia', 'id_tipo_contrato', 'id_tiempo', 'valor_contrato', 'valor_pagado', 'dias_adicionados']]

# -------------------------------
# Guardar en Delta Lake con Spark
# -------------------------------
spark = SparkSession.builder.appName("SECOP_Modelo").getOrCreate()

def guardar_delta(df, path):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("overwrite").save(path)

guardar_delta(df_fact, "gold/facts/contratos")
guardar_delta(df_entidades, "gold/dim/entidades")
guardar_delta(df_proveedores, "gold/dim/proveedores")
guardar_delta(df_geografia, "gold/dim/geografia")
guardar_delta(df_tipo, "gold/dim/tipo_contrato")
guardar_delta(df_tiempo, "gold/dim/tiempo")
guardar_delta(df_estado, "gold/dim/estado_contrato")
guardar_delta(df_recursos, "gold/dim/recursos_financieros")
guardar_delta(df_marcadores, "gold/dim/marcadores_especiales")
guardar_delta(df_responsables, "gold/dim/responsables")
guardar_delta(df_modalidad, "gold/dim/modalidad_adicional")
guardar_delta(df_pago, "gold/dim/tipo_pago")
guardar_delta(df_supervision, "gold/dim/entidad_supervision")
guardar_delta(df_categoria, "gold/dim/categoria_contrato")

print("Carga completa en Delta Lake.")

