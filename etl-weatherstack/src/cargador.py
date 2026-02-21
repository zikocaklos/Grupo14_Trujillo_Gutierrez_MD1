import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import logging
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class CargadorBD:
    """Clase para cargar datos transformados a PostgreSQL"""
    
    def __init__(self, db_url):
        self.db_url = db_url
        
    def _obtener_conexion(self):
        """Obtener conexión a la BD"""
        try:
            conn = psycopg2.connect(self.db_url)
            logger.info("✅ Conexión a BD establecida")
            return conn
        except psycopg2.Error as e:
            logger.error(f"❌ Error conectando a BD: {str(e)}")
            raise
    
    def cargar_datos_clima(self, df):
        """Cargar datos de clima a la tabla principal"""
        if df.empty:
            logger.warning("⚠️ DataFrame vacío - no hay datos para cargar")
            return 0
        
        try:
            conn = self._obtener_conexion()
            cursor = conn.cursor()
            
            # Preparar datos
            datos = []
            for _, row in df.iterrows():
                datos.append((
                    row.get('ciudad'),
                    row.get('pais'),
                    float(row['temperatura']) if pd.notna(row.get('temperatura')) else None,
                    float(row['sensacion_termica']) if pd.notna(row.get('sensacion_termica')) else None,
                    float(row['temp_minima']) if pd.notna(row.get('temp_minima')) else None,
                    float(row['temp_maxima']) if pd.notna(row.get('temp_maxima')) else None,
                    int(row['presion']) if pd.notna(row.get('presion')) else None,
                    int(row['humedad']) if pd.notna(row.get('humedad')) else None,
                    row.get('descripcion'),
                    float(row['viento_velocidad']) if pd.notna(row.get('viento_velocidad')) else None,
                    int(row['nubosidad']) if pd.notna(row.get('nubosidad')) else None,
                    row.get('timestamp'),
                    row.get('fecha_ingesta')
                ))
            
            # Insertar datos
            insertar_sql = """
                INSERT INTO clima (
                    ciudad, pais, temperatura, sensacion_termica, 
                    temp_minima, temp_maxima, presion, humedad, 
                    descripcion, viento_velocidad, nubosidad, 
                    timestamp, fecha_ingesta
                ) VALUES %s
                ON CONFLICT (ciudad, timestamp) DO UPDATE SET
                    temperatura = EXCLUDED.temperatura,
                    humedad = EXCLUDED.humedad,
                    fecha_ingesta = EXCLUDED.fecha_ingesta
            """
            
            execute_values(cursor, insertar_sql, datos, page_size=100)
            conn.commit()
            
            registros_cargados = len(datos)
            logger.info(f"✅ {registros_cargados} registros de clima cargados en BD")
            
            cursor.close()
            conn.close()
            
            return registros_cargados
            
        except psycopg2.Error as e:
            logger.error(f"❌ Error cargando datos: {str(e)}")
            return 0
    
    def cargar_metricas_ingesta(self, timestamp, registros_procesados, registros_insertados, estado):
        """Registrar métricas de ejecución"""
        try:
            conn = self._obtener_conexion()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO metricas_ingesta (
                    timestamp, registros_procesados, registros_insertados, estado
                ) VALUES (%s, %s, %s, %s)
            """, (timestamp, registros_procesados, registros_insertados, estado))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Métricas registradas")
            
        except psycopg2.Error as e:
            logger.error(f"❌ Error registrando métricas: {str(e)}")
    
    def verificar_conexion(self):
        """Verificar que la BD esté disponible"""
        try:
            conn = self._obtener_conexion()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"✅ BD disponible: {version[0][:50]}...")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ BD no disponible: {str(e)}")
            return False
    
    def crear_tablas_si_no_existen(self):
        """Crear tablas si no existen (respaldo)"""
        try:
            conn = self._obtener_conexion()
            cursor = conn.cursor()
            
            # Leer y ejecutar schema
            with open('/app/sql/schema.sql', 'r') as f:
                schema = f.read()
            
            cursor.execute(schema)
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Esquema verificado/creado")
            
        except Exception as e:
            logger.error(f"⚠️ Nota: {str(e)}")
