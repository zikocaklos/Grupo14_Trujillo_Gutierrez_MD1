import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class TransformadorDatos:
    """Clase para transformación y limpieza de datos climáticos"""
    
    @staticmethod
    def validar_tipos(df):
        """Validar y convertir tipos de datos"""
        if df.empty:
            logger.warning("DataFrame vacío en validación de tipos")
            return df
            
        try:
            # Columnas numéricas
            numeric_cols = [
                'temperatura', 'sensacion_termica', 'temp_minima', 'temp_maxima',
                'presion', 'humedad', 'viento_velocidad', 'nubosidad'
            ]
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convertir timestamp
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
            
            if 'fecha_ingesta' in df.columns:
                df['fecha_ingesta'] = pd.to_datetime(df['fecha_ingesta'], errors='coerce')
                
            logger.info("✅ Validación de tipos completada")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error en validación de tipos: {str(e)}")
            return df
    
    @staticmethod
    def limpiar_datos(df):
        """Eliminar duplicados y valores nulos críticos"""
        if df.empty:
            return df
            
        try:
            inicio = len(df)
            
            # Eliminar duplicados
            df = df.drop_duplicates(subset=['ciudad', 'timestamp'], keep='last')
            
            # Eliminar filas con columnas críticas nulas
            df = df.dropna(subset=['ciudad', 'temperatura', 'humedad'])
            
            final = len(df)
            logger.info(f"✅ Limpieza: {inicio} → {final} registros")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error en limpieza: {str(e)}")
            return df
    
    @staticmethod
    def normalizar_valores(df):
        """Normalizar valores numéricos"""
        if df.empty:
            return df
            
        try:
            # Normalizar temperatura (z-score)
            if 'temperatura' in df.columns and 'temperatura_normalizada' not in df.columns:
                mean_temp = df['temperatura'].mean()
                std_temp = df['temperatura'].std()
                if std_temp > 0:
                    df['temperatura_normalizada'] = (df['temperatura'] - mean_temp) / std_temp
                else:
                    df['temperatura_normalizada'] = 0
            
            # Normalizar humedad (0-1)
            if 'humedad' in df.columns and 'humedad_normalizada' not in df.columns:
                df['humedad_normalizada'] = df['humedad'] / 100.0
            
            logger.info("✅ Normalización completada")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error en normalización: {str(e)}")
            return df
    
    @staticmethod
    def enriquecer_datos(df):
        """Crear columnas derivadas"""
        if df.empty:
            return df
            
        try:
            if 'timestamp' in df.columns:
                df['fecha'] = pd.to_datetime(df['timestamp']).dt.date
                df['año'] = pd.to_datetime(df['timestamp']).dt.year
                df['mes'] = pd.to_datetime(df['timestamp']).dt.month
                df['dia'] = pd.to_datetime(df['timestamp']).dt.day
                df['hora'] = pd.to_datetime(df['timestamp']).dt.hour
                df['trimestre'] = pd.to_datetime(df['timestamp']).dt.quarter
                df['semana'] = pd.to_datetime(df['timestamp']).dt.isocalendar().week
            
            # Clasificar clima
            if 'temperatura' in df.columns:
                df['clasificacion_temp'] = pd.cut(
                    df['temperatura'],
                    bins=[-np.inf, 0, 10, 20, 30, np.inf],
                    labels=['Muy Frío', 'Frío', 'Templado', 'Cálido', 'Muy Cálido']
                )
            
            # Calcular índice de confort (simplificado)
            if 'temperatura' in df.columns and 'humedad' in df.columns:
                df['indice_confort'] = df['temperatura'] + (0.5555 * (df['humedad']/100 * 6.112 * 
                                       np.exp((17.67 * df['temperatura']) / (df['temperatura'] + 243.5)) - 10))
            
            logger.info("✅ Enriquecimiento de datos completado")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error en enriquecimiento: {str(e)}")
            return df
    
    @staticmethod
    def generar_reporte_calidad(df):
        """Generar reporte de calidad de datos"""
        if df.empty:
            logger.warning("⚠️ DataFrame vacío - no se puede generar reporte")
            return {}
        
        reporte = {
            'total_registros': len(df),
            'columnas': list(df.columns),
            'valores_nulos': df.isnull().sum().to_dict(),
            'duplicados': df.duplicated().sum(),
            'ciudades_unicas': df['ciudad'].nunique() if 'ciudad' in df.columns else 0,
            'memoria_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'timestamp_generacion': datetime.now().isoformat()
        }
        
        logger.info(f"📊 Reporte de Calidad:\n{reporte}")
        return reporte
    
    @classmethod
    def pipeline_completo(cls, df):
        """Ejecutar pipeline completo de transformación"""
        logger.info("🔄 Iniciando pipeline de transformación...")
        
        df = cls.validar_tipos(df)
        df = cls.limpiar_datos(df)
        df = cls.normalizar_valores(df)
        df = cls.enriquecer_datos(df)
        
        reporte = cls.generar_reporte_calidad(df)
        
        logger.info("✅ Pipeline de transformación completado")
        return df, reporte
