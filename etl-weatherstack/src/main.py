import os
import sys
import logging
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Importar módulos locales
from ingestor import IngestorClima
from transformador import TransformadorDatos
from cargador import CargadorBD

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PipelineClima:
    """Orquestador del pipeline completo de datos climáticos"""
    
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY', 'demo')
        self.db_url = os.getenv('DATABASE_URL', 'postgresql://dataminer:secure_pass@postgres:5432/datawarehouse')
        self.ingestor = IngestorClima(self.api_key)
        self.cargador = CargadorBD(self.db_url)
        self.intervalo = int(os.getenv('INGESTION_INTERVAL', 3600))
    
    def ejecutar_pipeline(self):
        """Ejecutar pipeline completo: ingesta → transformación → carga"""
        inicio = datetime.now()
        logger.info("=" * 80)
        logger.info(f"🚀 Iniciando pipeline en {inicio.isoformat()}")
        logger.info("=" * 80)
        
        try:
            # 1. Ingestar datos
            logger.info("📥 Fase 1: Ingesta desde API...")
            df_raw = self.ingestor.obtener_datos()
            registros_ingesta = len(df_raw)
            
            if df_raw.empty:
                logger.warning("⚠️ No se obtuvieron datos de la API")
                self.cargador.cargar_metricas_ingesta(
                    inicio, 0, 0, 'error_ingesta'
                )
                return
            
            logger.info(f"✅ Se ingirieron {registros_ingesta} registros")
            
            # 2. Transformar datos
            logger.info("🔄 Fase 2: Transformación de datos...")
            df_transformado, reporte = TransformadorDatos.pipeline_completo(df_raw)
            registros_transformados = len(df_transformado)
            
            # 3. Cargar a BD
            logger.info("💾 Fase 3: Carga a PostgreSQL...")
            registros_cargados = self.cargador.cargar_datos_clima(df_transformado)
            
            # 4. Registrar métricas
            self.cargador.cargar_metricas_ingesta(
                inicio, registros_ingesta, registros_cargados, 'exitoso'
            )
            
            # 5. Guardar CSV local (respaldo)
            self.ingestor.guardar_csv_local(df_transformado, '/app/logs/datos_transformados.csv')
            
            # Resumen
            fin = datetime.now()
            duracion = (fin - inicio).total_seconds()
            
            logger.info("=" * 80)
            logger.info("✅ PIPELINE COMPLETADO")
            logger.info(f"   Registros ingesta: {registros_ingesta}")
            logger.info(f"   Registros transformados: {registros_transformados}")
            logger.info(f"   Registros cargados: {registros_cargados}")
            logger.info(f"   Duración: {duracion:.2f} segundos")
            logger.info(f"   Fin: {fin.isoformat()}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline: {str(e)}", exc_info=True)
            self.cargador.cargar_metricas_ingesta(
                inicio, 0, 0, f'error: {str(e)}'
            )
    
    def verificar_bd(self):
        """Verificar conectividad a BD antes de iniciar"""
        logger.info("🔍 Verificando conectividad a BD...")
        intentos = 0
        while not self.cargador.verificar_conexion() and intentos < 5:
            intentos += 1
            logger.warning(f"⏳ Reintentando conexión ({intentos}/5)...")
            time.sleep(2)
        
        if intentos == 5:
            logger.error("❌ No se pudo conectar a BD. Abortando.")
            return False
        
        return True
    
    def agendar_ingesta(self):
        """Agendar ejecución periódica"""
        logger.info(f"📅 Agendando ingesta cada {self.intervalo} segundos")
        schedule.every(self.intervalo).seconds.do(self.ejecutar_pipeline)
        
        logger.info("⏱️  Iniciando iteración continua. Presiona Ctrl+C para detener.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n⛔ Pipeline detenido por el usuario")


def main():
    """Función principal"""
    pipeline = PipelineClima()
    
    # Verificar BD
    if not pipeline.verificar_bd():
        sys.exit(1)
    
    # Ejecutar una vez inmediatamente
    pipeline.ejecutar_pipeline()
    
    # Iniciar agendamiento para ejecuciones periódicas
    pipeline.agendar_ingesta()


if __name__ == "__main__":
    main()
