import requests
import pandas as pd
from datetime import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

logger = logging.getLogger(__name__)


class IngestorClima:
    """Clase para ingestar datos de clima desde API OpenWeather"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.ciudades = os.getenv('CITIES', 'Bogotá,Medellín,Cali,Barranquilla').split(',')
        self.sesion = self._crear_sesion_robusta()
        
    def _crear_sesion_robusta(self):
        """Crear sesión con reintentos automáticos"""
        sesion = requests.Session()
        reintentos = Retry(
            total=int(os.getenv('MAX_RETRIES', 3)),
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adaptador = HTTPAdapter(max_retries=reintentos)
        sesion.mount('http://', adaptador)
        sesion.mount('https://', adaptador)
        return sesion
    
    def obtener_datos(self):
        """Obtener datos de la API de OpenWeather"""
        datos = []
        timeout = int(os.getenv('API_TIMEOUT', 10))
        
        for ciudad in self.ciudades:
            try:
                logger.info(f"Ingiriendo datos para: {ciudad}")
                response = self.sesion.get(
                    self.base_url,
                    params={
                        'q': ciudad.strip(),
                        'appid': self.api_key,
                        'units': 'metric'
                    },
                    timeout=timeout
                )
                response.raise_for_status()
                
                if response.status_code == 200:
                    datos_json = response.json()
                    datos.append({
                        'ciudad': datos_json.get('name', ciudad),
                        'pais': datos_json.get('sys', {}).get('country', 'N/A'),
                        'temperatura': datos_json.get('main', {}).get('temp'),
                        'sensacion_termica': datos_json.get('main', {}).get('feels_like'),
                        'temp_minima': datos_json.get('main', {}).get('temp_min'),
                        'temp_maxima': datos_json.get('main', {}).get('temp_max'),
                        'presion': datos_json.get('main', {}).get('pressure'),
                        'humedad': datos_json.get('main', {}).get('humidity'),
                        'descripcion': datos_json.get('weather', [{}])[0].get('description', ''),
                        'viento_velocidad': datos_json.get('wind', {}).get('speed'),
                        'nubosidad': datos_json.get('clouds', {}).get('all'),
                        'timestamp': datos_json.get('dt'),
                        'fecha_ingesta': datetime.now()
                    })
                    logger.info(f"✅ Datos obtenidos para {ciudad}")
                    
            except requests.exceptions.Timeout:
                logger.error(f"❌ Timeout conectando con {ciudad}")
            except requests.exceptions.ConnectionError:
                logger.error(f"❌ Error de conexión con {ciudad}")
            except requests.exceptions.HTTPError as e:
                if response.status_code == 401:
                    logger.error(f"❌ API Key inválida para {ciudad}")
                elif response.status_code == 404:
                    logger.error(f"❌ Ciudad no encontrada: {ciudad}")
                else:
                    logger.error(f"❌ Error HTTP {response.status_code} para {ciudad}")
            except Exception as e:
                logger.error(f"❌ Error inesperado ingiriendo {ciudad}: {str(e)}")
        
        return pd.DataFrame(datos) if datos else pd.DataFrame()
    
    def guardar_csv_local(self, df, path='datos_ingesta.csv'):
        """Guardar datos en CSV local"""
        try:
            df.to_csv(path, index=False)
            logger.info(f"✅ {len(df)} registros guardados en {path}")
        except Exception as e:
            logger.error(f"❌ Error guardando CSV: {str(e)}")
