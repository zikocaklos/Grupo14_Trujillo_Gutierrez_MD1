#!/usr/bin/env python3
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy.exc import IntegrityError

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.database import SessionLocal, Base, engine
from scripts.models import Ciudad, RegistroClima, MetricasETL

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DATA_DIR = BASE_DIR / 'data'
INPUT_CSV = DATA_DIR / 'clima_transformado.csv'
INPUT_JSON = DATA_DIR / 'clima_transformado.json'

REQUIRED_COLUMNS = [
    'ciudad', 'pais', 'latitud', 'longitud',
    'temperatura', 'sensacion_termica', 'humedad',
    'velocidad_viento', 'descripcion', 'codigo_tiempo',
    'fecha_extraccion'
]


def crear_tablas_si_no_existen():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info('✅ Tablas creadas/verificadas correctamente')
    except Exception as e:
        logger.error(f'❌ Error creando/verificando tablas: {str(e)}')
        raise


def cargar_transformado():
    if INPUT_CSV.exists():
        df = pd.read_csv(INPUT_CSV)
    elif INPUT_JSON.exists():
        df = pd.read_json(INPUT_JSON)
    else:
        raise FileNotFoundError(
            'No se encontró clima_transformado.csv ni clima_transformado.json en data/. Ejecuta primero scripts/transformador.py'
        )

    df.columns = [col.strip().lower() for col in df.columns]
    return df


def preparar_ciudades(session, df):
    ciudades_unicas = df[[
        'ciudad', 'pais', 'latitud', 'longitud'
    ]].drop_duplicates(subset=['ciudad'])

    nombres = [str(v).strip() for v in ciudades_unicas['ciudad'].tolist()]
    existing = session.query(Ciudad).filter(Ciudad.nombre.in_(nombres)).all()
    existing_map = {c.nombre: c for c in existing}

    nuevas = []
    for _, row in ciudades_unicas.iterrows():
        nombre = str(row['ciudad']).strip()
        if nombre not in existing_map:
            nuevas.append(
                Ciudad(
                    nombre=nombre,
                    pais=str(row['pais']).strip(),
                    latitud=float(row['latitud']),
                    longitud=float(row['longitud'])
                )
            )

    if nuevas:
        session.add_all(nuevas)
        session.commit()
        for ciudad in nuevas:
            existing_map[ciudad.nombre] = ciudad

    return existing_map


def cargar_registros(session, df, ciudades_map):
    registros = []
    for _, row in df.iterrows():
        ciudad_nombre = str(row['ciudad']).strip()
        ciudad = ciudades_map.get(ciudad_nombre)
        if not ciudad:
            logger.warning(f'Ciudad no encontrada en el mapeo: {ciudad_nombre}')
            continue

        fecha_extraccion = pd.to_datetime(row.get('fecha_extraccion'), errors='coerce')
        if pd.isna(fecha_extraccion):
            fecha_extraccion = datetime.utcnow()

        registros.append(
            RegistroClima(
                ciudad_id=ciudad.id,
                temperatura=float(row.get('temperatura') or 0),
                sensacion_termica=float(row.get('sensacion_termica') or 0),
                humedad=float(row.get('humedad') or 0),
                velocidad_viento=float(row.get('velocidad_viento') or 0),
                descripcion=str(row.get('descripcion') or 'Desconocido').strip(),
                codigo_tiempo=int(row.get('codigo_tiempo') or 0),
                fecha_extraccion=fecha_extraccion.to_pydatetime()
            )
        )

    if registros:
        session.bulk_save_objects(registros)
        session.commit()

    return len(registros)


def guardar_metricas(session, extraidos, guardados, fallidos, estado):
    tiempo_ejecucion = (datetime.utcnow() - START_TIME).total_seconds()
    metricas = MetricasETL(
        registros_extraidos=extraidos,
        registros_guardados=guardados,
        registros_fallidos=fallidos,
        tiempo_ejecucion_segundos=tiempo_ejecucion,
        estado=estado,
        mensaje=f'Extraídos: {extraidos}, Guardados: {guardados}, Fallidos: {fallidos}'
    )
    session.add(metricas)
    session.commit()


def main():
    global START_TIME
    START_TIME = datetime.utcnow()

    print('\n' + '=' * 50)
    print('CARGA DE DATOS TRANSFORMADOS A LA BASE DE DATOS')
    print('=' * 50)

    crear_tablas_si_no_existen()

    df = cargar_transformado()
    total_filas = len(df)
    logger.info(f'📁 Datos transformados encontrados: {total_filas} filas')

    session = SessionLocal()
    try:
        ciudades_map = preparar_ciudades(session, df)
        registros_guardados = cargar_registros(session, df, ciudades_map)
        registros_fallidos = total_filas - registros_guardados

        guardar_metricas(session, total_filas, registros_guardados, registros_fallidos,
                         'SUCCESS' if registros_fallidos == 0 else 'PARTIAL')

        print(f'✅ Registros procesados: {total_filas}')
        print(f'✅ Registros guardados: {registros_guardados}')
        print(f'✅ Registros fallidos: {registros_fallidos}')
        print('\n' + '=' * 50 + '\n')

    except Exception as e:
        session.rollback()
        logger.error(f'❌ Error cargando datos en la BD: {str(e)}')
        guardar_metricas(session, total_filas, 0, total_filas, 'FAILED')
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()