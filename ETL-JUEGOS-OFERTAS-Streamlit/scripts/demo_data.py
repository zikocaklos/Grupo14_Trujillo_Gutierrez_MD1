#!/usr/bin/env python3
import random
from datetime import datetime
import pandas as pd
import logging

from scripts.database import SessionLocal
from scripts.models import Deal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/demo_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SAMPLE_TITLES = [
    'Aventura Épica',
    'Carreras Extrema',
    'RPG de Fantasía',
    'Simulador Urbano',
    'Táctico por Turnos',
    'Puzzle Psicológico',
    'Shooter Retro',
    'Mundo Abierto',
    'Estrategia Global',
    'Plataformas 3D',
    'Roguelike Clásico',
    'Supervivencia Oscura',
    'Fútbol Arcade',
    'Construcción y Gestión',
    'Cazador de Tesoros'
]

STORE_IDS = [1, 2, 3, 4, 5, 6, 7, 8]


def generar_datos_demo(n=50):
    datos = []

    for i in range(n):
        precio_normal = round(random.uniform(5, 60), 2)
        descuento = round(random.uniform(10, 90), 2)
        precio_oferta = round(precio_normal * (1 - descuento / 100), 2)

        datos.append({
            'titulo': random.choice(SAMPLE_TITLES) + f' #{i+1}',
            'precio_oferta': precio_oferta,
            'precio_normal': precio_normal,
            'ahorro_porcentaje': descuento,
            'store_id': random.choice(STORE_IDS),
            'rating_steam': random.choice([None, round(random.uniform(40, 98), 1)]),
            'metacritic': random.choice([None, random.randint(40, 98)]),
            'fecha_extraccion': datetime.now()
        })

    df = pd.DataFrame(datos)
    return df


def cargar_demo_en_db(df):
    db = SessionLocal()

    for _, row in df.iterrows():
        deal = Deal(
            titulo=row['titulo'],
            precio_oferta=row['precio_oferta'],
            precio_normal=row['precio_normal'],
            ahorro_porcentaje=row['ahorro_porcentaje'],
            store_id=row['store_id'],
            rating_steam=row['rating_steam'],
            metacritic=row['metacritic'],
            fecha_extraccion=row['fecha_extraccion']
        )
        db.add(deal)

    db.commit()
    db.close()
    logger.info('Datos demo cargados en la base de datos')


if __name__ == '__main__':
    df_demo = generar_datos_demo(80)
    df_demo.to_csv('data/demo_deals.csv', index=False)
    df_demo.to_json('data/demo_deals.json', orient='records', indent=2, date_format='iso')

    logger.info('Archivo demo guardado en data/demo_deals.csv y data/demo_deals.json')
    cargar_demo_en_db(df_demo)
    print(df_demo.head())
