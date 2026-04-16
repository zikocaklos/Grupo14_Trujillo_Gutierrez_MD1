#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
INPUT_CSV = os.path.join(DATA_DIR, 'clima.csv')
INPUT_JSON = os.path.join(DATA_DIR, 'clima_raw.json')
OUTPUT_CSV = os.path.join(DATA_DIR, 'clima_transformado.csv')
OUTPUT_JSON = os.path.join(DATA_DIR, 'clima_transformado.json')

CATEGORIA_TEMPERATURA = [
    (10, 'Muy frío'),
    (18, 'Frío'),
    (24, 'Templado'),
    (30, 'Cálido'),
    (100, 'Muy cálido')
]

ESTADO_POR_CODIGO = {
    113: 'Despejado',
    116: 'Parcialmente nublado',
    119: 'Nublado',
    122: 'Nublado',
    176: 'Lluvia ligera',
    266: 'Lluvia ligera',
    293: 'Lluvia ligera',
    302: 'Lluvia moderada',
    308: 'Lluvia fuerte',
    200: 'Tormenta',
    386: 'Tormenta',
    389: 'Tormenta',
    392: 'Nieve ligera',
    395: 'Nieve fuerte'
}


def cargar_datos():
    if os.path.exists(INPUT_CSV):
        df = pd.read_csv(INPUT_CSV)
    elif os.path.exists(INPUT_JSON):
        df = pd.read_json(INPUT_JSON)
    else:
        raise FileNotFoundError(
            'No se encontró clima.csv ni clima_raw.json en el directorio data. Ejecuta primero scripts/demo_data.py'
        )

    df.columns = [col.strip().lower() for col in df.columns]
    return df


def normalizar_texto(texto):
    if pd.isna(texto):
        return 'Desconocido'
    return str(texto).strip().title()


def categoria_temperatura(valor):
    if pd.isna(valor):
        return 'Desconocido'
    for limite, categoria in CATEGORIA_TEMPERATURA:
        if valor <= limite:
            return categoria
    return 'Desconocido'


def estado_por_codigo(codigo):
    try:
        codigo_int = int(codigo)
    except Exception:
        return 'Desconocido'
    return ESTADO_POR_CODIGO.get(codigo_int, 'Variable')


def limpiar_y_transformar(df):
    df = df.copy()

    for columna in ['ciudad', 'pais', 'descripcion']:
        if columna in df.columns:
            df[columna] = df[columna].apply(normalizar_texto)

    if 'fecha_extraccion' in df.columns:
        df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'], errors='coerce')
    else:
        df['fecha_extraccion'] = pd.to_datetime(datetime.now())

    for columna in ['temperatura', 'sensacion_termica', 'humedad', 'velocidad_viento', 'codigo_tiempo', 'latitud', 'longitud']:
        if columna in df.columns:
            df[columna] = pd.to_numeric(df[columna], errors='coerce')

    required_columns = ['ciudad', 'pais', 'latitud', 'longitud', 'temperatura', 'sensacion_termica', 'humedad', 'velocidad_viento', 'descripcion', 'codigo_tiempo', 'fecha_extraccion']
    df = df[[col for col in required_columns if col in df.columns]]

    df = df.drop_duplicates().reset_index(drop=True)
    df = df[df['ciudad'].notna()]

    for columna in ['temperatura', 'sensacion_termica', 'humedad', 'velocidad_viento', 'latitud', 'longitud', 'codigo_tiempo']:
        if columna in df.columns:
            valor_medio = df[columna].median(skipna=True)
            df[columna] = df[columna].fillna(valor_medio)

    if 'descripcion' in df.columns:
        df['descripcion'] = df['descripcion'].replace({'nan': 'Desconocido'}).fillna('Desconocido')

    df['temperatura_categoria'] = df['temperatura'].apply(categoria_temperatura)
    df['estado_clima'] = df['codigo_tiempo'].apply(estado_por_codigo)
    df['clima_severo'] = df.apply(
        lambda row: bool(row['velocidad_viento'] >= 35 or row['estado_clima'] in ['Tormenta', 'Nieve fuerte']),
        axis=1
    )

    return df


def guardar_transformado(df):
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    df.to_json(OUTPUT_JSON, orient='records', force_ascii=False, indent=2)

    print('\n' + '=' * 50)
    print('Transformación completada con éxito')
    print(f'Archivo CSV transformado: {OUTPUT_CSV}')
    print(f'Archivo JSON transformado: {OUTPUT_JSON}')
    print('=' * 50 + '\n')
    print(df[['ciudad', 'fecha_extraccion', 'temperatura', 'humedad', 'estado_clima', 'temperatura_categoria']].head(10).to_string(index=False))


if __name__ == '__main__':
    df = cargar_datos()
    df_transformado = limpiar_y_transformar(df)
    guardar_transformado(df_transformado)
