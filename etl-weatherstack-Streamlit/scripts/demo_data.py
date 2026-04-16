#!/usr/bin/env python3
import os
import json
import random
from datetime import datetime, timedelta
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')

CITIES = [
    {'ciudad': 'Bogota', 'pais': 'Colombia', 'latitud': 4.7110, 'longitud': -74.0721, 'temp': 18, 'humedad': 75, 'viento': 10},
    {'ciudad': 'Medellin', 'pais': 'Colombia', 'latitud': 6.2442, 'longitud': -75.5812, 'temp': 22, 'humedad': 70, 'viento': 9},
    {'ciudad': 'Cali', 'pais': 'Colombia', 'latitud': 3.4516, 'longitud': -76.5320, 'temp': 26, 'humedad': 75, 'viento': 8},
    {'ciudad': 'Barranquilla', 'pais': 'Colombia', 'latitud': 10.9685, 'longitud': -74.7813, 'temp': 29, 'humedad': 80, 'viento': 12},
    {'ciudad': 'Cartagena', 'pais': 'Colombia', 'latitud': 10.3910, 'longitud': -75.4794, 'temp': 31, 'humedad': 82, 'viento': 13},
    {'ciudad': 'Lima', 'pais': 'Peru', 'latitud': -12.0464, 'longitud': -77.0428, 'temp': 20, 'humedad': 78, 'viento': 11},
    {'ciudad': 'Miami', 'pais': 'USA', 'latitud': 25.7617, 'longitud': -80.1918, 'temp': 28, 'humedad': 78, 'viento': 14},
    {'ciudad': 'Mexico City', 'pais': 'Mexico', 'latitud': 19.4326, 'longitud': -99.1332, 'temp': 24, 'humedad': 65, 'viento': 9},
    {'ciudad': 'Madrid', 'pais': 'Spain', 'latitud': 40.4168, 'longitud': -3.7038, 'temp': 17, 'humedad': 60, 'viento': 8},
    {'ciudad': 'London', 'pais': 'UK', 'latitud': 51.5074, 'longitud': -0.1278, 'temp': 14, 'humedad': 70, 'viento': 12},
    {'ciudad': 'Tokyo', 'pais': 'Japan', 'latitud': 35.6895, 'longitud': 139.6917, 'temp': 22, 'humedad': 68, 'viento': 7},
    {'ciudad': 'Sao Paulo', 'pais': 'Brazil', 'latitud': -23.5505, 'longitud': -46.6333, 'temp': 25, 'humedad': 80, 'viento': 10},
]

WEATHER_CONDITIONS = [
    {'codigo': 113, 'descripcion': 'Sunny', 'peso': 20},
    {'codigo': 116, 'descripcion': 'Partly cloudy', 'peso': 18},
    {'codigo': 119, 'descripcion': 'Cloudy', 'peso': 14},
    {'codigo': 122, 'descripcion': 'Overcast', 'peso': 10},
    {'codigo': 176, 'descripcion': 'Patchy rain nearby', 'peso': 10},
    {'codigo': 266, 'descripcion': 'Light drizzle', 'peso': 6},
    {'codigo': 293, 'descripcion': 'Light rain', 'peso': 5},
    {'codigo': 302, 'descripcion': 'Moderate rain', 'peso': 4},
    {'codigo': 308, 'descripcion': 'Heavy rain', 'peso': 3},
    {'codigo': 200, 'descripcion': 'Thundery outbreaks', 'peso': 2},
    {'codigo': 392, 'descripcion': 'Patchy snow showers', 'peso': 2},
    {'codigo': 395, 'descripcion': 'Heavy snow', 'peso': 1},
]


def generar_registro(index):
    ciudad = random.choice(CITIES)
    temperatura = round(random.gauss(ciudad['temp'], 4), 1)
    sensacion_termica = round(temperatura + random.gauss(0, 2), 1)
    humedad = int(min(100, max(15, random.gauss(ciudad['humedad'], 12))))
    velocidad_viento = round(abs(random.gauss(ciudad['viento'], 5)), 1)

    opciones = [item['codigo'] for item in WEATHER_CONDITIONS]
    pesos = [item['peso'] for item in WEATHER_CONDITIONS]
    codigo = random.choices(opciones, pesos, k=1)[0]
    descripcion = next(item['descripcion'] for item in WEATHER_CONDITIONS if item['codigo'] == codigo)

    fecha_extraccion = datetime.now() - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    return {
        'ciudad': ciudad['ciudad'],
        'pais': ciudad['pais'],
        'latitud': ciudad['latitud'],
        'longitud': ciudad['longitud'],
        'temperatura': temperatura,
        'sensacion_termica': sensacion_termica,
        'humedad': humedad,
        'velocidad_viento': velocidad_viento,
        'descripcion': descripcion,
        'codigo_tiempo': codigo,
        'fecha_extraccion': fecha_extraccion.isoformat()
    }


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    registros = [generar_registro(i) for i in range(1000)]

    raw_json_path = os.path.join(DATA_DIR, 'clima_raw.json')
    raw_csv_path = os.path.join(DATA_DIR, 'clima.csv')

    with open(raw_json_path, 'w', encoding='utf-8') as f:
        json.dump(registros, f, indent=2, ensure_ascii=False)

    df = pd.DataFrame(registros)
    df.to_csv(raw_csv_path, index=False)

    print('\n' + '=' * 50)
    print('1000 registros sintéticos generados correctamente')
    print(f'JSON: {raw_json_path}')
    print(f'CSV: {raw_csv_path}')
    print('=' * 50 + '\n')


if __name__ == '__main__':
    main()
