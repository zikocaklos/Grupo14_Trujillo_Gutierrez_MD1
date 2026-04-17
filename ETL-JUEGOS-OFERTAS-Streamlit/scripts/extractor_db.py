#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import text
from scripts.database import engine


def leer_deals_db(limit=None):
    sql = 'SELECT * FROM deals ORDER BY fecha_extraccion DESC'
    if limit is not None:
        sql += ' LIMIT :limit'
        return pd.read_sql(text(sql), engine, params={'limit': limit})
    return pd.read_sql(sql, engine)


def leer_deals_por_categoria(categoria, limit=100):
    sql = '''
    SELECT *
    FROM deals
    WHERE ahorro_porcentaje >= :min_descuento
    ORDER BY ahorro_porcentaje DESC
    LIMIT :limit
    '''
    parametros = {
        'min_descuento': 20 if categoria == 'Medio' else 50 if categoria == 'Alto' else 80,
        'limit': limit
    }
    return pd.read_sql(text(sql), engine, params=parametros)


def resumen_estadistico():
    sql = '''
    SELECT
        COUNT(*) AS total_deals,
        AVG(precio_oferta) AS precio_oferta_promedio,
        AVG(precio_normal) AS precio_normal_promedio,
        AVG(ahorro_porcentaje) AS ahorro_promedio
    FROM deals
    '''
    return pd.read_sql(sql, engine)


if __name__ == '__main__':
    df = leer_deals_db(20)
    print('Registros leídos:', len(df))
    print(df.head(10))
    print('\nResumen estadístico:')
    print(resumen_estadistico())
