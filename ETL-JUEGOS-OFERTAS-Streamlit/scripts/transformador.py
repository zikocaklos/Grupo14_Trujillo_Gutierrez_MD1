import pandas as pd


def transformar_datos(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    df = df.dropna(subset=['titulo', 'precio_oferta', 'precio_normal'])

    df['precio_oferta'] = pd.to_numeric(df['precio_oferta'], errors='coerce')
    df['precio_normal'] = pd.to_numeric(df['precio_normal'], errors='coerce')
    df['ahorro_porcentaje'] = pd.to_numeric(df.get('ahorro_porcentaje', pd.Series()), errors='coerce')

    df = df.dropna(subset=['precio_oferta', 'precio_normal'])

    df.loc[df['precio_normal'] > 0, 'ahorro_real'] = (
        df['precio_normal'] - df['precio_oferta']
    )

    df.loc[df['precio_normal'] > 0, 'ahorro_porcentaje'] = (
        ((df['precio_normal'] - df['precio_oferta']) / df['precio_normal']) * 100
    ).round(2)

    df['categoria_descuento'] = pd.cut(
        df['ahorro_porcentaje'],
        bins=[-1, 20, 50, 80, 100],
        labels=['Bajo', 'Medio', 'Alto', 'Oferta brutal']
    ).astype(str)

    df['precio_relacion'] = (df['precio_oferta'] / df['precio_normal']).round(3)
    df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'], errors='coerce')

    df = df.sort_values(by='fecha_extraccion', ascending=False)
    df = df.reset_index(drop=True)

    return df


def guardar_transformado(df: pd.DataFrame, filename='data/deals_transformados.csv') -> None:
    df.to_csv(filename, index=False)
