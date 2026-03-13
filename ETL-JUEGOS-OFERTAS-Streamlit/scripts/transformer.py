import pandas as pd

def transformar_datos(df):

    # limpiar valores nulos
    df = df.dropna()

    # convertir tipos
    df["precio_oferta"] = df["precio_oferta"].astype(float)
    df["precio_normal"] = df["precio_normal"].astype(float)

    # calcular ahorro real
    df["ahorro_real"] = df["precio_normal"] - df["precio_oferta"]

    # clasificar descuento
    df["categoria_descuento"] = pd.cut(
        df["ahorro_porcentaje"],
        bins=[0,20,50,80,100],
        labels=["Bajo","Medio","Alto","Oferta brutal"]
    )

    return df