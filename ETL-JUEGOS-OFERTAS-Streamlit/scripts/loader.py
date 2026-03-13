import pandas as pd
from scripts.database import SessionLocal
from scripts.models import Deal

def cargar_datos(df):

    db = SessionLocal()

    for _, row in df.iterrows():

        deal = Deal(
            titulo=row["titulo"],
            precio_oferta=row["precio_oferta"],
            precio_normal=row["precio_normal"],
            ahorro_porcentaje=row["ahorro_porcentaje"],
            store_id=row["store_id"],
            rating_steam=row["rating_steam"],
            metacritic=row["metacritic"]
        )

        db.add(deal)

    db.commit()
    db.close()

    print("Datos cargados en PostgreSQL")