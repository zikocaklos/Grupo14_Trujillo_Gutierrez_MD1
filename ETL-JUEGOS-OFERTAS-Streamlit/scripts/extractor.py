#!/usr/bin/env python3
import requests
import pandas as pd
import json
import logging
from datetime import datetime
import time

from scripts.database import SessionLocal
from scripts.models import Deal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class CheapSharkExtractor:

    def __init__(self):
        self.base_url = "https://www.cheapshark.com/api/1.0/deals"
        self.params = {
            "storeID": "1",
            "upperPrice": "30",
            "pageSize": "30",
            "sortBy": "Savings"
        }

    def extraer_deals(self):

        try:
            response = requests.get(self.base_url, params=self.params, timeout=10)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", 5)
                logger.warning(f"Rate limit alcanzado. Esperando {retry_after} segundos...")
                time.sleep(int(retry_after))
                return []

            response.raise_for_status()
            logger.info("Datos extraídos correctamente de CheapShark")

            return response.json()

        except Exception as e:
            logger.error(f"Error al consumir API: {e}")
            return []

    def procesar_datos(self, deals):

        datos = []

        for deal in deals:

            datos.append({
                "titulo": deal.get("title"),
                "precio_oferta": float(deal.get("salePrice")),
                "precio_normal": float(deal.get("normalPrice")),
                "ahorro_porcentaje": float(deal.get("savings")),
                "store_id": deal.get("storeID"),
                "rating_steam": deal.get("steamRatingPercent"),
                "metacritic": deal.get("metacriticScore"),
                "fecha_extraccion": datetime.now()
            })

        return datos


if __name__ == "__main__":

    extractor = CheapSharkExtractor()

    deals = extractor.extraer_deals()

    datos = extractor.procesar_datos(deals)

    if datos:

        df = pd.DataFrame(datos)

        df.to_csv("data/deals.csv", index=False)

        logger.info("CSV guardado en data/deals.csv")

        with open("data/deals_raw.json", "w") as f:
            json.dump(datos, f, indent=2, default=str)

        logger.info("JSON guardado en data/deals_raw.json")

        # guardar en base de datos

        db = SessionLocal()

        for row in datos:

            deal = Deal(
                titulo=row["titulo"],
                precio_oferta=row["precio_oferta"],
                precio_normal=row["precio_normal"],
                ahorro_porcentaje=row["ahorro_porcentaje"],
                store_id=row["store_id"],
                rating_steam=row["rating_steam"],
                metacritic=row["metacritic"],
                fecha_extraccion=row["fecha_extraccion"]
            )

            db.add(deal)

        db.commit()
        db.close()

        logger.info("Datos guardados en PostgreSQL")

        print("\nResumen de datos extraídos:\n")

        print(df.head())