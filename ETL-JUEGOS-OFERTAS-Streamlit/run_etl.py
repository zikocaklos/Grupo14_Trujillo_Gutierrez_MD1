from scripts.extractor import CheapSharkExtractor
from scripts.transformador import transformar_datos
from scripts.loader import cargar_datos

import pandas as pd

def run_pipeline():

    extractor = CheapSharkExtractor()

    deals = extractor.extraer_deals()

    datos = extractor.procesar_datos(deals)

    df = pd.DataFrame(datos)

    df_transformado = transformar_datos(df)

    cargar_datos(df_transformado)

    print("ETL completado")


if __name__ == "__main__":
    run_pipeline()