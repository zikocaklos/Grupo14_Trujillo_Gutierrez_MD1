import os
import sys

print("🚀 Iniciando proceso ETL CheapShark...\n")

resultado = os.system("python scripts/extractor.py")

if resultado == 0:
    os.system("python scripts/dashboard.py")
else:
    print("❌ El extractor falló. No se ejecutará el dashboard.")
    sys.exit(1)

print("\n✅ Proceso finalizado correctamente")

#python scripts/main.py