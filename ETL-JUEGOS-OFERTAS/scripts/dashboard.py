#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar datos
df = pd.read_csv("data/deals.csv")

# Tomar top 10 por ahorro
df_top = df.sort_values("ahorro_porcentaje", ascending=False).head(10)

fig, axes = plt.subplots(2, 2, figsize=(15, 10))
fig.suptitle("Dashboard de Ofertas - CheapShark", fontsize=16, fontweight='bold')

# 1️⃣ Top descuentos
axes[0, 0].barh(df_top["titulo"], df_top["ahorro_porcentaje"])
axes[0, 0].set_title("Top 10 Mayores Descuentos (%)")
axes[0, 0].invert_yaxis()

# 2️⃣ Precio normal vs oferta
x = np.arange(len(df_top))
width = 0.35
axes[0, 1].bar(x - width/2, df_top["precio_normal"], width, label="Normal")
axes[0, 1].bar(x + width/2, df_top["precio_oferta"], width, label="Oferta")
axes[0, 1].set_title("Precio Normal vs Oferta")
axes[0, 1].set_xticks(x)
axes[0, 1].set_xticklabels(df_top["titulo"], rotation=90)
axes[0, 1].legend()

# 3️⃣ Distribución de precios
axes[1, 0].hist(df["precio_oferta"], bins=10)
axes[1, 0].set_title("Distribución de Precios en Oferta")

# 4️⃣ Rating vs Descuento
axes[1, 1].scatter(df["ahorro_porcentaje"], df["rating_steam"])
axes[1, 1].set_title("Rating Steam vs Ahorro")
axes[1, 1].set_xlabel("Ahorro (%)")
axes[1, 1].set_ylabel("Rating Steam")

plt.tight_layout()
plt.savefig("data/dashboard.png", dpi=300, bbox_inches="tight")

logger.info("Dashboard guardado en data/dashboard.png")
plt.show()