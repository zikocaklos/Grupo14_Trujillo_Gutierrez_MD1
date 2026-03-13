from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from scripts.database import Base

class Deal(Base):

    __tablename__ = "deals"

    id = Column(Integer, primary_key=True, index=True)

    titulo = Column(String, nullable=False)

    precio_oferta = Column(Float)
    precio_normal = Column(Float)

    ahorro_porcentaje = Column(Float)

    store_id = Column(Integer)

    rating_steam = Column(Float)

    metacritic = Column(Integer)

    fecha_extraccion = Column(DateTime, default=datetime.utcnow)