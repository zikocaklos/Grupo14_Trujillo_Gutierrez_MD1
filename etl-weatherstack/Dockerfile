FROM python:3.11-slim

WORKDIR /app

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY src/ ./src/
COPY sql/ ./sql/

# Variable para conexión a BD
ENV DATABASE_URL=postgresql://dataminer:secure_pass@postgres:5432/datawarehouse
ENV PYTHONUNBUFFERED=1

# Crear directorio de logs
RUN mkdir -p /app/logs

# Ejecutar ingestor
CMD ["python", "src/main.py"]
