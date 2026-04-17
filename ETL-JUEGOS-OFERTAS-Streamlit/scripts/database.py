import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

def _sanitize_database_url(url: str) -> str:
    if not url:
        return ''
    if '[YOUR-PASSWORD]' in url:
        return ''
    if '[' in url and ']' in url:
        try:
            scheme, rest = url.split('://', 1)
            userpass, host = rest.split('@', 1)
            if ':' in userpass:
                user, password = userpass.split(':', 1)
                password = password.strip('[]')
                return f'{scheme}://{user}:{password}@{host}'
        except ValueError:
            return url
    return url

load_dotenv()

DB_USER = os.getenv('DB_USER')
DATABASE_URL = _sanitize_database_url(os.getenv('DATABASE_URL', ''))
# Si ya tenemos un DB_USER específico de Supabase y el URL directo usa el usuario postgres,
# preferimos reconstruir la URL completa desde las variables de entorno.
if DB_USER and DB_USER != 'postgres' and DATABASE_URL and f'{DB_USER}:' not in DATABASE_URL:
    DATABASE_URL = ''

if not DATABASE_URL:
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_SSLMODE = os.getenv('DB_SSLMODE', 'require')

    if DB_HOST and DB_PORT and DB_USER and DB_PASSWORD and DB_NAME:
        DATABASE_URL = (
            f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
            f'?sslmode={DB_SSLMODE}'
        )
    else:
        DATABASE_URL = 'postgresql://postgres:1221@localhost:5432/juegos_ofertas_etl'

engine = create_engine(DATABASE_URL, echo=False, future=True)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()