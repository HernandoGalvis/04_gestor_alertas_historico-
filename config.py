import os
import pytz
# Zona horaria y configuración de logging
TIME_ZONE_BOT = 'America/Bogota'
UTC = pytz.timezone('UTC')
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# API Bitget (no compartas tus claves reales en público)
API_KEY = "bg_960545b06d4926544f82fc1be1dd40ad"
API_SECRET = "d55c87152c6a04e4c27be96b736e7c7ef06a3f86eb79ee0251b314575053e9be"
API_PASSPHRASE = "guicanmevionacer"
BITGET_API_URL = "https://api.bitget.com"

# Configuración de la base de datos
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "pg_autobot"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}

# Parámetros de ejecución
RANGO_FECHAS = {
    "inicio": "2024-01-01 00:00:00",    # Modifica aquí para tus pruebas
    "fin":    "2025-06-01 00:00:00"
}

N_PROCESOS = 3        # Número de procesos simultáneos para multiproceso
CHUNK_SIZE = 5000     # No implementado en lectura por lotes pero disponible para futuros ajustes
