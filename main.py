"""
VERSIÓN: 1.0.0
TÍTULO: Generador Multiproceso de Alertas de Indicadores Financieros con is_closed
DESCRIPCIÓN:
    - Este script genera alertas históricas basadas en indicadores técnicos, procesando snapshots cada 5 minutos
      para múltiples tickers y temporalidades.
    - Usa multiprocessing para paralelizar el cálculo por ticker, optimizando el uso de recursos en servidores multinúcleo.
    - Divide el procesamiento en paquetes por ticker y día, minimizando uso de memoria y permitiendo commits frecuentes.
    - El campo is_closed indica si el snapshot corresponde al cierre real de la vela.
    - El campo is_closed es propagado a la tabla de alertas_generadas.
"""

import logging
import pandas as pd
from datetime import datetime
from config import RANGO_FECHAS
from db_connect import fetch_dataframe, fetchall_dict, execute_many
from utils import native
from concurrent.futures import ProcessPoolExecutor, as_completed

# ==== CONFIGURACIÓN DE LOGGING ====
logging.basicConfig(level=logging.INFO)

# ==== FUNCIONES DE CARGA DESDE BASE DE DATOS ====
def obtener_tickers_activos():
    """Obtiene la lista de tickers activos desde la base de datos."""
    rows = fetchall_dict("SELECT ticker FROM tickers WHERE activo IS TRUE")
    return [row["ticker"] for row in rows]

def cargar_criterios():
    """Carga los criterios activos desde la base de datos."""
    criterios = fetchall_dict(
        "SELECT id_criterio, nombre_criterio, tipo_criterio, parametros_relevantes, puntos_maximos_base, direccion, activo, temporalidades_implicadas FROM catalogo_criterios WHERE activo = TRUE"
    )
    return criterios

def cargar_rangos_por_criterio(criterio_id):
    """Carga los rangos de ponderación asociados a un criterio específico."""
    rangos = fetchall_dict(
        "SELECT * FROM criterio_rangos_ponderacion WHERE id_criterio_fk = %s", (criterio_id,)
    )
    return rangos

def cargar_indicadores(ticker, fecha_ini, fecha_fin):
    """
    Carga todos los snapshots de indicadores para un ticker y rango de fechas.
    Incluye tanto abiertos como cerrados (is_closed).
    """
    query = """
        SELECT *
        FROM indicadores
        WHERE ticker = %s AND "timestamp" BETWEEN %s AND %s
        ORDER BY "timestamp"
    """
    return fetch_dataframe(query, params=(ticker, fecha_ini, fecha_fin))

# ==== FUNCIONES AUXILIARES ====
def extraer_ymd(timestamp):
    """Extrae año, mes y día de un timestamp."""
    if isinstance(timestamp, pd.Timestamp):
        ts = timestamp
    else:
        ts = pd.to_datetime(timestamp)
    return ts.year, ts.month, ts.day

def formatear_resultado_criterio(nombre_rango, tipo_impacto, puntaje):
    """Devuelve cadena legible resumen del resultado del criterio."""
    return f"{nombre_rango} | {tipo_impacto} | puntos={puntaje:.2f}"

# ==== FUNCIONES DE EVALUACIÓN DE CRITERIOS ====
def evaluar_indicador_vs_constante(fila, criterio, rangos):
    """
    Evalúa si el valor de un indicador cae dentro de los rangos definidos,
    generando una alerta si corresponde.
    """
    campo = criterio["parametros_relevantes"].strip()
    valor = fila.get(campo)
    if valor is None:
        return None
    resultado = valor
    for rango in rangos:
        lim_inf = float(rango.get("limite_inferior"))
        lim_sup = float(rango.get("limite_superior"))
        operador = (rango["operador"] or "").upper()
        incluye_inf = rango.get("incluye_limite_inferior", True)
        incluye_sup = rango.get("incluye_limite_superior", True)
        match = False
        # Lógica para operador BETWEEN
        if operador == "BETWEEN":
            if incluye_inf and incluye_sup:
                match = lim_inf <= resultado <= lim_sup
            elif incluye_inf and not incluye_sup:
                match = lim_inf <= resultado < lim_sup
            elif not incluye_inf and incluye_sup:
                match = lim_inf < resultado <= lim_sup
            else:
                match = lim_inf < resultado < lim_sup

        if match:
            porcentaje = float(rango["porcentaje_puntos_base"])
            puntos_maximos = float(criterio["puntos_maximos_base"]) if criterio.get("puntos_maximos_base") else 10.0
            tipo_impacto = (rango.get("tipo_impacto") or "").upper()
            puntos_long = puntos_short = puntos_neutral = 0.0
            puntaje = puntos_maximos * porcentaje / 100
            if tipo_impacto == "LONG":
                puntos_long = puntaje
            elif tipo_impacto == "SHORT":
                puntos_short = puntaje
            elif tipo_impacto == "NEUTRAL":
                puntos_neutral = 0.0
                puntaje = 0.0
            yyyy, mm, dd = extraer_ymd(fila["timestamp"])
            is_closed = fila.get("is_closed", None)
            # Retorna tupla lista para execute_many, propagando is_closed
            alerta = (
                str(criterio["id_criterio"]),
                str(fila["ticker"]),
                native(fila["timeframe"]),
                str(fila["timestamp"]),
                f"{campo}:{valor:.4f}",
                '', '',
                formatear_resultado_criterio(rango["nombre_rango"], tipo_impacto, puntaje),
                rango["id_rango"],
                float(puntos_long), float(puntos_short), float(puntos_neutral),
                int(yyyy), int(mm), int(dd),
                is_closed
            )
            return alerta
    return None

def evaluar_indicador_vs_indicador(fila, criterio, rangos):
    """
    Evalúa la relación entre dos indicadores, comparando el ratio con los rangos definidos.
    """
    params = [x.strip() for x in criterio["parametros_relevantes"].split(";")]
    if len(params) != 2:
        return None
    campo1, campo2 = params
    valor1 = fila.get(campo1)
    valor2 = fila.get(campo2)
    if valor1 is None or valor2 is None or valor2 == 0:
        return None
    resultado = (valor1 / valor2) * 100
    for rango in rangos:
        lim_inf = float(rango.get("limite_inferior"))
        lim_sup = float(rango.get("limite_superior"))
        operador = (rango["operador"] or "").upper()
        incluye_inf = rango.get("incluye_limite_inferior", True)
        incluye_sup = rango.get("incluye_limite_superior", True)
        match = False
        if operador == "BETWEEN":
            if incluye_inf and incluye_sup:
                match = lim_inf <= resultado <= lim_sup
            elif incluye_inf and not incluye_sup:
                match = lim_inf <= resultado < lim_sup
            elif not incluye_inf and incluye_sup:
                match = lim_inf < resultado <= lim_sup
            else:
                match = lim_inf < resultado < lim_sup

        if match:
            porcentaje = float(rango["porcentaje_puntos_base"])
            puntos_maximos = float(criterio["puntos_maximos_base"]) if criterio.get("puntos_maximos_base") else 10.0
            tipo_impacto = (rango.get("tipo_impacto") or "").upper()
            puntos_long = puntos_short = puntos_neutral = 0.0
            puntaje = puntos_maximos * porcentaje / 100
            if tipo_impacto == "LONG":
                puntos_long = puntaje
            elif tipo_impacto == "SHORT":
                puntos_short = puntaje
            elif tipo_impacto == "NEUTRAL":
                puntos_neutral = 0.0
                puntaje = 0.0
            yyyy, mm, dd = extraer_ymd(fila["timestamp"])
            is_closed = fila.get("is_closed", None)
            alerta = (
                str(criterio["id_criterio"]),
                str(fila["ticker"]),
                native(fila["timeframe"]),
                str(fila["timestamp"]),
                f"{campo1}:{valor1:.4f}/{campo2}:{valor2:.4f}",
                '', '',
                formatear_resultado_criterio(rango["nombre_rango"], tipo_impacto, puntaje),
                rango["id_rango"],
                float(puntos_long), float(puntos_short), float(puntos_neutral),
                int(yyyy), int(mm), int(dd),
                is_closed
            )
            return alerta
    return None

def evaluar_orden_indicadores(fila, criterio, rangos):
    """
    Evalúa si varios indicadores cumplen un orden definido (ej: EMA10 > EMA20 > EMA50),
    útil para tendencias.
    """
    campos = [x.strip() for x in criterio["parametros_relevantes"].split(";")]
    direccion = criterio.get("direccion", "desc").lower()
    valores = [fila.get(campo) for campo in campos]
    if any(v is None for v in valores):
        return None
    count_ok = 0
    for i in range(len(valores)-1):
        if direccion == "desc":
            if valores[i] > valores[i+1]:
                count_ok += 1
        else:
            if valores[i] < valores[i+1]:
                count_ok += 1
    rango_asignado = None
    for rango in rangos:
        operador = (rango["operador"] or "").upper()
        lim_inf = int(rango.get("limite_inferior", 0))
        lim_sup = int(rango.get("limite_superior", 0))
        incluye_inf = rango.get("incluye_limite_inferior", True)
        incluye_sup = rango.get("incluye_limite_superior", True)
        match = False
        valor = count_ok
        if operador == "BETWEEN":
            if incluye_inf and incluye_sup:
                match = lim_inf <= valor <= lim_sup
            elif incluye_inf and not incluye_sup:
                match = lim_inf <= valor < lim_sup
            elif not incluye_inf and incluye_sup:
                match = lim_inf < valor <= lim_sup
            else:
                match = lim_inf < valor < lim_sup
        if match:
            rango_asignado = rango
            break
    if not rango_asignado:
        return None
    porcentaje = float(rango_asignado["porcentaje_puntos_base"])
    puntos_maximos = float(criterio["puntos_maximos_base"]) if criterio.get("puntos_maximos_base") else 10.0
    tipo_impacto = (rango_asignado.get("tipo_impacto") or "").upper()
    puntos_long = puntos_short = puntos_neutral = 0.0
    puntaje = puntos_maximos * porcentaje / 100
    if tipo_impacto == "LONG":
        puntos_long = puntaje
    elif tipo_impacto == "SHORT":
        puntos_short = puntaje
    elif tipo_impacto == "NEUTRAL":
        puntos_neutral = 0.0
        puntaje = 0.0

    yyyy, mm, dd = extraer_ymd(fila["timestamp"])
    valor_detalle_1 = ";".join(f"{campo}:{fila.get(campo):.4f}" for campo in campos)
    is_closed = fila.get("is_closed", None)
    alerta = (
        str(criterio["id_criterio"]),
        str(fila["ticker"]),
        native(fila["timeframe"]),
        str(fila["timestamp"]),
        valor_detalle_1,
        '', '',
        formatear_resultado_criterio(rango_asignado["nombre_rango"], tipo_impacto, puntaje),
        rango_asignado["id_rango"],
        float(puntos_long), float(puntos_short), float(puntos_neutral),
        int(yyyy), int(mm), int(dd),
        is_closed
    )
    return alerta

def evaluar_umbral_dinamico(fila, criterio, rangos):
    """
    Evalúa umbrales calculados dinámicamente (por fórmula) y los compara,
    útil para condiciones adaptativas.
    """
    try:
        params = [x.strip() for x in criterio["parametros_relevantes"].split(";")]
        if len(params) != 2:
            return None
        indicador_objetivo = params[0]
        formula_umbral = params[1]
        valor_objetivo = fila.get(indicador_objetivo)
        if valor_objetivo is None:
            return None
        contexto = {k: fila.get(k) for k in fila.keys()}
        umbral = eval(formula_umbral, {}, contexto)
    except Exception as e:
        logging.warning(f"Error evaluando umbral dinámico: {e}")
        return None

    for rango in rangos:
        operador = (rango["operador"] or "").strip()
        match = False
        if operador == ">":
            match = valor_objetivo > umbral
        elif operador == "<":
            match = valor_objetivo < umbral
        elif operador == ">=":
            match = valor_objetivo >= umbral
        elif operador == "<=":
            match = valor_objetivo <= umbral
        elif operador == "==":
            match = valor_objetivo == umbral
        else:
            continue

        if match:
            porcentaje = float(rango["porcentaje_puntos_base"])
            puntos_maximos = float(criterio.get("puntos_maximos_base") or 10.0)
            tipo_impacto = (rango.get("tipo_impacto") or "").upper()
            puntos_long = puntos_short = puntos_neutral = 0.0
            puntaje = puntos_maximos * porcentaje / 100
            if tipo_impacto == "LONG":
                puntos_long = puntaje
            elif tipo_impacto == "SHORT":
                puntos_short = puntaje
            elif tipo_impacto == "NEUTRAL":
                puntos_neutral = 0.0
                puntaje = 0.0
            yyyy, mm, dd = extraer_ymd(fila["timestamp"])
            is_closed = fila.get("is_closed", None)
            valor_detalle_1 = f"{indicador_objetivo}:{valor_objetivo:.4f}; umbral:{umbral:.4f}"
            alerta = (
                str(criterio["id_criterio"]),
                str(fila["ticker"]),
                native(fila["timeframe"]),
                str(fila["timestamp"]),
                valor_detalle_1,
                '', '',
                formatear_resultado_criterio(rango["nombre_rango"], tipo_impacto, puntaje),
                rango["id_rango"],
                float(puntos_long), float(puntos_short), float(puntos_neutral),
                int(yyyy), int(mm), int(dd),
                is_closed
            )
            return alerta
    return None

# ==== FUNCIÓN PRINCIPAL DE PROCESAMIENTO POR TICKER ====
def procesar_ticker(ticker, criterios_simples, fecha_inicio, fecha_fin):
    """
    Procesa todos los snapshots de un ticker en el rango dado.
    Divide en paquetes por día (para bajo uso de memoria y commits frecuentes).
    Devuelve ticker y total de alertas generadas.
    """
    from db_connect import fetch_dataframe, fetchall_dict, execute_many  # Import dentro del proceso
    logging.info(f">>> INICIO procesamiento ticker: {ticker} <<<")
    df = cargar_indicadores(ticker, fecha_inicio, fecha_fin)
    if df.empty:
        logging.warning(f"No hay datos para {ticker}")
        return ticker, 0
    df["ticker"] = ticker

    # Agrupa el DataFrame por día calendario
    df['fecha'] = pd.to_datetime(df['timestamp']).dt.date
    fechas = df['fecha'].unique()
    fechas = sorted(fechas)
    total_alertas = 0

    # CICLO PRINCIPAL: por día
    for fecha in fechas:
        df_dia = df[df['fecha'] == fecha]
        if df_dia.empty:
            continue
        alertas = []
        logging.info(f"--- INICIO paquete: ticker={ticker}, fecha={fecha}, registros={len(df_dia)} ---")
        # CICLO por criterio
        for criterio in criterios_simples:
            tipo = criterio.get("tipo_criterio")
            rangos = cargar_rangos_por_criterio(criterio["id_criterio"])
            if not rangos:
                continue
            # CICLO por snapshot (registro de indicadores)
            for idx, fila in df_dia.iterrows():
                if tipo == "indicador_vs_constante":
                    alerta = evaluar_indicador_vs_constante(fila, criterio, rangos)
                elif tipo == "indicador_vs_indicador":
                    alerta = evaluar_indicador_vs_indicador(fila, criterio, rangos)
                elif tipo == "orden_indicadores":
                    alerta = evaluar_orden_indicadores(fila, criterio, rangos)
                elif tipo == "umbral_dinamico":
                    alerta = evaluar_umbral_dinamico(fila, criterio, rangos)
                else:
                    alerta = None
                if alerta:
                    alertas.append(alerta)
        # Commit de alertas del paquete diario
        if alertas:
            execute_many("""
                INSERT INTO alertas_generadas
                (id_criterio_fk, ticker, timeframe, timestamp_alerta, valor_detalle_1, valor_detalle_2, valor_detalle_3, resultado_criterio, id_rango_fk, puntos_long, puntos_short, puntos_neutral, yyyy, mm, dd, is_closed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, alertas)
        logging.info(f"--- FIN paquete: ticker={ticker}, fecha={fecha}, alertas generadas={len(alertas)} ---")
        total_alertas += len(alertas)
    logging.info(f">>> FIN procesamiento ticker: {ticker} | Total alertas generadas: {total_alertas} <<<")
    return ticker, total_alertas

# ==== FUNCIÓN PRINCIPAL (MULTIPROCESO) ====
def main():
    """
    Orquesta la ejecución paralela por tickers usando ProcessPoolExecutor.
    """
    logging.info(f"==== INICIO SCRIPT ALERTAS INDICADORES (Multiprocessing) ====")
    criterios = cargar_criterios()
    criterios_simples = [c for c in criterios if c.get("tipo_criterio") != "multi_timeframe"]
    logging.info(f"Criterios simples encontrados: {len(criterios_simples)}")
    tickers = obtener_tickers_activos()
    logging.info(f"Tickers activos: {tickers}")

    fecha_inicio = RANGO_FECHAS["inicio"]
    fecha_fin = RANGO_FECHAS["fin"]

    max_procesos = 3  # Ajusta según la capacidad de tu máquina

    # Procesamiento paralelo por tickers
    with ProcessPoolExecutor(max_workers=max_procesos) as executor:
        futures = []
        for ticker in tickers:
            futures.append(executor.submit(procesar_ticker, ticker, criterios_simples, fecha_inicio, fecha_fin))
        for future in as_completed(futures):
            ticker, total_alertas = future.result()
            logging.info(f"Resumen Ticker {ticker}: alertas totales generadas = {total_alertas}")

    logging.info(f"==== FIN SCRIPT ALERTAS INDICADORES ====")

# ==== EJECUCIÓN PRINCIPAL ====
if __name__ == "__main__":
    main()

"""
VERSIÓN: 1.0.0
FIN DEL SCRIPT Generador Multiproceso de Alertas de Indicadores Financieros con is_closed
"""
