import logging
import pandas as pd
from datetime import datetime
from config import RANGO_FECHAS
from db_connect import fetch_dataframe, fetchall_dict, execute_many
from utils import native

logging.basicConfig(level=logging.INFO)

def obtener_tickers_activos():
    rows = fetchall_dict("SELECT ticker FROM tickers WHERE activo IS TRUE")
    return [row["ticker"] for row in rows]

def cargar_criterios():
    criterios = fetchall_dict(
        "SELECT id_criterio, nombre_criterio, tipo_criterio, parametros_relevantes, puntos_maximos_base, direccion, activo FROM catalogo_criterios WHERE activo = TRUE"
    )
    return criterios

def cargar_rangos_por_criterio(criterio_id):
    rangos = fetchall_dict(
        "SELECT * FROM criterio_rangos_ponderacion WHERE id_criterio_fk = %s", (criterio_id,)
    )
    return rangos

def cargar_indicadores(ticker, fecha_ini, fecha_fin):
    query = """
        SELECT *
        FROM indicadores
        WHERE ticker = %s AND "timestamp" BETWEEN %s AND %s
        ORDER BY "timestamp"
    """
    return fetch_dataframe(query, params=(ticker, fecha_ini, fecha_fin))

def extraer_ymd(timestamp):
    if isinstance(timestamp, pd.Timestamp):
        ts = timestamp
    else:
        ts = pd.to_datetime(timestamp)
    return ts.year, ts.month, ts.day

def formatear_resultado_criterio(nombre_rango, tipo_impacto, puntaje):
    return f"{nombre_rango} | {tipo_impacto} | puntos={puntaje:.2f}"

def evaluar_indicador_vs_constante(fila, criterio, rangos):
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
                int(yyyy), int(mm), int(dd)
            )
            return alerta
    return None

def evaluar_indicador_vs_indicador(fila, criterio, rangos):
    params = [x.strip() for x in criterio["parametros_relevantes"].split(";")]
    if len(params) != 2:
        return None  # Mal definida la parametrización
    campo1, campo2 = params
    valor1 = fila.get(campo1)
    valor2 = fila.get(campo2)
    if valor1 is None or valor2 is None or valor2 == 0:
        return None
    resultado = (valor1 / valor2) * 100  # Ratio como porcentaje

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
                int(yyyy), int(mm), int(dd)
            )
            return alerta
    return None

def evaluar_orden_indicadores(fila, criterio, rangos):
    campos = [x.strip() for x in criterio["parametros_relevantes"].split(";")]
    direccion = criterio.get("direccion", "desc").lower()  # "desc" para alcista/LONG, "asc" para bajista/SHORT
    valores = [fila.get(campo) for campo in campos]
    if any(v is None for v in valores):
        return None
    count_ok = 0
    for i in range(len(valores)-1):
        if direccion == "desc":
            if valores[i] > valores[i+1]:
                count_ok += 1
        else:  # "asc"
            if valores[i] < valores[i+1]:
                count_ok += 1
    # count_ok es el valor a comparar en los rangos
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
        int(yyyy), int(mm), int(dd)
    )
    return alerta

def evaluar_y_guardar_alertas(ticker):
    criterios = cargar_criterios()
    df = cargar_indicadores(ticker, RANGO_FECHAS["inicio"], RANGO_FECHAS["fin"])
    if df.empty:
        logging.warning(f"No hay datos para {ticker}")
        return ticker, 0
    df["ticker"] = ticker
    alertas = []

    for criterio in criterios:
        tipo = criterio["tipo_criterio"]
        rangos = cargar_rangos_por_criterio(criterio["id_criterio"])
        if not rangos:
            continue
        if tipo == "indicador_vs_constante":
            for idx, fila in df.iterrows():
                alerta = evaluar_indicador_vs_constante(fila, criterio, rangos)
                if alerta:
                    alertas.append(alerta)
        elif tipo == "indicador_vs_indicador":
            for idx, fila in df.iterrows():
                alerta = evaluar_indicador_vs_indicador(fila, criterio, rangos)
                if alerta:
                    alertas.append(alerta)
        elif tipo == "orden_indicadores":
            for idx, fila in df.iterrows():
                alerta = evaluar_orden_indicadores(fila, criterio, rangos)
                if alerta:
                    alertas.append(alerta)

    if alertas:
        execute_many("""
            INSERT INTO alertas_generadas
            (id_criterio_fk, ticker, timeframe, timestamp_alerta, valor_detalle_1, valor_detalle_2, valor_detalle_3, resultado_criterio, id_rango_fk, puntos_long, puntos_short, puntos_neutral, yyyy, mm, dd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, alertas)
        logging.info(f"{ticker}: {len(alertas)} alertas registradas.")
    else:
        logging.info(f"{ticker}: No se generaron alertas.")
    return ticker, len(alertas)

def main():
    logging.info(f"==== INICIO SCRIPT ALERTAS INDICADORES ====")
    tickers = obtener_tickers_activos()
    logging.info(f"Tickers activos: {tickers}")
    for ticker in tickers:
        evaluar_y_guardar_alertas(ticker)
    logging.info(f"==== FIN SCRIPT ALERTAS INDICADORES ====")

if __name__ == "__main__":
    main()