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
        "SELECT id_criterio, nombre_criterio, tipo_criterio, parametros_relevantes, puntos_maximos_base, direccion, activo, temporalidades_implicadas FROM catalogo_criterios WHERE activo = TRUE"
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

def evaluar_umbral_dinamico(fila, criterio, rangos):
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
                int(yyyy), int(mm), int(dd)
            )
            return alerta
    return None

def evaluar_rsi_multi_timeframe(fila, criterio, rangos, df_multi):
    campo = criterio["parametros_relevantes"].strip()
    tfs = [tf.strip() for tf in criterio["temporalidades_implicadas"].split(";") if tf.strip()]
    timestamp = fila["timestamp"]
    ticker = fila["ticker"]

    id_criterio = str(criterio.get("id_criterio", "")).lower()
    es_long = "long" in id_criterio
    es_short = "short" in id_criterio

    if not rangos:
        return None
    primer_rango = rangos[0]
    operador = (primer_rango.get("operador") or "<=").strip()
    if es_long:
        umbral = min(float(r.get("limite_superior", 35)) for r in rangos)
    elif es_short:
        umbral = max(float(r.get("limite_inferior", 70)) for r in rangos)
    else:
        umbral = float(primer_rango.get("limite_superior", 35))

    cumple = 0
    valores = []
    for tf in tfs:
        df_tf = df_multi.get(tf)
        valor = None
        if df_tf is not None and not df_tf.empty:
            sub = df_tf[df_tf["timestamp"] <= timestamp]
            if not sub.empty:
                fila_tf = sub.iloc[-1]
                valor = fila_tf.get(campo)
        valores.append((tf, valor))
        if valor is None:
            continue
        if operador == "<=" and valor <= umbral:
            cumple += 1
        elif operador == "<" and valor < umbral:
            cumple += 1
        elif operador == ">=" and valor >= umbral:
            cumple += 1
        elif operador == ">" and valor > umbral:
            cumple += 1

    rango_asignado = None
    for rango in rangos:
        rango_operador = (rango.get("operador") or "BETWEEN").upper()
        lim_inf = int(rango.get("limite_inferior", 0))
        lim_sup = int(rango.get("limite_superior", 0))
        incluye_inf = rango.get("incluye_limite_inferior", True)
        incluye_sup = rango.get("incluye_limite_superior", True)
        valor_eval = cumple
        match = False
        if rango_operador == "BETWEEN":
            if incluye_inf and incluye_sup:
                match = lim_inf <= valor_eval <= lim_sup
            elif incluye_inf and not incluye_sup:
                match = lim_inf <= valor_eval < lim_sup
            elif not incluye_inf and incluye_sup:
                match = lim_inf < valor_eval <= lim_sup
            else:
                match = lim_inf < valor_eval < lim_sup
        elif rango_operador == "==":
            match = valor_eval == lim_inf
        elif rango_operador == ">=":
            match = valor_eval >= lim_inf
        elif rango_operador == "<=":
            match = valor_eval <= lim_sup
        if match:
            rango_asignado = rango
            break
    if not rango_asignado:
        return None

    porcentaje = float(rango_asignado.get("porcentaje_puntos_base", 100))
    puntos_maximos = float(criterio.get("puntos_maximos_base") or 10.0)
    tipo_impacto = (rango_asignado.get("tipo_impacto") or "").upper()
    puntos_long = puntos_short = puntos_neutral = 0.0
    puntaje = puntos_maximos * porcentaje / 100
    if tipo_impacto == "LONG":
        puntos_long = puntaje
    elif tipo_impacto == "SHORT":
        puntos_short = puntaje
    elif tipo_impacto == "NEUTRAL":
        puntos_neutral = puntaje

    yyyy, mm, dd = extraer_ymd(fila["timestamp"])
    valor_detalle_1 = "rsi_14 - " + ", ".join(
        f"{tf}={v:.2f}" if v is not None else f"{tf}=None"
        for tf, v in valores
    )
    timeframe_mayor = tfs[0] if tfs else str(fila["timeframe"])
    alerta = (
        str(criterio["id_criterio"]),
        str(fila["ticker"]),
        timeframe_mayor,
        str(fila["timestamp"]),
        valor_detalle_1,
        '', '',
        formatear_resultado_criterio(rango_asignado["nombre_rango"], tipo_impacto, puntaje),
        rango_asignado["id_rango"],
        float(puntos_long), float(puntos_short), float(puntos_neutral),
        int(yyyy), int(mm), int(dd)
    )
    return alerta

def main():
    logging.info(f"==== INICIO SCRIPT ALERTAS INDICADORES ====")
    criterios = cargar_criterios()
    criterios_simples = [c for c in criterios if c.get("tipo_criterio") != "multi_timeframe"]
    criterios_multi = [c for c in criterios if c.get("tipo_criterio") == "multi_timeframe"]

    logging.info(f"Criterios simples encontrados: {len(criterios_simples)}")
    logging.info(f"Criterios multi_timeframe encontrados: {len(criterios_multi)}")

    tickers = obtener_tickers_activos()
    logging.info(f"Tickers activos: {tickers}")

    for ticker in tickers:
        df = cargar_indicadores(ticker, RANGO_FECHAS["inicio"], RANGO_FECHAS["fin"])
        if df.empty:
            logging.warning(f"No hay datos para {ticker}")
            continue
        df["ticker"] = ticker

        # --- Ciclo para criterios simples ---
        alertas = []
        for criterio in criterios_simples:
            tipo = criterio.get("tipo_criterio")
            rangos = cargar_rangos_por_criterio(criterio["id_criterio"])
            if not rangos:
                continue
            for idx, fila in df.iterrows():
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
        if alertas:
            execute_many("""
                INSERT INTO alertas_generadas
                (id_criterio_fk, ticker, timeframe, timestamp_alerta, valor_detalle_1, valor_detalle_2, valor_detalle_3, resultado_criterio, id_rango_fk, puntos_long, puntos_short, puntos_neutral, yyyy, mm, dd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, alertas)
            logging.info(f"{ticker}: {len(alertas)} alertas registradas (simples).")
        else:
            logging.info(f"{ticker}: No se generaron alertas simples.")

        # --- Ciclo especial para criterios multi_timeframe ---
        if criterios_multi:
            logging.info(f"{ticker}: Procesando criterios multi_timeframe...")
            tfs_usados = set()
            for criterio in criterios_multi:
                tfs = [tf.strip() for tf in str(criterio.get("temporalidades_implicadas", "")).split(";") if tf.strip()]
                tfs_usados.update(tfs)
            df_multi = {}
            for tf in tfs_usados:
                df_multi[tf] = df[df["timeframe"] == tf]

            for criterio in criterios_multi:
                logging.info(f"{ticker}: Procesando criterio multi_timeframe: {criterio['nombre_criterio']} ({criterio['id_criterio']})")
                rangos = cargar_rangos_por_criterio(criterio["id_criterio"])
                tfs = [tf.strip() for tf in str(criterio.get("temporalidades_implicadas", "")).split(";") if tf.strip()]
                if not tfs or not rangos:
                    continue
                tf_mayor = tfs[0]
                df_mayor = df_multi.get(tf_mayor)
                if df_mayor is None or df_mayor.empty:
                    logging.warning(f"{ticker}: No hay datos para TF mayor {tf_mayor} en criterio {criterio['nombre_criterio']}")
                    continue
                alertas_mt = []
                for idx, fila in df_mayor.iterrows():
                    alerta = evaluar_rsi_multi_timeframe(fila, criterio, rangos, df_multi)
                    if alerta:
                        alertas_mt.append(alerta)
                if alertas_mt:
                    execute_many("""
                        INSERT INTO alertas_generadas
                        (id_criterio_fk, ticker, timeframe, timestamp_alerta, valor_detalle_1, valor_detalle_2, valor_detalle_3, resultado_criterio, id_rango_fk, puntos_long, puntos_short, puntos_neutral, yyyy, mm, dd)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, alertas_mt)
                    logging.info(f"{ticker}: {len(alertas_mt)} alertas multi_timeframe registradas para criterio {criterio['nombre_criterio']}.")
                else:
                    logging.info(f"{ticker}: No se generaron alertas multi_timeframe para criterio {criterio['nombre_criterio']}.")
        else:
            logging.info("No existen criterios multi_timeframe definidos en el sistema.")

    logging.info(f"==== FIN SCRIPT ALERTAS INDICADORES ====")

if __name__ == "__main__":
    main()