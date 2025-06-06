import operator
import re
import pandas as pd
from db_connect import get_connection, fetchall_dict

def cargar_operadores_bd():
    """
    Devuelve:
      - operador_to_python: {operador: operador_python}
      - python_to_desc: {operador_python: descripcion}
    """
    query = "SELECT operador, operador_python, descripcion FROM operadores"
    rows = fetchall_dict(query)
    operador_to_python = {row["operador"]: row["operador_python"] for row in rows if row["operador_python"]}
    python_to_desc = {row["operador_python"]: row["descripcion"] for row in rows if row["operador_python"]}
    return operador_to_python, python_to_desc

FUNCIONES_OPERADOR = {
    "gt": operator.gt,
    "ge": operator.ge,
    "lt": operator.lt,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
    "between": lambda x, a, b: a <= x <= b,
    "not_between": lambda x, a, b: not (a <= x <= b),
    "in_": lambda x, lista: x in lista,
    "not_in": lambda x, lista: x not in lista,
    "like": lambda x, pattern: bool(re.match(pattern.replace("%", ".*"), str(x))),
    "not_like": lambda x, pattern: not bool(re.match(pattern.replace("%", ".*"), str(x))),
    "order": lambda valores: all(valores[i] > valores[i+1] for i in range(len(valores)-1)),
    "order_most": lambda valores: sum([valores[i] > valores[i+1] for i in range(len(valores)-1)]) >= (len(valores)-1) // 2,
    "order_less": lambda valores: all(valores[i] < valores[i+1] for i in range(len(valores)-1)),
    "threshold": None,
    "custom": None,
}

OPERADOR_TO_PYTHON, PYTHON_TO_DESC = cargar_operadores_bd()

def aplicar_operador(valor, operador, limite_inferior=None, limite_superior=None, valores=None):
    """
    Aplica el operador (según mapping de BD) sobre el valor o lista de valores.
    - valor: valor único a evaluar (para thresholds, between, etc)
    - operador: string, tomado de la tabla de criterios/rangos
    - limite_inferior, limite_superior: umbrales usados según el tipo de comparación
    - valores: lista, usada para operadores tipo ORDER/ORDER_MOST
    """
    op_python = OPERADOR_TO_PYTHON.get(operador)
    if not op_python:
        return False
    func = FUNCIONES_OPERADOR.get(op_python)
    if not func:
        return False
    if op_python in ["order", "order_most", "order_less"]:
        if valores is not None:
            return func(valores)
        return False
    elif op_python in ["between", "not_between"]:
        return func(valor, limite_inferior, limite_superior)
    elif op_python in ["in_", "not_in"]:
        return func(valor, limite_superior if limite_superior is not None else limite_inferior)
    elif op_python in ["like", "not_like"]:
        return func(valor, limite_superior if limite_superior is not None else limite_inferior)
    elif op_python in ["gt", "ge", "lt", "le", "eq", "ne"]:
        return func(valor, limite_superior if limite_superior is not None else limite_inferior)
    return False

def mostrar_operadores_disponibles():
    print("Operadores cargados desde BD:")
    for op, py in OPERADOR_TO_PYTHON.items():
        print(f"{op:15} -> {py:15} | {PYTHON_TO_DESC.get(py, '')}")

def native(x):
    if hasattr(x, 'item'):
        return x.item()
    if isinstance(x, (pd.Timestamp, )):
        return str(x)
    return x