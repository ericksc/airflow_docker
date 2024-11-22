import shutil
import os
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pathlib import Path
from uuid import uuid4


def review(item):
    return pd.Series({
        'Suma': item.sum(),
        'p_value': item[item.notna()].size / item.size,
        'missing': item[item.isna()].index.to_list()
    })


def cheapest(item):
    return item[item['Suma'] == item['Suma'].min()].iloc[-1]

def dia_to_ndia(dia):
    return (dia - datetime(1970, 1, 1)).days

def best_price_by_list(data, lista):
    if data is None:
        print('No data available')
        return

    historico  = (
        data.groupby(['Fecha', 'Lugar', 'Producto'])['Precio'].min()
            .unstack(level=0)
            .swaplevel()
            .sort_index()
            .sort_index(axis=1)
    )

    selection = (
        historico.iloc[:, [-1]]
            .unstack()
            .loc[lista]
    )

    # quitar un nivel. quitar el nivel de las fechas
    selection = selection.droplevel(level=0, axis=1)

    res = (
        selection.apply(review, axis=0)
        .T.reset_index()
        .groupby(['p_value']).apply(cheapest)
    )
    return res


def procesar_facturas(ti):
    archivos_movidos = ti.xcom_pull(task_ids='mover_facturas', key='archivos_movidos')
    print(archivos_movidos)

    df = pd.concat([pd.read_csv(filename) for filename in archivos_movidos])

    print(df.head())

    lista = ["arroz", "frijoles"]

    mejor_precio = best_price_by_list(data=df, lista=lista)
    print('-'*10)
    print(mejor_precio)
    base_folder = "./data/"
    filename = f'mejor_precio_{str(uuid4())[:6]}.csv'
    ruta_destino = f'{base_folder}facturas_procesadas/{filename}'

    mejor_precio.to_csv(ruta_destino)


def move_n_files(ti):
    print("Iniciando move_n_files")
    base_folder = "./data/"
    ruta_original = f'{base_folder}facturas_originales/'
    ruta_destino = f'{base_folder}facturas_por_procesar/'
    n = 100
    lista_archivos = [filename for filename in Path(ruta_original).rglob("*.csv")]
    lista_archivos.sort()
    n_archivos = lista_archivos[:n]

    archivos_movidos = []

    for archivo in n_archivos:
        file = Path(archivo)
        converted_file = f"{ruta_destino}/{file.name}"
        shutil.move(file, converted_file)
        print(f"archivo movido desde {file} hacia {converted_file}")
        archivos_movidos.append(converted_file)

    ti.xcom_push(key='archivos_movidos', value=archivos_movidos)

default_args = {
    'owner': 'erick',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='proceso_busqueda_mejor_precio_dag',
        default_args=default_args,
        description='This is our first dag that we write',
        start_date=datetime(2024, 10, 16, 0),
        schedule_interval="* * * * *",
        catchup=False
) as dag:
    revisar_pwd = BashOperator(
        task_id='pwd',
        bash_command="pwd"
    )
    revisar_pycwd = PythonOperator(
        task_id='py_cwd',
        python_callable=lambda: print(os.getcwd())
    )
    mover_facturas = PythonOperator(
        task_id="mover_facturas",
        python_callable=move_n_files)

    procesar_facturas = PythonOperator(
        task_id="procesar_facturas",
        python_callable=procesar_facturas)

    revisar_pwd >> revisar_pycwd >> mover_facturas >> procesar_facturas
