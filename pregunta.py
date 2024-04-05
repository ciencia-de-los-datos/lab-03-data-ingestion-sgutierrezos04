"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
from io import StringIO
import re

def ingest_data():

    with open("clusters_report.txt") as fp:

        columns1 = re.sub(r"\s{2,}", "\t", fp.readline().strip()).split("\t")
        columns2 = re.sub(r"\s{2,}", "\t", fp.readline()).split("\t")
        fp.readline()
        fp.readline()
    columns = []
    for i in range(len(columns1)):
        if len(columns2) > i:
            columns.append(str(columns1[i] + " " + columns2[i]).strip())
        else:
            columns.append(columns1[i])
    CLUSTER, CANTIDAD, PORCENTAJE, PRINCIPALES = [
        col.lower().replace(" ", "_") for col in columns]
    df = pd.read_fwf("clusters_report.txt",  skiprows=4, header=None)
    df.columns = [CLUSTER, CANTIDAD, PORCENTAJE, PRINCIPALES]

    df = df.ffill()

    df[PORCENTAJE] = df[PORCENTAJE].str.replace(
        ",", ".").str.replace(" %", "").astype(float)

    df = df.groupby([CLUSTER, CANTIDAD, PORCENTAJE])[
        PRINCIPALES].agg(lambda x: x).reset_index(name=PRINCIPALES)

    df[PRINCIPALES] = df[PRINCIPALES].str.join(" ").str.replace(
        r"\s{2,}", " ", regex=True).str.replace(".", "").str.strip()
    
    return df
