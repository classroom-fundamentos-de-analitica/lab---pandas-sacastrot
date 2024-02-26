"""
Laboratorio - Manipulación de Datos usando Pandas
-----------------------------------------------------------------------------------------

Este archivo contiene las preguntas que se van a realizar en el laboratorio.

Utilice los archivos `tbl0.tsv`, `tbl1.tsv` y `tbl2.tsv`, para resolver las preguntas.

"""
import pandas as pd

tbl0 = pd.read_csv("tbl0.tsv", sep="\t")
tbl1 = pd.read_csv("tbl1.tsv", sep="\t")
tbl2 = pd.read_csv("tbl2.tsv", sep="\t")


def pregunta_01():
  """
  ¿Cuál es la cantidad de filas en la tabla `tbl0.tsv`?

  Rta/
  40

  """
  respuesta = len(tbl0)
  return respuesta



def pregunta_02():
  """
  ¿Cuál es la cantidad de columnas en la tabla `tbl0.tsv`?

  Rta/
  4

  """
  respuesta = len(tbl0.columns)
  return respuesta



def pregunta_03():
    """
    ¿Cuál es la cantidad de registros por cada letra de la columna _c1 del archivo
    `tbl0.tsv`?

    Rta/
    A     8
    B     7
    C     5
    D     6
    E    14
    Name: _c1, dtype: int64

    """
    respuesta = tbl0.groupby('_c1')['_c1'].count()
    return respuesta



def pregunta_04():
    """
    Calcule el promedio de _c2 por cada letra de la _c1 del archivo `tbl0.tsv`.

    Rta/
    A    4.625000
    B    5.142857
    C    5.400000
    D    3.833333
    E    4.785714
    Name: _c2, dtype: float64
    """
    respuesta = tbl0.groupby('_c1')['_c2'].mean()
    return respuesta



def pregunta_05():
    """
    Calcule el valor máximo de _c2 por cada letra en la columna _c1 del archivo
    `tbl0.tsv`.

    Rta/
    _c1
    A    9
    B    9
    C    9
    D    7
    E    9
    Name: _c2, dtype: int64
    """
    respuesta = tbl0.groupby('_c1')['_c2'].max()
    return respuesta


def pregunta_06():
    """
    Retorne una lista con los valores unicos de la columna _c4 de del archivo `tbl1.csv`
    en mayusculas y ordenados alfabéticamente.

    Rta/
    ['A', 'B', 'C', 'D', 'E', 'F', 'G']

    """
    respuesta = [x.upper() for x in sorted(tbl1['_c4'].unique().tolist())]
    return respuesta


def pregunta_07():
    """
    Calcule la suma de la _c2 por cada letra de la _c1 del archivo `tbl0.tsv`.

    Rta/
    _c1
    A    37
    B    36
    C    27
    D    23
    E    67
    Name: _c2, dtype: int64
    """
    respuesta = tbl0.groupby('_c1').sum()['_c2']
    return respuesta


def pregunta_08():
    """
    Agregue una columna llamada `suma` con la suma de _c0 y _c2 al archivo `tbl0.tsv`.

    Rta/
        _c0 _c1  _c2         _c3  suma
    0     0   E    1  1999-02-28     1
    1     1   A    2  1999-10-28     3
    2     2   B    5  1998-05-02     7
    ...
    37   37   C    9  1997-07-22    46
    38   38   E    1  1999-09-28    39
    39   39   E    5  1998-01-26    44

    """
    respuesta = tbl0.assign(suma=tbl0['_c0'] + tbl0['_c2'])
    return respuesta


def pregunta_09():
    """
    Agregue el año como una columna al archivo `tbl0.tsv`.

    Rta/
        _c0 _c1  _c2         _c3  year
    0     0   E    1  1999-02-28  1999
    1     1   A    2  1999-10-28  1999
    2     2   B    5  1998-05-02  1998
    ...
    37   37   C    9  1997-07-22  1997
    38   38   E    1  1999-09-28  1999
    39   39   E    5  1998-01-26  1998

    """
    respuesta = tbl0.assign(year=tbl0['_c3'].str.split('-').str[0])
    return respuesta


def pregunta_10():
    valores = tbl0[['_c1', '_c2']].groupby(['_c1'])['_c2'].apply(list).tolist()
    c2 = []

    for letra in valores:
        texto = ''
        for valor in sorted(letra):
            texto += f'{valor}:'
        
        c2.append(texto[:-1])

    return pd.DataFrame({
        '_c2': c2
    }, index = pd.Series(['A', 'B', 'C', 'D', 'E'], name='_c1'))


def pregunta_11():
    valores = tbl1.groupby(['_c0'])['_c4'].apply(list).tolist()
    c0 = tbl1['_c0'].unique().tolist()
    c4 = []

    for numero in valores:
        texto = ''
        for valor in sorted(numero):
            texto += f'{valor},'
        
        c4.append(texto[:-1])

    return pd.DataFrame({
        '_c0': c0,
        '_c4': c4
    })


def pregunta_12():
    c5a = tbl2.groupby(['_c0'])['_c5a'].apply(list).tolist()
    c5b = tbl2.groupby(['_c0'])['_c5b'].apply(list).tolist()
    c0 = tbl1['_c0'].unique().tolist()
    
    c5 = []

    for i in range(len(c5a)):
        x = []

        for j in range(len(c5a[i])):
            x.append(f'{c5a[i][j]}:{c5b[i][j]}')
        
        texto = ''

        for valor in sorted(x):
            texto += f'{valor},'

        c5.append(texto[:-1])

    return pd.DataFrame({
        '_c0': c0,
        '_c5': c5
    })


def pregunta_13():
    """
    Si la columna _c0 es la clave en los archivos `tbl0.tsv` y `tbl2.tsv`, compute la
    suma de tbl2._c5b por cada valor en tbl0._c1.

    Rta/
    _c1
    A    146
    B    134
    C     81
    D    112
    E    275
    Name: _c5b, dtype: int64
    """
    respuesta = tbl0.merge(tbl2, on='_c0').groupby('_c1').sum()['_c5b']
    return respuesta
