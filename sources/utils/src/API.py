"""
API para RDU do banco de dados.
O caminho do banco de dados é dado por DB_PATH e pode ser alterado pelo método db_path.
"""

from __future__ import annotations

import pandas as pd
import sqlite3


DB_PATH = './sources/data/resources/DB/output.db'
SEVERITIES = ['Low', 'Medium', 'High', 'Informational', 'Undetermined']


def get_auditors(*auditors: int | str) -> dict | list[dict]:
    """
    Método para leitura de auditores pelo ID.

    :param auditors: ID ou Nome do auditor.
    :return: Dicionário representando o auditor.
    """
    with sqlite3.connect(DB_PATH) as db:

        if len(auditors) > 0:
            auditor_list = []
            for auditor in auditors:
                cursor = db.cursor()
                script = f"SELECT * FROM auditors WHERE {'id' if not type(auditor) == str else 'name'} = ?"
                cursor.execute(script, [auditor])
                auditor = cursor.fetchone()
                auditor_list.append({'id': auditor[0], 'name': auditor[1]})
            return auditor_list
        elif len(auditors) == 0:
            auditor_list = []
            cursor = db.cursor()
            script = "SELECT * FROM auditors"
            cursor.execute(script)
            buffer = cursor.fetchall()
            for auditor in buffer:
                auditor_list.append({'id': auditor[0], 'name': auditor[1]})
            return auditor_list
        else:
            return []


def get_findings_by_auditors(*auditors: str | int) -> pd.DataFrame:
    """
    Método para leitura de findings pelo nome ou ID do auditor.

    :param auditors: Nome ou ID do auditor.
    :return: Pandas DataFrame.
    """

    # Pega a lista com nome e id dos auditores
    auditor_list = get_auditors(*auditors)
    with sqlite3.connect(DB_PATH) as db:

        if len(auditor_list) > 0:
            ids = [auditor['id'] for auditor in auditor_list]

            script = f"SELECT * FROM findings WHERE auditor_id IN {tuple(ids) if len(ids) > 1 else f'({ids[0]})'}"
            findings = pd.read_sql_query(script, db)
            return findings

        elif len(auditors) == 0:
            script = "SELECT * FROM findings"
            findings = pd.read_sql_query(script, db)
            return findings

        else:
            return pd.DataFrame()


def get_findings_by_severities(*severities: str) -> pd.DataFrame:
    """
    Método para leitura de findings pela severidade.

    :param severities: Severidade do finding.
    :return: Pandas DataFrame.
    """

    with sqlite3.connect(DB_PATH) as db:
        if len(severities) == 0:
            # Retorna DataFrame da tabela findings
            script = "SELECT * FROM findings"
            findings = pd.read_sql_query(script, db)
            return findings

        script = f"SELECT * FROM findings WHERE severity IN " \
                 f"{tuple(severities) if len(severities) > 1 else f'({severities[0]!r})'}"
        findings = pd.read_sql_query(script, db)
        return findings


def get_row_count(table: str) -> int:
    """
    Método para encontrar o número de linhas existe em uma determinada tabela.

    :param table: Nome da tabela.
    :return: Número de linhas na tabela.
    """
    if type(table) is not str:
        print("Campo table só pode ser str!")
        return 0  # Retorna 0 se o tipo não for válido.

    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()

        script = f"SELECT COUNT(*) as total FROM {table}"
        try:
            cursor.execute(script)
        except sqlite3.OperationalError:
            return 0  # Retorna 0 se a tabela não for encontrada
        total = cursor.fetchone()[0]
        return total


def execute(script: str, values: list) -> list:
    """
    Método para executar um comando SQL genérico.

    :param script: Script SQL para execução.
    :param values: Lista com os valores a do script SQL.
    :return: Lista com os resultados encontrados.
    """

    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()
        cursor.execute(script, values)
        return cursor.fetchall()


def get_dataframe(script: str, *values) -> pd.DataFrame:
    """
    Método para retornar um dataframe pandas de acordo com o comando SQL passado como parâmetro

    :param script: Comando SQL.
    :param values: Valores usados no script.
    :return: Dataframe com o resultado da busca no DB
    """

    with sqlite3.connect(DB_PATH) as db:
        dataframe = pd.read_sql_query(script, db, params=values)
        return dataframe


def get_issues(**kwargs) -> pd.DataFrame:
    """
    Método para retornar issues de acordo com os pares de valor passados.
    Possíveis keywords:

    - auditor_id = int

    - auditor_ids = int[]

    - auditor_name = str

    - auditor_names = str[]

    - issue_id = int

    - issue_ids = int[]

    - issue_title = str

    - issue_titles = str[]

    :param kwargs: Par chave-valor para busca do(s) resultado(s).
    :return: DataFrame com issues e nomes dos auditores se não houver parâmetros.
             DataFrame com issues e auditores onde auditor_id(s) ou auditor_name(s) forem encontrados.
             DataFrame com issues e auditores onde issue_id(s) ou issue_title(s) forem encontrados.
    """

    if not kwargs:
        with sqlite3.connect(DB_PATH) as db:
            script = """
                            SELECT * FROM (SELECT issue.* FROM issues issue
                                    INNER JOIN auditors_issues ai on issue.id = ai.issue_id) LEFT JOIN
                                        (SELECT ad.* FROM auditors ad INNER JOIN auditors_issues ai 
                                            on ad.id = ai.auditor_id) 
                                                as auditor ON auditor.id;
                            """

            df = pd.read_sql_query(script, db)
            return df
    if "auditor_id" in kwargs:
        auditor_id = None
        try:
            auditor_id = kwargs['auditor_id']
        except KeyError:
            pass

        with sqlite3.connect(DB_PATH) as db:
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE id = {auditor_id}) as auditor
                                 INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                    INNER JOIN issues iss on si.issue_id = iss.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "auditor_ids" in kwargs:
        with sqlite3.connect(DB_PATH) as db:
            auditor_ids = tuple(kwargs['auditor_ids'])
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE id IN {auditor_ids}) as auditor
                                 INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                    INNER JOIN issues iss on si.issue_id = iss.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "auditor_name" in kwargs:
        auditor_name = None
        try:
            auditor_name = kwargs['auditor_name']
        except KeyError:
            pass

        with sqlite3.connect(DB_PATH) as db:
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE name = '{auditor_name}') as 
                                auditor INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                    INNER JOIN issues iss on si.issue_id = iss.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "auditor_names" in kwargs:
        with sqlite3.connect(DB_PATH) as db:
            auditor_names = tuple(kwargs['auditor_names'])
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE name IN {auditor_names}) as 
                                auditor INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                    INNER JOIN issues iss on si.issue_id = iss.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "issue_id" in kwargs:
        issue_id = None
        try:
            issue_id = kwargs['issue_id']
        except KeyError:
            pass

        with sqlite3.connect(DB_PATH) as db:
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM issues WHERE id = {issue_id}) as issue
                                 INNER JOIN auditors_issues ai on issue.id = ai.issue_id) as si
                                    INNER JOIN auditors ad on si.auditor_id = ad.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "issue_ids" in kwargs:
        with sqlite3.connect(DB_PATH) as db:
            issue_ids = tuple(kwargs['issue_ids'])
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM issues WHERE id IN {issue_ids}) as issue
                                 INNER JOIN auditors_issues ai on issue.id = ai.issue_id) as si
                                    INNER JOIN auditors ad on si.auditor_id = ad.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "issue_title" in kwargs:
        issue_title = None
        try:
            issue_title = kwargs['issue_title']
        except KeyError:
            pass

        with sqlite3.connect(DB_PATH) as db:
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM issues WHERE title = '{issue_title}') as issue
                                 INNER JOIN auditors_issues ai on issue.id = ai.issue_id) as si
                                    INNER JOIN auditors ad on si.auditor_id = ad.id;
                            """
            df = pd.read_sql_query(script, db)
            return df

    elif "issue_titles" in kwargs:
        with sqlite3.connect(DB_PATH) as db:
            issue_titles = tuple(kwargs['issue_titles'])
            script = f"""
                            SELECT * FROM (SELECT * FROM (SELECT * FROM issues WHERE title IN {issue_titles}) as issue
                                 INNER JOIN auditors_issues ai on issue.id = ai.issue_id) as si
                                    INNER JOIN auditors ad on si.auditor_id = ad.id;
                            """
            df = pd.read_sql_query(script, db)
            return df
