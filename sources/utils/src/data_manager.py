from __future__ import annotations
from json_converter import convert
import numpy as np
import pandas as pd
from os.path import exists
import os

import sqlite3

sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))


def json_to_sql(json_data: str | list, db_path: str, is_new: bool) -> bool:
    """
    Função utilizada para transformar dados estruturados em JSON para SQL.

    :param json_data: JSON contendo os dados que serão passados para o DB SQLite.
    :param db_path: Local do banco de dados para conversão.
    :param is_new: Boolean informando se este é um DB novo ou um existente.
    :return: Retorna True se a operação foi realizada com sucesso.
    """

    if type(json_data) == str:
        json_data = convert(json_data)

    if is_new:
        try:
            # Se for um banco novo, tenta remover um possível banco antigo.
            os.remove(db_path)
        except FileNotFoundError:
            # Ignoramos o erro se o caminho do banco não for encontrado.
            pass
        with sqlite3.connect(db_path) as db:
            # Criamos e conectamos ao banco de dados.
            cursor = db.cursor()

            with open('../resources/DB/CREATE_DB.sql', 'r') as create_script:
                # Criamos as tabelas de acordo com o script CREATE_DB.sql.
                statements = create_script.read().split(';')
                for statement in statements:
                    # Como não é possível executar múltiplos statements ao mesmo tempo,
                    # separamos o script anterior por ';' em uma lista e executamos um por um.
                    cursor.execute(statement)
                    db.commit()

    if not exists(db_path):
        print("DB não existe ou falhou em ser criado.")
        return False

    with sqlite3.connect(db_path) as db:
        # Conectamos ao banco já criado.
        cursor = db.cursor()

        # Dicionário estruturado para a tabela issues.
        issues = {'id': [], 'title': [], 'repos': [], 'type': [], 'start_date': [], 'end_date': [],
                  'auditors_count': [], 'specifications': [], 'published': [], 'findings_count': []}
        auditors = {'name': [], 'issue_id': []}  # Dicionário estruturado para a tabela auditors.
        findings = {'title': [], 'severity': [],
                    'auditor': [], 'issue_id': []}  # Dicionário estruturado para a tabela findings.

        for issue_id, data in enumerate(json_data):
            issues['id'].append(issue_id + 1)
            issues['title'].append(data['title'])
            issues['repos'].append(data['repos'])
            issues['type'].append(data['type'])
            issues['start_date'].append(data['start_date'])
            issues['end_date'].append(data['end_date'])
            issues['auditors_count'].append(int(len(data['auditors'])))
            issues['specifications'].append(data['specification'])
            issues['published'].append(1 if data['published'] == 'true' else 0)
            issues['findings_count'].append(int(len(data['findings'])))

            if int(len(data['auditors'])) == 0:
                # Atribui o nome None aos elementos sem auditores.
                auditors['name'].append('None')
                auditors['issue_id'].append(issue_id + 1)
                for report in data['findings']:
                    # Popula o dicionário findings.
                    findings['title'].append(report['title'])
                    findings['severity'].append(report['severity'])
                    findings['auditor'].append('None')
                    findings['issue_id'].append(issue_id + 1)
            else:
                for auditor in data['auditors']:
                    auditors['name'].append(auditor) if not auditor.endswith(' ') else auditors['name'].append(auditor[:-1])
                    auditors['issue_id'].append(issue_id + 1)

                    for report in data['findings']:
                        # Popula o dicionário findings.
                        findings['title'].append(report['title'])
                        findings['severity'].append(report['severity'])
                        findings['auditor'].append(auditor) if not auditor.endswith(' ') else \
                            findings['auditor'].append(auditor[:-1])
                        findings['issue_id'].append(issue_id + 1)

        issues_df = pd.DataFrame(issues)
        auditors_df = pd.DataFrame(auditors)
        findings_df = pd.DataFrame(findings)

        for issue in issues_df.iloc():
            sql_script = """
            INSERT INTO issues(title, repos, type, start_date, end_date, auditors_count,
            specifications, published, findings_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(sql_script, issue.values[1:])
            db.commit()

        auditors_set = set()
        for auditor in auditors_df.iloc():
            # Adiciona os auditores na tabela auditors do DB.
            if auditor['name'] not in auditors_set:
                sql_script = """
                            INSERT INTO auditors(name) VALUES (?);
                            """
                cursor.execute(sql_script, [auditor['name']])
                db.commit()
            sql_script = """
            INSERT INTO auditors_issues(auditor_id, issue_id) VALUES (?, ?);
            """
            cursor.execute(sql_script, [API.get_auditor_id(auditor['name']), auditor['issue_id']])
            auditors_set.add(auditor['name'])

        for finding in findings_df.iloc():
            # Adiciona os findings na tabela findings do DB.
            sql_script = """
            INSERT INTO findings(title, severity, auditor_id, issue_id) VALUES (?, ?, ?, ?);
            """
            cursor.execute(sql_script, finding.values)
            db.commit()

    return True


class API:
    """
    API para RDU do banco de dados.
    O caminho do banco de dados é dado por DB_PATH e pode ser alterado pelo método db_path.
    """
    DB_PATH = './sources/data/resources/DB/output.db'
    SEVERITIES = ['Low', 'Medium', 'High', 'Informational', 'Undetermined']

    @classmethod
    def get_auditors(cls, *auditor_ids: int) -> dict | list[dict]:
        """
        Método para leitura de auditores pelo ID.

        :param auditor_ids: ID do auditor
        :return: Dicionário representando o auditor
        """
        with sqlite3.connect(cls.DB_PATH) as db:
            if len(auditor_ids) > 0:
                auditors = []
                for auditor_id in auditor_ids:
                    cursor = db.cursor()
                    script = "SELECT * FROM auditors WHERE id = ?"
                    cursor.execute(script, [auditor_id])
                    auditor = cursor.fetchone()
                    auditors.append({'id': auditor[0], 'name': auditor[1]})
                return auditors[0] if len(auditors) == 1 else auditors
            elif len(auditor_ids) == 0:
                auditors = []
                cursor = db.cursor()
                script = "SELECT * FROM auditors"
                cursor.execute(script)
                buffer = cursor.fetchall()
                for auditor in buffer:
                    auditors.append({'id': auditor[0], 'name': auditor[1]})
                return auditors
            else:
                return []

    @classmethod
    def get_auditor_id(cls, name: str) -> int:
        """
        Função utilizada para encontrar o ID dos auditores através do nome dado como parâmetro.

        :param name: Nome do auditor.
        :return: ID inteiro do auditor.
        """

        with sqlite3.connect(cls.DB_PATH) as db:
            # Conecta ao banco de dados.
            cursor = db.cursor()
            # Seleciona o id de acordo com o nome do auditor.
            cursor.execute("SELECT id FROM auditors WHERE name = ?", [name])
            # Retorna o valor encontrado pelo SELECT acima.
            auditor_id = cursor.fetchone()
            return auditor_id[0]

    @classmethod
    def get_findings_by_auditors(cls, *auditors: str | int) -> list[dict]:
        """
        Método para leitura de findings pelo nome ou ID do auditor.

        :param auditors: Nome ou ID do auditor.
        :return: Lista de dicionários representado os findings.
        """

        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()

            if len(auditors) > 0:
                findings = []
                for auditor in auditors:
                    if type(auditor) == int:
                        script = "SELECT * FROM findings WHERE auditor_id = ?"
                    else:
                        script = "SELECT id FROM auditors WHERE name = ?"
                        try:
                            cursor.execute(script, [auditor])
                        except sqlite3.OperationalError:
                            return []  # Retorna lista vazia se der algum erro.
                        auditor = cursor.fetchone()[0]
                        script = "SELECT * FROM findings WHERE auditor_id = ?"

                    cursor.execute(script, [auditor])
                    buffer = cursor.fetchall()
                    auditor = cls.get_auditors(auditor)
                    for finding in buffer:
                        finding_dict = {'id': finding[0], 'title': finding[1], 'severity': finding[2],
                                        'auditor': auditor}
                        findings.append(finding_dict)

                return findings

            elif len(auditors) == 0:
                script = "SELECT * FROM findings"

                cursor.execute(script)
                findings = []
                buffer = cursor.fetchall()
                for finding in buffer:
                    auditor = cls.get_auditors(finding[3])
                    finding_dict = {'id': finding[0], 'title': finding[1], 'severity': finding[2],
                                    'auditor': auditor}
                    findings.append(finding_dict)

                return findings

            else:
                return []

    @classmethod
    def get_findings_by_severities(cls, *severities: str) -> list[dict]:
        """
        Método para leitura de findings pela severidade.

        :param severities: Severidade do finding.
        :return: Lista de dicionários representado os findings.
        """

        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()

            findings = []
            for severity in severities:
                script = "SELECT * FROM findings WHERE severity = ?"
                try:
                    cursor.execute(script, [severity])
                except sqlite3.OperationalError:
                    return []  # Retorna lista vazia se der algum erro.
                buffer = cursor.fetchall()
                for finding in buffer:
                    auditor = cls.get_auditor(finding[3])
                    finding_dict = {'id': finding[0], 'title': finding[1], 'severity': finding[2], 'auditor': auditor}
                    findings.append(finding_dict)

            return findings

    @classmethod
    def get_row_count(cls, table: str) -> int:
        """
        Método para encontrar o número de linhas existe em uma determinada tabela.

        :param table: Nome da tabela.
        :return: Número de linhas na tabela.
        """
        if type(table) is not str:
            print("Campo table só pode ser str!")
            return 0  # Retorna 0 se o tipo não for válido.

        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()

            script = f"SELECT COUNT(*) as total FROM {table}"
            try:
                cursor.execute(script)
            except sqlite3.OperationalError:
                return 0  # Retorna 0 se a tabela não for encontrada
            total = cursor.fetchone()[0]
            return total

    @classmethod
    def execute(cls, script: str, values: list) -> list:
        """
        Método para executar um comando SQL genérico.

        :param script: Script SQL para execução.
        :param values: Lista com os valores a do script SQL.
        :return: Lista com os resultados encontrados.
        """

        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()
            cursor.execute(script, values)
            return cursor.fetchall()

    @classmethod
    def get_dataframe(cls, script: str) -> pd.DataFrame:
        """
        Método para retornar um dataframe pandas de acordo com o comando SQL passado como parâmetro

        :param script: Comando SQL.
        :return: Dataframe com o resultado da busca no DB
        """

        with sqlite3.connect(cls.DB_PATH) as db:
            dataframe = pd.read_sql_query(script, db)
            return dataframe

    @classmethod
    def get_issues(cls, **kwargs) -> pd.DataFrame | str | int | list[str | int]:
        """
        Método para retornar issues de acordo com os pares de valor passados.
        possíveis keywords:
        - auditor_id = int
        - auditor_ids = int[]
        - auditor_name = str
        - auditor_names = str[]
        - issue_id = int
        - issue_ids = int[]
        - issue_title = str
        - issue_titles = str[]

        TODO: issue_id implementation
        TODO: issue_ids implementation
        TODO: issue_title implementation
        TODO: issue_titles implementation

        :param kwargs: Par chave valor para busca do(s) resultado(s).
        :return: DataFrame com issues e nomes dos auditores se não houver parâmetros,
                 DataFrame com issues onde auditor_id(s) ou auditor_name(s) forem encontrados,
                 título da issue se passado keyword issue_id,
                 id da issue se passado keyword issue_title,
                 lista com títulos das issues se passado keyword issue_ids,
                 lista com ids das issues se passado keyword issue_titles.
        """

        if not kwargs:
            with sqlite3.connect(cls.DB_PATH) as db:
                script = """
                                SELECT * FROM
                                    (SELECT issue.* FROM issues issue
                                        INNER JOIN auditors_issues ai on issue.id = ai.issue_id)
                                    LEFT JOIN
                                    (SELECT ad.* FROM auditors ad
                                        INNER JOIN auditors_issues ai on ad.id = ai.auditor_id) as auditor ON auditor.id;
                                """

                df = pd.read_sql_query(script, db)
                return df
        if "auditor_id" in kwargs:
            auditor_id = None
            try:
                auditor_id = kwargs['auditor_id']
            except KeyError:
                pass

            with sqlite3.connect(cls.DB_PATH) as db:
                script = f"""
                                SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE id = {auditor_id}) as auditor
                                     INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                        INNER JOIN issues iss on si.issue_id = iss.id;
                                """
                df = pd.read_sql_query(script, db)
                return df

        elif "auditor_ids" in kwargs:
            with sqlite3.connect(cls.DB_PATH) as db:
                auditor_ids = tuple(kwargs['auditor_ids'])
                script = f"""
                                SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE id IN {auditor_ids}) as auditor
                                     INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                        INNER JOIN issues iss on si.issue_id = iss.id;
                                """
                df = pd.read_sql_query(script, db)
                return df

        elif "auditor_name" in kwargs:
            auditor_name = kwargs['auditor_name']
            with sqlite3.connect(cls.DB_PATH) as db:
                script = f"""
                                SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE name = '{auditor_name}') as auditor
                                     INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                        INNER JOIN issues iss on si.issue_id = iss.id;
                                """
                df = pd.read_sql_query(script, db)
                return df

        elif "auditor_names" in kwargs:
            with sqlite3.connect(cls.DB_PATH) as db:
                auditor_names = tuple(kwargs['auditor_names'])
                script = f"""
                                SELECT * FROM (SELECT * FROM (SELECT * FROM auditors WHERE name IN {auditor_names}) as auditor
                                     INNER JOIN auditors_issues ai on auditor.id = ai.auditor_id) as si
                                        INNER JOIN issues iss on si.issue_id = iss.id;
                                """
                df = pd.read_sql_query(script, db)
                return df

    @classmethod
    def db_path(cls, path: str = None) -> str:
        """
        Muda e/ou retorna o caminho para o banco de dados.
        :param path: Caminho para o banco de dados.
        :return: Retorna caminho do banco de dados.
        """

        if path is not None:
            cls.DB_PATH = path

        return cls.DB_PATH

    @classmethod
    def severities(cls) -> list[str]:
        """
        Método para consulta das severidades disponíveis.
        :return: Lista com os labels de severidade.
        """
        return cls.SEVERITIES


if __name__ == '__main__':
    API.db_path('../../data/resources/DB/output.db')
    # json_to_sql('../../data/resources/JSON/output.json', '../../data/resources/DB/output.db', True)
