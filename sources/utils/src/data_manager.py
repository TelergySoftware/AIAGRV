from __future__ import annotations
from json_converter import convert
from os.path import exists
import os

import sqlite3


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

    with sqlite3.connect('../../data/resources/DB/output.db') as db:
        # Conectamos ao banco já criado.
        cursor = db.cursor()
        auditors = set()  # Set vazio com os nomes dos auditores.
        findings = {'title': [], 'severity': [], 'auditor': []}  # Dicionário para estruturado para a tabela findings.
        for data in json_data:
            if len(data['auditors']) == 0:
                # Atribui o nome None aos elementos sem auditores.
                auditors.add('None')
                for report in data['findings']:
                    # Popula o dicionário findings.
                    findings['title'].append(report['title'])
                    findings['severity'].append(report['severity'])
                    findings['auditor'].append('None')
            for auditor in data['auditors']:
                auditors.add(auditor)
                for report in data['findings']:
                    # Popula o dicionário findings.
                    findings['title'].append(report['title'])
                    findings['severity'].append(report['severity'])
                    findings['auditor'].append(auditor)

        for auditor in auditors:
            # Adiciona os auditores na tabela auditors do DB.
            sql_script = """
            INSERT INTO auditors(name) VALUES (?);
            """
            cursor.execute(sql_script, [auditor])
            db.commit()

        for title, severity, auditor in zip(findings['title'], findings['severity'], findings['auditor']):
            # Adiciona os findings na tabela findings do DB.
            sql_script = """
            INSERT INTO findings(title, severity, auditor_id) VALUES (?, ?, ?);
            """
            cursor.execute(sql_script, [title, severity, get_auditor_id(auditor, db_path)])
            db.commit()

    return True


def get_auditor_id(name: str, db_path: str) -> int:
    """
    Função utilizada para encontrar o ID dos auditores através do nome dado como parâmetro.

    :param name: Nome do auditor.
    :param db_path: Local do banco de dados.
    :return: ID inteiro do auditor.
    """

    with sqlite3.connect(db_path) as db:
        # Conecta ao banco de dados.
        cursor = db.cursor()
        # Seleciona o id de acordo com o nome do auditor.
        cursor.execute("SELECT id FROM auditors WHERE name = ?", [name])
        # Retorna o valor encontrado pelo SELECT acima.
        auditor_id = cursor.fetchone()
        return auditor_id[0]


class API:
    """
    API para RDU do banco de dados.
    O caminho do banco de dados é dado por DB_PATH e pode ser alterado pelo método db_path.
    """
    DB_PATH = './sources/data/resources/DB/output.db'

    @classmethod
    def get_auditor(cls, auditor_id: int) -> dict:
        """
        Método para leitura do auditor pelo ID.

        :param auditor_id: ID do auditor
        :return: Dicionário representando o auditor
        """
        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()
            script = "SELECT * FROM auditors WHERE id = ?"
            cursor.execute(script, [auditor_id])
            auditor = cursor.fetchone()
            return {'id': auditor[0], 'name': auditor[1]}

    @classmethod
    def get_findings_by_auditor(cls, auditor: str | int) -> list[dict]:
        """
        Método para leitura de findings pelo nome ou ID do auditor.

        :param auditor: Nome ou ID do auditor.
        :return: Lista de dicionários representado os findings.
        """
        if type(auditor) not in [str, int]:
            print("O campo auditor só pode ser str ou int!")
            return []  # Retorna lista vazia se o tipo não for válido.

        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()

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
            findings = []
            auditor = cls.get_auditor(auditor)
            for finding in buffer:
                finding_dict = {'id': finding[0], 'title': finding[1], 'severity': finding[2], 'auditor': auditor}
                findings.append(finding_dict)

            return findings

    @classmethod
    def get_findings_by_severity(cls, severity: str) -> list[dict]:
        """
        Método para leitura de findings pela severidade.

        :param severity: Severidade do finding.
        :return: Lista de dicionários representado os findings.
        """
        if type(severity) is not str:
            print("Campo severity só pode ser str!")
            return []  # Retorna lista vazia se o tipo não for válido.

        with sqlite3.connect(cls.DB_PATH) as db:
            cursor = db.cursor()

            script = "SELECT * FROM findings WHERE severity = ?"
            try:
                cursor.execute(script, [severity])
            except sqlite3.OperationalError:
                return []  # Retorna lista vazia se der algum erro.
            buffer = cursor.fetchall()
            findings = []
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
    def db_path(cls, path: str = None) -> str:
        """
        Muda e/ou retorna o caminho para o banco de dados.
        :param path: Caminho para o banco de dados.
        :return: Retorna caminho do banco de dados.
        """

        if path is not None:
            cls.DB_PATH = path

        return cls.DB_PATH


# if __name__ == '__main__':
#     json_to_sql('../../data/resources/JSON/output.json', '../../data/resources/DB/output.db', True)
