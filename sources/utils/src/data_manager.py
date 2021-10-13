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


if __name__ == '__main__':
    json_to_sql('../../data/resources/JSON/output.json', '../../data/resources/DB/output.db', True)
