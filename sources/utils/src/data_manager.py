from __future__ import annotations
from json_converter import convert
import numpy as np
import pandas as pd
from os.path import exists
import os
import API

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
            auditor_id = API.get_auditor_id(finding['auditor'])
            cursor.execute(sql_script, [finding['title'], finding['severity'], auditor_id, finding['issue_id']])
            db.commit()

    return True


if __name__ == '__main__':
    API.db_path('../../data/resources/DB/output.db')
    json_to_sql('../../data/resources/JSON/output.json', '../../data/resources/DB/output.db', True)
