import pandas as pd
from collections import defaultdict
import os

data_path = "../data/input/"


def salvar_relacoes_entre_cnpjs():
    return pd.read_csv(
        data_path + 'relacao_societario_tratada.csv',
        header=None,
        names=['cnpj_1', 'cnpj_2'],
        dtype=str
    )


def salvar_informacoes_licitacoes():
    return pd.read_csv(
        data_path + 'infos_licitacoes.csv',
        dtype=str,
        sep=';'
    )


def salvar_cnpjs_por_licitacao():
    return pd.read_csv(
        data_path + 'licitacoes_cnpjs_licitantes.csv',
        dtype=str,
        sep=';'
    )


def cnpjs_relacionados_por_cnpj(relacoes_entre_cnpjs: pd.DataFrame) -> dict:
    """Cada CNPJ presente em algum processo licitatório é uma chave do dicionário.
    Essa chave acessa uma lista de CNPJs relacionados criada com base no arquivo
    de vínculos (sociedade, telefone, endereço...)

    d[cnpj] = [cnpj_1, ..., cnpj_n]    
    """
    d = defaultdict(list)
    relacoes = relacoes_entre_cnpjs.values
    for cnpj_1, cnpj_2 in relacoes:
        d[cnpj_1].append(cnpj_2)
    return d


def cnpjs_por_licitacao(cnpjs_por_licitacao: pd.DataFrame) -> dict:
    """Cada licitação é uma chave do dicionário.
    Essa chave acessa uma lista de CNPJs que participaram dessa licitação. 
    A lista é criada com base no arquivo de cnpjs licitantes.

    d[licitacao] = [cnpj_1, ..., cnpj_n]    
    """
    d = defaultdict(list)
    dados_licitacao = cnpjs_por_licitacao.values
    for licitacao, cnpj_licitante in dados_licitacao:
        d[licitacao].append(cnpj_licitante)
    return d


def licitacoes_por_municipio(informacoes_licitacoes: pd.DataFrame) -> dict:
    """Cada município presente em algum processo licitatório é uma chave do dicionário.
    Essa chave acessa uma lista de licitações naquele município.
    A lista é criada com base no arquivo de informações das licitações e de CNPJs licitantes.

    d[municipio] = [licitacao_1, ..., licitacao_n]    
    """
    d = defaultdict(list)
    informacoes_licitacoes = informacoes_licitacoes.iloc[:, :2]
    for licitacao, municipio in informacoes_licitacoes.values:
        d[municipio].append(licitacao)
    return d


def cnpjs_por_municipio(licitacoes_por_municio: dict, cnpjs_por_licitacao: dict) -> dict:
    """Cada município presente em algum processo licitatório é uma chave do dicionário.
    Essa chave acessa uma lista de cnpjs que participaram de licitacoes naquele município.
    A lista é criada com base no arquivo de informações das licitações e de CNPJs licitantes.

    d[municipio] = [cnpj_1, ..., cnpjs_n]    
    """
    d = defaultdict(list)
    for municipio, licitacoes_do_municipio in licitacoes_por_municio.items():
        for licitacao in licitacoes_do_municipio:
            cnpjs = cnpjs_por_licitacao[licitacao]
            for cnpj in cnpjs:
                d[municipio].append(cnpj)
    return d
