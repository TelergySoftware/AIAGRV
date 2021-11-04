import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx


def inicializa_grafo():
    """Inicializa e retorna objeto networkX grafo."""
    return nx.Graph()


def gera_grafo_licitacao(licitacao: str, dic_relacoes, dic_licitacoes):
    """Gera o grafo da licitacao."""
    cnpjs_licitantes = dic_licitacoes[licitacao]
    G = inicializa_grafo()
    for cnpj in cnpjs_licitantes:
        G.add_node(cnpj)
        relacoes_do_cnpj = dic_relacoes[cnpj]
        for cnpj_relacionado in relacoes_do_cnpj:
            if cnpj_relacionado in cnpjs_licitantes:
                G.add_edge(cnpj, cnpj_relacionado)
    return G


def lista_cliques(grafo: nx.Graph) -> list:
    """Retorna a lista de cliques encontradas no grafo."""
    return list(nx.find_cliques(grafo))


def conta_cliques(grafo: nx.Graph) -> int:
    """Conta o numero de cliques ignorando aquelas de tamanho menor que 2."""
    cliques = list(nx.find_cliques(grafo))
    qtd = 0
    for clique in cliques:
        tamanho = len(clique)
        if tamanho >= 2:
            qtd += 1
    return qtd


def expande_cliques(linha: pd.Series) -> list:
    """Retorna uma lista de tuplas em que cada tupla armazena os cnpjs daquela
    clique e o tamanho da clique (se a clique é menor que 2 é ignorada.).
    """
    resultado = pd.DataFrame(linha)
    grafo = linha['grafo']
    cliques = list(nx.find_cliques(grafo))
    resultado = []
    for clique in cliques:
        tamanho = len(clique)
        if tamanho >= 2:
            resultado.append((clique, tamanho))
    return resultado


def calcula_densidade(grafo: nx.Graph) -> float:
    """Retorna a densidade do grafo."""
    return nx.density(grafo)


def lista_cnpjs_max_clique(grafo: nx.Graph) -> list:
    """Retorna lista dos cnpjs integrantes da max clique."""
    cliques = list(nx.find_cliques(grafo))
    cliques.sort(reverse=True, key=len)
    try:
        return cliques[0]
    except:
        return []


def calcula_tamanho_max_clique(cliques: list):
    """Retorna o tamanho da clique de maior tamanho dentre cliques numa lista."""
    try:
        return max([len(clique) for clique in cliques])
    except:
        return 0


def calcula_quantidade_vertices(grafo: nx.Graph) -> int:
    """Retorna o numero de vertices em um grafo."""
    return grafo.number_of_nodes()


def calcula_quantidade_arestas(grafo: nx.Graph) -> int:
    """Retorna o numero de arestas em um grafo."""
    return grafo.number_of_edges()


def calcula_grau_competicao(grafo: nx.Graph) -> int:
    """Calcula o grau de competição para o grafo de uma licitação.
    O grau de competição é definido como a razão entre o número de
    componentes conexas e o total de vértices.
    """
    n_componentes_conexas = nx.number_connected_components(grafo)
    n_vertices = calcula_quantidade_vertices(grafo)
    if n_vertices != 0:
        return n_componentes_conexas / n_vertices
    else:
        return float('NaN')


def plota_grafo(grafo: nx.Graph, titulo: str, caminho_saida: str = None) -> plt.figure:
    """Plota o grafo.
    """
    plt.figure(figsize=(15, 15))
    pos = nx.spring_layout(grafo)
    nx.draw_networkx_nodes(grafo, pos, node_size=25)
    nx.draw_networkx_edges(grafo, pos)
    plt.title(titulo)
    if caminho_saida:
        plt.savefig(caminho_saida)
