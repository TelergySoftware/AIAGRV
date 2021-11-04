# Alimenta Multidupehack
# Requer execução do script de modelagem de grafos.

# Recebe os arquivos de cnpjs por licitação e de relações entre cnpjs.
# Trata os dados removendo relações duplicadas (considerando que modelamos
# licitações como grafos não direcionados). Salva os dados processados
# em um arquivo .csv onde cada linha apresenta 3 colunas:

# cnpj_1    cnpj_2      licitação

# Ou seja, apresenta um vínculo entre CNPJs e a licitação onde esse vínculo
# ocorre. Haverá repetição de duplas de cnpjs, mas o valor no campo licitação
# deve variar entre as linhas que apresentam a mesma dupla.

import pandas as pd
import carregamento_dados as cd


def main():
    # Carrega o arquivo de cnpjs por licitação e o arquivo de vínculos.
    dump_path = '../data/output/csv/'
    relacoes_entre_cnpjs = cd.salvar_relacoes_entre_cnpjs()
    cnpjs_por_licitacao = cd.salvar_cnpjs_por_licitacao()
    print("Raw files loaded.")

    # O uso do dicionário remove potenciais dados duplicados.
    d: dict = {}

    # Vamos extrair arestas dos grafos criados pelo script modela_grafos.py,
    # para isso, utilizamos o resultado salvo em pickle.
    df = pd.read_pickle('../data/output/pickles/' + 'grafos_licitacoes')
    print("Graph data loaded.")

    # Extraimos somente as colunas 'licitacao' (para identificar o grafo) e 'grafo'.
    lic_graph = df[['licitacao', 'grafo']].values

    # Inicializa o dicionário com listas vazias para cada aresta,
    # sendo cada aresta um par de CNPJs.
    for lic, graph in lic_graph:
        edges = graph.edges
        if len(edges) != 0:
            # muitas licitações não possuem qualquer aresta
            # por isso checamos se há aresta ou não
            for edge in edges:
                d[edge] = []

    # Alimenta o dicionário com as licitações onde esse par de CNPJs
    # ocorre.
    for lic, graph in lic_graph:
        edges = graph.edges
        if len(edges) != 0:
            for edge in edges:
                d[edge].append(lic)

    # Temos até aqui um dicionário indexado por par de CNPJs em que cada
    # chave armazena uma lista de licitações. Percorremos então cada chave
    # do dicionário e entramos na lista de licitações. Para cada licitação
    # desta chave, criamos uma nova linha de saída.
    with open(dump_path + 'edges', 'w') as f:
        for cnpj_pair in d.keys():
            cnpj1 = cnpj_pair[0]
            cnpj2 = cnpj_pair[1]
            licitacoes = d[cnpj_pair]
            for licitacao in licitacoes:
                f.write(f"{cnpj1},{cnpj2},{licitacao}\n")
    print('Output saved to', dump_path + 'edges')


if __name__ == '__main__':
    main()
