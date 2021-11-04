# ==============================================================================
# RELATÓRIO 2 - RELATÓRIO DAS CLIQUES MAXIMIMAIS POR LICITAÇÃO
# ==============================================================================
# *ATENÇÃO: CADA CLIQUE MAXIMAL ENCONTRADA IRÁ GERAR UMA NOVA LINHA NESTE RELATÓRIO

# ano;municipio;tipo_processo_licitatorio;id_licitacao;valor;vínculo_em_uso;
# quantidade_cnpjs(vértices);quantidade_vinculos(arestas);densidade_grafo;
# tam_clique_encontrada;lista_de_cnpjs_separado_por_virgula_compondo_clique

# OBS. se uma licitação tiver 5 cliques maximais, ela aparecerá em 5 linhas,
# cada linha só mudará o tamanho da clique e os CNPJs envolvidos

# A menor clique interessante é a de tamanho 2.


from collections import defaultdict

import pandas as pd

import carregamento_dados as cd
import ferramentas_grafos as fg


def main():
    # Carrega os 3 arquivos principais.
    csv_path = '../data/output/csv/'
    pickle_path = '../data/output/pickles/'
    relacoes_entre_cnpjs = cd.salvar_relacoes_entre_cnpjs()
    informacoes_licitacoes = cd.salvar_informacoes_licitacoes()
    cnpjs_por_licitacao = cd.salvar_cnpjs_por_licitacao()

    # Cria dicionários de relações entre CNPJs e de CNPJs por licitação.
    d_relacoes = cd.cnpjs_relacionados_por_cnpj(relacoes_entre_cnpjs)
    d_licitacoes = cd.cnpjs_por_licitacao(cnpjs_por_licitacao)

    # Cria a lista de licitações que possui informações de CNPJs licitantes.
    dados_licitacao = cnpjs_por_licitacao.values
    licitacoes = [licitacao for licitacao, _ in dados_licitacao]

    # Cria dicionário que armazena informações principais por licitação.
    d = {
        licitacao: {
            'cnpjs': [],
            'ano': None,
            'municipio': None,
            'modalidade': None,
            'valor': None,
            'grafo': None,
            'cliques': None
        } for licitacao in licitacoes
    }

    # Alimenta o dicionário com a lista de CNPJs licitantes por licitação.
    for licitacao, cnpj in cnpjs_por_licitacao.values:
        d[licitacao]['cnpjs'].append(cnpj)

    # Alimenta o dicionário com dados de cada licitação (município, ano, ...)
    # Utiliza a posição da coluna no arquivo original, deve ser checado se essa
    # ordem for alterada.
    for l in informacoes_licitacoes.values:
        try:
            d[l[0]]['municipio'] = l[1]
            d[l[0]]['ano'] = l[5]
            d[l[0]]['modalidade'] = l[3]
            d[l[0]]['valor'] = l[7]
        except:
            # Exceção para licitações que não possuem informações de CNPJs licitantes,
            # nada é feito.
            pass

    # Gera o grafo de cada licitação e, em seguida, lista as cliques desse grafo.
    # Idealmente, poderia ser recuperado do arquivo gerado em gera_grafos_licitacoes.py
    # Como a execução do script é relativamente rápida, optou-se por recalcular esses dados.
    for licitacao in licitacoes:
        grafo = fg.gera_grafo_licitacao(
            licitacao, d_relacoes, d_licitacoes
        )
        cliques = fg.lista_cliques(grafo)

        d[licitacao]['grafo'] = grafo
        d[licitacao]['cliques'] = cliques

    # Cria o dicionário para armazenar informações relativas às cliques encontradas.
    c = defaultdict(dict)

    clique_id = 0
    for licitacao in pd.Series(licitacoes).unique():
        for clique in d[licitacao]['cliques']:
            tamanho_clique = len(clique)
            if tamanho_clique >= 2:
                cnpjs = []
                printable_cnpjs = ''
                for cnpj in clique:
                    printable_cnpjs += (str(cnpj) + ';')
                    cnpjs.append(cnpj)
                c[clique_id] = {
                    'ano': d[licitacao]['ano'],
                    'municipio': d[licitacao]['municipio'],
                    'tipo_processo_licitatorio': d[licitacao]['modalidade'],
                    'id_licitacao': licitacao,
                    'valor': d[licitacao]['valor'],
                    'vinculo_em_uso': '1',
                    'quantidade_cnpjs': len(d[licitacao]['cnpjs']),
                    'quantidade_vinculos': fg.calcula_quantidade_arestas(d[licitacao]['grafo']),
                    'densidade_grafo': fg.calcula_densidade(d[licitacao]['grafo']),
                    'tam_clique_encontrada': tamanho_clique,
                    'lista_de_cnpjs_compondo_clique': printable_cnpjs,
                    'cnpjs': cnpjs
                }
                clique_id += 1

    # Transforma o dicionário de cliques em um DataFrame para melhor visualização dos
    # dados e para salvamento do arquivo.
    cliques = pd.DataFrame.from_dict(
        data=c,
        orient='index'
    )

    # Salva o resultado em arquivo .csv e em Pickle para processamento posterior.
    cliques.to_csv(csv_path + 'relatorio_2')
    cliques.to_pickle(pickle_path + 'cliques_picles')


if __name__ == '__main__':
    main()

