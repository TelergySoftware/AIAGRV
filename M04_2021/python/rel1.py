# =======================================================================================
# RELATÓRIO 1 - RELATÓRIO DA DENSIDADE DO GRAFO E DOS CNPJs ENVOLVIDOS EM CADA LICITAÇÃO
# =======================================================================================

# ano;municipio;tipo_processo_licitatorio;id_licitacao;valor;vínculo_em_uso;
# quantidade_cnpjs(vértices);quantidade_vinculos(arestas);densidade_grafo;
# tamanho_max_clique_encontrada;lista_de_cnpjs_separado_por_virgula

# OBS. colocar os CNPJs separados por vírgula permitirá que eles fiquem
# dentro de um único campo.
# OBS. vínculo_em_uso será o campo que irá identificar qual tipo de vínculo
# estamos usando. Podemos definir um padrão para esses vínculos
# - Vínculo Societário = 1
# - Vínculo Endereços  = 2
# - Vínculo Telefones  = 3

# ou usar strings até termos previsão de um banco de dados para isso:
# - Vínculo Societário = V_SOCIE
# - Vínculo Endereços  = V_ENDER
# - Vínculo Telefones  = V_TELEF

import pandas as pd

# Pacotes implementados
import carregamento_dados as cd
import ferramentas_grafos as fg


def main():
    dump_path = '../data/output/csv/'
    df = pd.read_pickle('../data/output/pickles/' + 'grafos_licitacoes')

    # Gera a coluna com o número de vértices do grafo das licitações
    df["quantidade_cnpjs"] = df['grafo'].apply(
        lambda x: fg.calcula_quantidade_vertices(x)
    )

    # Gera a coluna com o número de arestas do grafo das licitações
    df["quantidade_vinculos"] = df['grafo'].apply(
        lambda x: fg.calcula_quantidade_arestas(x)
    )

    # Gera a coluna com a densidade do grafo das licitações
    df["densidade"] = df['grafo'].apply(
        lambda x: fg.calcula_densidade(x)
    )

    # Gera a coluna das cliques dos grafos das licitações
    df["qtd_cliques"] = df['grafo'].apply(
        lambda x: fg.conta_cliques(x)
    )

    # Gera a coluna das cliques dos grafos das licitações
    df["tamanho_max_clique"] = df['grafo'].apply(
        lambda x: fg.calcula_tamanho_max_clique(fg.lista_cliques(x))
    )

    df = df[df.quantidade_cnpjs != 0]

    # A coluna que armazena o grafo para cada licitação não é necessária
    # no relatório e pode ser removida.
    df = df.drop('grafo', axis=1)

    # Salva o relatório 1 em arquivo csv
    df.to_csv(dump_path + 'relatorio_1')


if __name__ == '__main__':
    main()


