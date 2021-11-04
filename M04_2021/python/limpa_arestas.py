import pandas as pd


def main():
    # Carrega o arquivo de cnpjs por licitação e o arquivo de vínculos.
    dump_path = '../data/input/'

    relacoes_entre_cnpjs = pd.read_csv(
        dump_path + 'relacao_cnpjs_societario.csv',
        header=None,
        names=['cnpj_1', 'cnpj_2'],
        dtype=str,
        sep=' '
    )

    # Iniciamos um dicionário em que a chave é o par de cnpjs vinculados.
    # Inicialmente cada chave referencia uma lista vazia.
    lista_relacoes = relacoes_entre_cnpjs.values

    # Ordenamos o vínculo para não adicionarmos duas vezes uma mesma aresta.
    lista_relacoes_ordenada = [sorted(relacao) for relacao in lista_relacoes]

    # O uso do dicionário remove potenciais dados duplicados.
    d = {(relacao[0], relacao[1]): None for relacao in lista_relacoes_ordenada}

    with open(dump_path + 'relacao_societario_tratada.csv', 'w') as f:
        for cnpj_pair in d.keys():
            cnpj1 = cnpj_pair[0]
            cnpj2 = cnpj_pair[1]
            f.write(
                f"{cnpj1},{cnpj2}\n"
            )


if __name__ == '__main__':
    main()
