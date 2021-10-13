from __future__ import annotations
import json


def convert(json_input: str, modify: bool = False) -> dict | list:
    """
    Transforma um json em um dicionário ou uma lista de dicionários.

    :param json_input: Local do arquivo json.
    :param modify: Boolean para modificar ou não a entrada.
    :return: Dicionário ou lista de dicionários resultado da conversão.
    """

    with open(json_input, encoding='utf-8') as file:
        # Carrega o arquivo json em um buffer.
        buffer = json.load(file)

    output_list = []  # Lista vazia para população do arquivo de saída.
    for element in buffer:
        data = {}  # Dicionário vazio para população de cada objeto do json.
        for key in element:
            # Iterando sobre as chaves do json carregado.
            if modify:
                if key not in ["url", "contract_signatures", "test_signatures", "tools"]:
                    # Lista acima representa os dados que não são necessários.
                    if key == 'repos':
                        data[key] = len(element['repos'])
                    elif key == 'specification':
                        data[key] = len(element['specification'])
                    else:
                        data[key] = element[key]
            else:
                data[key] = element[key]
        output_list.append(data)
    return output_list if len(output_list) > 1 else output_list[0]


def main():
    with open('../../data/resources/JSON/output.json', 'w', encoding='utf-8') as output:
        output_list = convert('prod_parsed-original_para_trabalhar.json')
        json.dump(output_list, output, indent=4)


if __name__ == '__main__':
    main()
