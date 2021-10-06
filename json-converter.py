import json


def main():
    with open('sources/prod_parsed.json') as file:
        # Carrega o arquivo json em um buffer.
        buffer = json.load(file)
    with open('sources/output.json', 'w') as output:
        output_list = []  # Lista vazia para população do arquivo de saída.
        for element in buffer:
            data = {}  # Dicionário vazio para população de cada objeto do json.
            for key in element:
                # Iterando sobre as chaves do json carregado.
                if key not in ["auditors", "url", "contract_signatures", "test_signatures", "tools"]:
                    # Lista acima representa os dados que não são necessários.
                    if key == 'repos':
                        data[key] = len(element['repos'])
                    else:
                        data[key] = element[key]
            output_list.append(data)
        json.dump(output_list, output)


if __name__ == '__main__':
    main()
