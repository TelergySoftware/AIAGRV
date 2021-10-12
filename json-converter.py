import json


def main():
    with open('prod_parsed-original_para_trabalhar.json', encoding='utf-8') as file:
        # Carrega o arquivo json em um buffer.
        buffer = json.load(file)
    with open('output.json', 'w', encoding='utf-8') as output:
        output_list = []  # Lista vazia para população do arquivo de saída.
        for element in buffer:
            data = {}  # Dicionário vazio para população de cada objeto do json.
            for key in element:
                # Iterando sobre as chaves do json carregado.
                if key not in ["url", "contract_signatures", "test_signatures", "tools"]:
                    # Lista acima representa os dados que não são necessários.
                    if key == 'repos':
                        data[key] = len(element['repos'])
                    elif key == 'specification':
                        data[key] = len(element['specification'])
                    else:
                        data[key] = element[key]
            output_list.append(data)
        json.dump(output_list, output, indent=4)


if __name__ == '__main__':
    main()
