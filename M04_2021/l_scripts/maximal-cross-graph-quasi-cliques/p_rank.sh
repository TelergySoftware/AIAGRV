#!/bin/sh

# p_rank.sh valores socios

awk -F '[; ]' '
FILENAME == ARGV[1] {
    valor[$1] = $2
}

FILENAME == ARGV[2] {
    0. Armazenar valores de todas as licitações que um par concorre
    total[$1, $2] = 0
    1. Para cada linha, consultar o valor da licitacao em $3
    v = valor[$3]
    2. Somar ao par este valor: total[$1, $2] += valor[$3]
}

-> Imprimir o resultado em um arquivo iterando em total[$1, $2]

    
