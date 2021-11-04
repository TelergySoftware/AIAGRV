#!/bin/sh

if [ -z "$2" ]
then
    printf "Uso: $0 padrões valores

Cada linha do arquivo \"padrões\" contém dois campos separados por
' ': CNPJs e licitações, ambos separados por ','.  Cada CNPJ e
licitação pode ser seguido de '#' e do ruído coberto, como
multidupehack informa com a opção --pn.  Sem a informação, se
considere que o ruído é 0.

Cada linha do arquivo \"valores\" contém dois campos separados por ';'
ou ' ': uma licitação e seu valor.
"
    # Esse formato pode ser diferente, modificando o script abaixo.
    exit
fi

awk -F '[; ]' '
FILENAME == ARGV[1] {
    value[$1] = $2 }

FILENAME == ARGV[2] {
    sum_of_values = 0
    grade = 0
    nb_of_vertices = split($1, a, ",")
    max_nb_of_edges = nb_of_vertices * (nb_of_vertices - 1)
    nb_of_procurements = split($2, a, ",")
    for (i = 1; i <= nb_of_procurements; ++i) {
        split(a[i], b, "#")
        sum_of_values += value[b[1]]
        grade += value[b[1]] * (max_nb_of_edges - b[2])^2 / max_nb_of_edges / 2 }
    printf "%s\t%s\t%d\t%d\t%d\t%d\n", $1, $2, grade, nb_of_vertices, nb_of_procurements, sum_of_values }' "$2" "$1" | LC_ALL=C sort -k 3,3gr -k 4,4nr -k 5nr -k 1
