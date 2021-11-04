#!/bin/sh

# multidupehack deve ser instalado.

WEIGHT=0.6

if [ -z "$1" -o "$1" = "-h" -o "$1" = "--help" ]
then
    printf "Uso: $0 vínculo-societário [vínculo-telefônico] [peso-telefônico]

Cada linha de um arquivo com vínculos contém três campos separados por
',' e/ou ' ': dois CNPJs vínculados e uma licitação da qual
participaram.  Um CNPJ é uma cadeia de dígitos com um número fixo de
dígitos.  Uma licitação também.

O peso telefônico padrão é $WEIGHT.
"
    # Esse formato pode ser diferente, modificando o script abaixo.
    exit
fi

TMP=`mktemp -dt maximal-cross-graph-quasi-cliques.sh.XXXXXX`
trap "rm -r $TMP 2>/dev/null" 0

# Vínculos são simétricos e devem ser inseridos em ordem crescente de peso (para multidupehack, a última definição de uma arresta vale).
if [ -n "$2" ]
then
    if [ -n "$3" ]
    then
	WEIGHT=$3
    fi
    awk -F '[, ]+' -v weight=$WEIGHT '{ print $1, $2, $3, weight "\n" $2, $1, $3, weight }' "$2" > $TMP/edges
fi
awk -F '[, ]+' '{ print $1, $2, $3, "1\n" $2, $1, $3, 1 }' "$1" >> $TMP/edges

# Cross-graph quasi-cliques máximas de tamanho pelo menos 3, com no máximo um vínculo (duas arrestas no grafo direcionado) que falta em cada licitação e nenhuma restrição sobre o número de arrestas envolvendo cada vértice.
max_noise=$(sort -uk 3,3 $TMP/edges | wc -l)
multidupehack -c '0 1' -e "$max_noise $max_noise 2" -s 3 --pn $TMP/edges | ./canonical-output.awk | tee $TMP/quasi # multidupehack demora ~34s com os dois tipos de vínculo e 0,6 para o peso telefônico

# Cross-graph cliques máximas com exatamente 2 CNPJs não incluídas na saída anterior.
multidupehack -c '0 1' -s 2 -S 2 $TMP/edges | ./canonical-output.awk | awk '
FILENAME == ARGV[1] {
    gsub(/#[0-9.]*/, "")
    quasi[$0] }

FILENAME == ARGV[2] {
    valid = 1
    super_pattern = gensub(/,/, ",.*", 1, $1) ".* .*" gensub(/,/, ",.*", "g", $2)
    for (q in quasi)
        if (q ~ super_pattern)
            valid = 0
    if (valid)
        print }' $TMP/quasi - # multidupehack demora ~32s com os dois tipos de vínculo e 0,6 para o peso telefônico
