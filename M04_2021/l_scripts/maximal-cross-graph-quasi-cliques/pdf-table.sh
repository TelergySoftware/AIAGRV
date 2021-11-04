#!/bin/sh

# Em Debian e derivadas, os pacotes texlive-latex-recommended e texlive-extra-utils devem ser instalados

NB_OF_CNPJS_PER_ROW=2
NB_OF_PROCUREMENTS_PER_ROW=4

if [ -z "$2" ]
then
    printf "Uso: $0 padrões out.pdf [quantos_cnpjs_por_linha] [quantas_licitações_por_linha]

Cada linha do arquivo \"padrões\" contém dois campos separados por
' ': CNPJs e licitações, ambos separados por ','.  Cada CNPJ e
licitação pode ser seguido de '#' e do ruído coberto, como
multidupehack informa com a opção --pn.  Sem a informação, se
considere que o ruído é 0.

Os números padrões de CNPJs e licitações por linha são $NB_OF_CNPJS_PER_ROW e $NB_OF_PROCUREMENTS_PER_ROW.
"
    # Esse formato pode ser diferente, modificando o script abaixo.
    exit
fi

if [ -n "$3" ]
then
    NB_OF_CNPJS_PER_ROW=$3
    if [ -n "$4" ]
    then
	NB_OF_PROCUREMENTS_PER_ROW=$4
    fi
fi

TMP=`mktemp -dt pdf-table.sh.XXXXXX`
trap "rm -r $TMP 2>/dev/null" 0

awk -v nb_of_cnpjs_per_row=$NB_OF_CNPJS_PER_ROW -v nb_of_procurements_per_row=$NB_OF_PROCUREMENTS_PER_ROW '
function print_cnpj(c) {
    split(c, a, "#")
    printf "\\textcolor{black!" 100 - 100 * a[2] / (m - 1) / n "!}{" substr(a[1], 1, 2) "." substr(a[1], 3, 3) "." substr(a[1], 6, 3) "/" substr(a[1], 9, 4) "-" substr(a[1], 13) "} " }

function print_procurement(p) {
    split(p, a, "#")
    printf "\\textcolor{black!" 100 - 100 * a[2] / (m - 1) / m "!}{" a[1] "} " }

BEGIN {
    print "\\documentclass{article}\\usepackage[paperheight=\\maxdimen]{geometry}\n\\usepackage{booktabs,xcolor}\n\\begin{document}\n\\pagestyle{empty}\n\\begin{tabular}{ccr}\n\\toprule\n\\textbf{CNPJs} & \\textbf{Licitações} & \\textbf{Nível de alarme}\\\\\n\\midrule" }

{
    print "\\midrule"
    m = split($1, cnpj, ",")
    n = split($2, procurement, ",")
    for (i = 1; i <= nb_of_cnpjs_per_row; ++i)
	print_cnpj(cnpj[i])
    printf "& "
    for (j = 1; j <= nb_of_procurements_per_row; ++j)
	print_procurement(procurement[j])
    print "& " $3 "\\\\"
    while (i <= m || j <= n) {
	for (k = 0; i <= m && k != nb_of_cnpjs_per_row; ++k)
            print_cnpj(cnpj[i++])
	printf "& "
	for (k = 0; j <= n && k != nb_of_procurements_per_row; ++k)
            print_procurement(procurement[j++])
	print "& \\\\" } }

END {
    print "\\bottomrule\n\\end{tabular}\n\\end{document}" }' "$1" > $TMP/out.tex

out=$(realpath "$2")
cd $TMP
pdflatex out.tex
pdfcrop out.pdf "$out"
# # PNG feio e com cores erradas.
# pdfcrop out.pdf
# inkscape -o "$out" out-crop.pdf
