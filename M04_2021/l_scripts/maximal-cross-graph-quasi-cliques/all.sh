#!/bin/sh

# Apenas o vínculo societário
mkdir -p apenas-vínculo-societário/tabelas-no-relatório-M04.2/

./maximal-cross-graph-quasi-cliques.sh sócio-em-comum | \
    ./rank.sh - valores_lic.csv | \
    tee apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.pdf

head -5 apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - apenas-vínculo-societário/tabelas-no-relatório-M04.2/top-5-alarme.pdf

sort -k 5,5nr -k 4,4 -k 1 apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.tsv | \
    head -5 | \
    ./pdf-table.sh - apenas-vínculo-societário/tabelas-no-relatório-M04.2/top-5-número-de-licitações.pdf

awk '$4 == 3 { print; exit }' apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - apenas-vínculo-societário/tabelas-no-relatório-M04.2/3-CNPJs-maior-alarme.pdf

awk '$4 == 3 && /#2/ { print; exit }' apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - apenas-vínculo-societário/tabelas-no-relatório-M04.2/3-CNPJs-com-ruído-maior-alarme.pdf

awk '$4 == 4 { print; exit }' apenas-vínculo-societário/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - apenas-vínculo-societário/tabelas-no-relatório-M04.2/4-CNPJs-maior-alarme.pdf

# Vínculo societário e vínculo telefônico com grau de existência 0,6 (valor padrão)
mkdir -p vínculos-societários-e-telefônicos/tabelas-no-relatório-M04.2/
./maximal-cross-graph-quasi-cliques.sh sócio-em-comum telefone-em-comum | \
    ./rank.sh - valores_lic.csv | \
    tee vínculos-societários-e-telefônicos/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - vínculos-societários-e-telefônicos/2-cliques-and-1-noise-3+-cliques.pdf

awk '$4 == 3 { print; exit }' vínculos-societários-e-telefônicos/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - vínculos-societários-e-telefônicos/tabelas-no-relatório-M04.2//3-CNPJs-maior-alarme.pdf

awk '$1 == "03173334000155,13431621000171"' vínculos-societários-e-telefônicos/2-cliques-and-1-noise-3+-cliques.tsv | \
    ./pdf-table.sh - vínculos-societários-e-telefônicos/tabelas-no-relatório-M04.2/arresta-03173334000155,13431621000171.pdf
