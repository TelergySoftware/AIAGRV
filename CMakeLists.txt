cmake_minimum_required(VERSION 3.18.4)


project(ICdataUtils VERSION 1.0.0 LANGUAGES CXX DESCRIPTION "ICdataUtils é um módulo para python, desenvolvido com a função de otimizar análise de grandes bancos de dados.") 

# Set the standard C++ version to C++17
set(CMAKE_CXX_STANDARD 17)

# Add the sources subdirectory
add_subdirectory(sources)