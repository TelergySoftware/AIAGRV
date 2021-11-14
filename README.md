# AIAGRV
Aplicação de Inteligência Artificial no Gerenciamento de Redes Veiculares: Em direção à uma Rede Autônoma

## Compilação do módulo ICdataUtils

Para compilar o módulo é necessário ter a biblioteca `boost` instalada.
Makefile também seria interessante para automatizar a compilação, mas não é necessário.

### Compilando com Makefile
Na pasta CPP, abra um terminal e digite `make` e o módulo será compilado na pasta `shared`
com o nome `ICdataUtils.so`. Para importar este módulo, utilize:
```python
from CPP.shared import ICdataUtils
```