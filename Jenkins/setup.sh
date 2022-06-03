#!/bin/bash

#seta para terminar execução ao ocorrer algum erro
set -e

echo "Gerando ambiente python3"

python3 -m pip install --upgrade pip
pip3 install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org -r ./Jenkins/requirements.txt

