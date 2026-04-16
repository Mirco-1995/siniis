#!/usr/bin/env sh
# Questo script shell deve essere presente in tutti i progetti Python da rilasciare:
# la pipeline Jenkins considera progetti validi solo quelli che contengono questo file.
#
# È possibile utilizzare qualunque combinazione di tool di packaging: setuptools,
# distutils, pybuilder, poetry... purché al termine della procedura i pacchetti da
# rilasciare su PyPI si trovino nella cartella $PROJECT_TWINE_DIR (variabile impostata da Jenkins).
# Opzionalmente, è possibile rilasciare nella cartella $PROJECT_COVERAGE_DIR i report
# di coverage in formato XML.
#
# Lo script viene lanciato con un ambiente virtuale di build attivo, quindi qualsiasi
# operazione effettuata sull'ambiente python (pip ecc) rimane confinata all'interno di questo.

install_tool() {
  # Installazione dei tool per gestire il processo di build
  # ad esempio:
  #   pip install poetry
  # oppure, in caso di utilizzo di un semplice setup.py (setuptools/distutils),
  # installare almeno il modulo wheel per consentire l'operazione bdist_wheel:
  #   pip install wheel
  pip install -r requirements_build.txt
}

build() {
  # Creazione pacchetti distribuibili (sdist e wheel)
  # ad esempio:
  #   poetry build
  # oppure:
  #   python setup.py sdist bdist_wheel
  #
  # È possibile utilizzare la variabile $ENVIRONMENT per accedere
  # al profilo richiesto dalla pipeline.
  export POETRY_CACHE_DIR=".pypoetry"
  poetry build
  poetry pack-dist --platform "$PLATFORM" --with cli
  # Nessun test
#  if [ -z "${branch}" ]; then
#    # Eseguiamo i test solamente se non siamo su WebO (env.branch non definito)
#    poetry install --with cli,test
#    coverage run --source ./src -m unittest discover -s test && coverage xml -o "$(basename "$(pwd)")_coverage.xml"
#  fi;
}

release() {
  # Rilascio degli artefatti su cartella $PROJECT_TWINE_DIR (OBBLIGATORIO) e dei report
  # di coverage sulla cartella $PROJECT_COVERAGE_DIR
  cp dist/*.* "${PROJECT_TWINE_DIR}"
  # Nessun test
#  if [ -z "${branch}" ]; then
#    # Copiamo i file di coverage solamente se non siamo su WebO (env.branch non definito)
#    cp ./*_coverage.xml "${PROJECT_COVERAGE_DIR}"
#  fi;
}

# Lancio consecutivo delle fasi
install_tool && build && release
