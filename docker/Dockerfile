FROM continuumio/miniconda3

RUN conda config --append channels conda-forge
RUN conda config --append channels bioconda

RUN conda install python=3.10
RUN conda install -c bioconda poppunk=2.6.7

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install

COPY . /beebop
WORKDIR /beebop
EXPOSE 5000

CMD poetry run waitress-serve --port=5000 'beebop.app:app'
