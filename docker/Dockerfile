FROM continuumio/miniconda3

WORKDIR /

RUN . /etc/profile.d/conda.sh
#RUN conda activate beebop_py
RUN conda config --append channels conda-forge
RUN conda config --append channels bioconda
RUN conda install graph-tool
RUN conda install mandrake
RUN conda install rapidnj


# Install beebop_py
RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install

COPY . /beebop
WORKDIR /beebop
EXPOSE 5000

#CMD poetry run waitress-serve --port=5000 'beebop.app:app'
