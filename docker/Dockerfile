FROM mrcide/poppunk:bacpop-43
WORKDIR /

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install

COPY . /beebop
WORKDIR /beebop
EXPOSE 5000

CMD poetry run waitress-serve --port=5000 'beebop.app:app'
