FROM mrcide/poppunk:bacpop-20
WORKDIR /

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install

COPY . /beebop
WORKDIR /beebop
EXPOSE 8080

CMD poetry run waitress-serve 'beebop.app:app'
