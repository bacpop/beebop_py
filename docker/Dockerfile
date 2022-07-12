FROM mrcide/poppunk:bacpop-20
WORKDIR /

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install

COPY beebop /beebop
EXPOSE 8080

WORKDIR /beebop-run
RUN mkdir -p /beebop-run/storage

CMD poetry run waitress-serve 'beebop.app:app'
