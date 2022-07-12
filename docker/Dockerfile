FROM mrcide/poppunk:bacpop-20
WORKDIR /

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install

COPY beebop /beebop
COPY scripts/download_db /usr/bin/beebop_download_db
EXPOSE 8080

WORKDIR /beebop-run
CMD poetry run waitress-serve 'beebop.app:app'
