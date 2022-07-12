FROM mrcide/poppunk:bacpop-20

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install \ 
    && poetry config virtualenvs.create true 

COPY beebop /beebop
EXPOSE 8080
CMD poetry run waitress-serve 'beebop.app:app'
