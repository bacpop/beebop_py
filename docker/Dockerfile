FROM python:3

RUN pip install poetry
COPY  *.toml *.lock /
RUN poetry config virtualenvs.create false \
    && poetry install \ 
    && poetry config virtualenvs.create true 

COPY beebop /beebop
EXPOSE 5000
ENV FLASK_APP=/beebop/app.py 
CMD poetry run flask run