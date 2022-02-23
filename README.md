![Python application CI](https://github.com/bacpop/beebop_py/actions/workflows/python-app.yml/badge.svg)

# beebop_py
## Python API for beebop

### Usage

Clone the repository with
```
git clone git@github.com:bacpop/beebop_py.git
```
You will need Python installed, as well as [Poetry](https://python-poetry.org/), which you can get on Linux with 
```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```

To install all required dependencies go into the project folder and run 
```
poetry install
```
Start the flask app with
```
export FLASK_APP=beebop/app.py
flask run
```
In a second terminal, you can now query the flask endpoints:
- /version
  ```
  curl http://127.0.0.1:5000/version
  ```
### Testing
Testing can be done by running `pytest`
