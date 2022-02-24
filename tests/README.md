### conftest.py
'conftest.py' defines pytest fixtures, which are pieces of code that can be reused across tests. 
'app', 'client' and 'runner' are used to set up a test client for testing the application, without running a live server.

### test_*.py
'test_*.py' files define the tests. 
A request to the client can be done via 'client.get()' or 'client.post()'. The returned data can be retrieved with 'response.data'. More information is available on [Testing Flask Applications](https://flask.palletsprojects.com/en/2.0.x/testing/)