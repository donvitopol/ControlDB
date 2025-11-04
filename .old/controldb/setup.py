from setuptools import setup
# Ref: https://pythonhosted.org/an_example_pypi_project/setuptools.html
#  bdist_wininst     create an executable installer for MS Windows
setup(
    name = 'controldb',
    version = '1.6',
    description = 'Easy management of a MS Access DataBase.',
    author = 'donvitopol',
    author_email = 'don_vito_pol@hotmail.com',
    py_modules = ['ControlDBGet', 'ControlDB', 'Table', 'MyDB', 'MultipleDataBases'],
    status = "Development"
)