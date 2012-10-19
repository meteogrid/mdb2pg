from setuptools import setup

setup(
    name='mdb2pg',
    version='1.0',
    description='Dumps a MDB file into a postgresql database',
    author='Alberto Valverde Gonzalez',
    author_email='alberto@meteogrid.com',
    url='http://github.com/meteogrid/mdb2pg',
    py_modules=['mdb2pg'],
    entry_points={'console_scripts': ['mdb2pg = mdb2pg:main']}
)
