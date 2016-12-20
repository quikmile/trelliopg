from setuptools import setup

setup(name='trelliopg',
      version='0.0.1',
      author='Abhishek Verma',
      author_email='ashuverma1989@gmail.com',
      url='https://github.com/technomaniac/trelliopg',
      description='Postgres database connector and micro-orm',
      packages=['trelliopg'],
      install_requires=['asyncpg'])
