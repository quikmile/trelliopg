from setuptools import setup

setup(name='trelliopg',
      version='0.0.13',
      author='Abhishek Verma, Nirmal Singh',
      author_email='ashuverma1989@gmail.com, nirmal.singh.cer08@itbhu.ac.in',
      url='https://github.com/technomaniac/trelliopg',
      description='Postgres database connector and micro-orm',
      packages=['trelliopg'],
      install_requires=['asyncpg'])
