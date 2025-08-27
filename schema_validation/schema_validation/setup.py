from setuptools import setup, find_packages

setup(
  name='schemaValidLib',
  version="2024.10.10.17.43",
  author=["riya-talreja", "amrit-agarwal"],
  url='https://databricks.com',
  author_email=['riya.talreja@hp.com', 'amrit.agarwal@hp.com'],
  description='schema validation lib wheel file for DataOS Data Products for bronze data',
  packages= find_packages(),
  install_requires=[
    'setuptools',
    'pyspark',
    'markdown'
  ]
)
