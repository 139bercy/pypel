from setuptools import setup, find_packages

setup(name='pypel',
      version='0.9.0',
      description="Python Pipeline into Elasticsearch",
      packages=find_packages(exclude=('tests', 'docs', "docker", "Doc"), where="."),
      author="Quentin Dimarellis",
      author_email="quentin.dimarellis@finances.gouv.fr",
      install_requires=["pandas >= 1.2.0",
                        "elasticsearch",
                        "openpyxl >= 3.0.0",
                        "numpy >= 1.19.1",
                        "unidecode",
                        "xlrd >= 2.0.0"])
