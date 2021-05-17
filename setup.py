from distutils.core import setup

setup(name='pypel',
      version='0.1a',
      packages=["pypel", "pypel.processes"],
      author="Quentin Dimarellis",
      author_email="quentin.dimarellis@finances.gouv.fr",
      install_requires=["pandas ~= 1.2.0",
                        "elasticsearch",
                        "openpyxl >= 3.0.0",
                        "numpy ~= 1.19.1",
                        "unidecode"])
