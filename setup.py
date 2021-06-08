try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(name='pypel',
<<<<<<< HEAD
      version='0.1.7',
=======
      version='0.1.6',
>>>>>>> refacto - increment patch version
      description="PYthon Pipeline into ELasticsearch",
      packages=["pypel"],
      author="Quentin Dimarellis",
      author_email="quentin.dimarellis@finances.gouv.fr",
      install_requires=["pandas >= 1.2.0",
                        "elasticsearch",
                        "openpyxl >= 3.0.0",
                        "numpy >= 1.19.1",
                        "unidecode",
                        "xlrd >= 2.0.0"])
