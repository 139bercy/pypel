import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pypel
import os


@pytest.fixture
def ep_strip_dpt():
    return pypel.BaseProcess("default")


def mockreturn_initdf(self):
    test = {"Projet": ["nom du projet"],
            "ENtReprISE": ["MINISTERE DE L'ECONOMIE DES FINANCES ET DE LA RELANCE"],
            "TYPE eNTReprISE": ["PME"],
            "SIreN": [110020013],
            "SIREt": [11002001300097],
            "Département": [" 75 "],
            "Ville": ["Paris"],
            "MonTANT INVESTISSEMENT": [10000],
            "DESCRIPTION pROJET": ["belle description de ce projet"],
            "RETOMBEES PROJET": ["belle retombées du projet"],
            "MONTANT PARTICIPATION éTAT": [5000],
            "CODE_COMMUNE_ETABLISSEMENT": ['75112'],
            "Statut": ["decidé"]
            }
    test_df = pd.DataFrame(test)
    return test_df


def mockreturn_path_to_data():
    return os.path.abspath("tests/fake_data")


def test_integration_no_date(ep_strip_dpt, params, monkeypatch):
    monkeypatch.setattr(pypel.BaseProcess, "extract", mockreturn_initdf)
    expected = {"PROJET": ["nom du projet"],
                "ENTREPRISE": ["MINISTERE DE L'ECONOMIE DES FINANCES ET DE LA RELANCE"],
                "TYPE_ENTREPRISE": ["PME"],
                "SIREN": [110020013],
                "SIRET": [11002001300097],
                "DEPARTEMENT": ["75"],
                "VILLE": ["Paris"],
                "MONTANT_INVESTISSEMENT": [10000],
                "DESCRIPTION_PROJET": ["belle description de ce projet"],
                "RETOMBEES_PROJET": ["belle retombées du projet"],
                "MONTANT_PARTICIPATION_ETAT": [5000],
                "CODE_COMMUNE_ETABLISSEMENT": ['75112'],
                "STATUT": ["decidé"]
                }
    df = ep_strip_dpt.extract()
    obtained = ep_strip_dpt.transform(df,
                                      column_replace={"é": "e", " ": "_"},
                                      strip=["DEPARTEMENT"])
    expected_df = pd.DataFrame(expected)
    assert_frame_equal(expected_df, obtained, check_names=True)
