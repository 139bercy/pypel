import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pypel
import os


@pytest.fixture
def ep_strip_dpt():
    return pypel.BaseProcess("default")


def mockreturn_initdf(self, file_path: str, sheet_name: str = 0, skiprows=None):
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
            "DATE DEPOT PROJET": ["2019/05/12"],
            "DATE DEBUT INSTRUCTION": ["2019/05/12"],
            "MONTANT PARTICIPATION éTAT": [5000],
            "DATE DECISION": ["2019/05/12"],
            "CODE_COMMUNE_ETABLISSEMENT": ['75112'],
            "Statut": ["decidé"]
            }
    test_df = pd.DataFrame(test)
    return test_df


def mockreturn_path_to_data():
    return os.path.abspath("tests/fake_data")


def test_integration(ep_strip_dpt, params, monkeypatch):
    monkeypatch.setattr(pypel.BaseProcess, "init_dataframe", mockreturn_initdf)
    monkeypatch.setattr(pypel.BaseProcess, "get_path_to_data", mockreturn_path_to_data)
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
                "DATE_DEPOT_PROJET": ["2019-05-12"],
                "DATE_DEBUT_INSTRUCTION": ["2019-05-12"],
                "MONTANT_PARTICIPATION_ETAT": [5000],
                "DATE_DECISION": ["2019-05-12"],
                "CODE_COMMUNE_ETABLISSEMENT": ['75112'],
                "STATUT": ["decidé"]
                }
    ep_strip_dpt.get_es_actions("file", params["Processes"]["DummyProcess"], params)  # file is a placeholder
    expected_df = pd.DataFrame(expected)
    assert_frame_equal(expected_df, ep_strip_dpt.df, check_names=True)
