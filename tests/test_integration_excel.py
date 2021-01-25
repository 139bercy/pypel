# -*- coding: UTF-8 -*-
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pypel
import os


@pytest.fixture
def ep():
    return pypel.ExcelProcess("default", backup=False)


def mockreturn_initdf(self, file_path: str, sheet_name: str = 0, skiprows=None):
    test = {"Projet": ["nom du projet"],
            "ENtReprISE": ["MINISTERE DE L'ECONOMIE DES FINANCES ET DE LA RELANCE"],
            "TYPE eNTReprISE": ["PME"],
            "SIreN": [110020013],
            "SIREt": [11002001300097],
            "Département": ["75"],
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


def mockreturn_test_init_df(self, file_path, **kwargs):
    return self.init_dataframe(file_path, **kwargs)


def prep_tests(m):
    m.setattr(pypel.ExcelProcess, "init_dataframe", mockreturn_initdf)


def prep_test_init_dataframe(m):
    m.setattr(pypel.ExcelProcess, "get_es_actions", mockreturn_test_init_df)


def test_integration_excel(ep, params, monkeypatch):
    prep_tests(monkeypatch)
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
    ep.get_es_actions("file", params["Processes"]["DummyExcelProcess"], params)  # file is a placeholder
    expected_df = pd.DataFrame(expected)
    assert_frame_equal(expected_df, ep.df, check_names=True)


def test_init_dataframe_excel(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
    expected_default = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
    prep_test_init_dataframe(monkeypatch)
    obtained_default = ep.get_es_actions(path)
    assert_frame_equal(expected_default, obtained_default)
    del obtained_default, expected_default


def test_init_dataframe_excel_skiprows(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
    prep_test_init_dataframe(monkeypatch)
    expected_skip_5 = pd.DataFrame(data=[[7, 7, 7, 7, 7],
                                         [8, 8, 8, 8, 8],
                                         [9, 9, 9, 9, 9]], columns=[6, "6.1", "6.2", "6.3", "6.4"])
    obtained_skip_5 = ep.get_es_actions(path, skiprows=5)
    assert_frame_equal(expected_skip_5, obtained_skip_5)
    del expected_skip_5, obtained_skip_5


def test_init_dataframe_excel_sheetname(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
    prep_test_init_dataframe(monkeypatch)
    expected_sheetname = pd.DataFrame(data=[[9]], columns=["a"])
    obtained_sheetname = ep.get_es_actions(path, sheet_name="TEST")
    assert_frame_equal(expected_sheetname, obtained_sheetname)


def test_init_dataframe_excel_csv(ep, params, monkeypatch):
    path_csv = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv")
    expected_csv = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
    prep_test_init_dataframe(monkeypatch)
    obtained_csv = ep.get_es_actions(path_csv)
    assert_frame_equal(expected_csv, obtained_csv)


def test_init_bad_filename(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_bad_filename$.csv")
    prep_test_init_dataframe(monkeypatch)
    expected_badfilename = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
    obtained_bad_filename = ep.get_es_actions(path)
    assert_frame_equal(expected_badfilename, obtained_bad_filename)


def test_init_bad_filename_excel(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_bad_filename$.xlsx")
    prep_test_init_dataframe(monkeypatch)
    expected_badfilename = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
    obtained_bad_filename = ep.get_es_actions(path)
    assert_frame_equal(expected_badfilename, obtained_bad_filename)
