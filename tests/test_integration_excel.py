# -*- coding: UTF-8 -*-
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pypel.processes as proc
import os
import datetime as dt


@pytest.fixture
def ep():
    return proc.Process()


def mockreturn_extract(self, dummy):
    d = {"PRoJET": ["nom du projet"],
         "ENTREPrISE": ["MINISTERE DE L'ECONOMIE DES FINANCES ET DE LA RELANCE"],
         "TYPE ENTREPRISE": ["PME"],
         "SIRéN": [110020013],
         "SIRéT": [11002001300097],
         "DéPARTEMENT": ["75"],
         "VILlE": ["Paris"],
         "MONTANT INVESTISSEMENT": [10000],
         "DESCRIPTION_PROJET": ["belle description de ce projet"],
         "RETOMBéES PROJET": ["belle retombées du projet"],
         "DATE_DEPOT_PROJET": [dt.datetime.strptime("2019/05/12", "%Y/%m/%d")],
         "DATE_DEBUT_INSTRUCTION": [dt.datetime.strptime("2019/05/12", "%Y/%m/%d")],
         "MONTANT_PARTICIPATION_ETAT": [5000],
         "DATE_DECISION": [dt.datetime.strptime("2019/05/12", "%Y/%m/%d")],
         "CODE_COMMUNE_ETABLISSEMENT": ['75112'],
         "STATUT": ["decidé"]
         }
    df = pd.DataFrame(d)
    return df


def test_integration_excel(ep, params, monkeypatch):
    monkeypatch.setattr(proc.Process, "extract", mockreturn_extract)
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
    df = ep.extract("")
    obtained = ep.transform(df,
                            column_replace={"é": "e", " ": "_"},
                            date_format="%Y-%m-%d",
                            date_columns=["DATE_DEPOT_PROJET", "DATE_DEBUT_INSTRUCTION", "DATE_DECISION"])
    expected_df = pd.DataFrame(expected)
    assert_frame_equal(expected_df, obtained, check_names=True)


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
                                          [9, 9, 9, 9, 9]], columns=["A", "B", "C", "D", "E"])
    df = ep.extract(path)
    obtained_default = ep.transform(df)
    assert_frame_equal(expected_default, obtained_default)


def test_init_dataframe_excel_skiprows(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
    expected_skip_5 = pd.DataFrame(data=[[6, 6, 6, 6, 6],
                                         [7, 7, 7, 7, 7],
                                         [8, 8, 8, 8, 8],
                                         [9, 9, 9, 9, 9]],
                                   columns=["0", "1", "2", "3", "4"])

    df = ep.extract(path, skiprows=5, header=None)
    obtained_skip_5 = ep.transform(df)
    assert_frame_equal(expected_skip_5, obtained_skip_5)


def test_init_dataframe_excel_sheetname(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
    expected_sheetname = pd.DataFrame(data=[[9]], columns=["A"])
    df = ep.extract(path, sheet_name="TEST")
    obtained_sheetname = ep.transform(df)
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
                                      [9, 9, 9, 9, 9]], columns=["A", "B", "C", "D", "E"])
    df = ep.extract(path_csv)
    obtained_csv = ep.transform(df)
    assert_frame_equal(expected_csv, obtained_csv)


def test_init_bad_filename(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_bad_filename$.csv")
    expected_badfilename = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["A", "B", "C", "D", "E"])
    df = ep.extract(path)
    obtained_bad_filename = ep.transform(df)
    assert_frame_equal(expected_badfilename, obtained_bad_filename)


def test_init_bad_filename_excel(ep, params, monkeypatch):
    path = os.path.join(os.getcwd(), "tests", "fake_data", "test_bad_filename$.xlsx")
    expected_badfilename = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                              [2, 2, 2, 2, 2],
                                              [3, 3, 3, 3, 3],
                                              [4, 4, 4, 4, 4],
                                              [5, 5, 5, 5, 5],
                                              [6, 6, 6, 6, 6],
                                              [7, 7, 7, 7, 7],
                                              [8, 8, 8, 8, 8],
                                              [9, 9, 9, 9, 9]], columns=["A", "B", "C", "D", "E"])
    df = ep.extract(path)
    obtained_bad_filename = ep.transform(df)
    assert_frame_equal(expected_badfilename, obtained_bad_filename)
