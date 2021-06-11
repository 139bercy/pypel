#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re

import pandas as pd
from elasticsearch import helpers


def fr_str_to_float(column):
    """
    returns a column or df_ with . instead of ,
    useful for going from french-notation floats to english-notation floats
    """
    column = column.replace(",", ".", regex=True)
    column = column.apply(float)
    return column


def parse_date(df_):
    """
    reformats df_ with DATE columns as datetimes of format year-month-day
    """
    for column in df_.columns:
        if "DATE" in str(column):
            df_[column] = pd.to_datetime(df_[column], errors='coerce', dayfirst=True)
            df_[column] = df_[column].dt.strftime('%Y-%m-%d')
            df_[column] = df_[column].apply(pd.to_datetime)
            df_[column] = df_[column].replace({pd.np.nan: None})
    return df_


def parse_date_eng_format(df_):
    """
    reformats df_ with DATE columns as datetimes of format year-month-day
    """
    for column in df_.columns:
        if "DATE" in str(column):
            df_[column] = pd.to_datetime(df_[column], errors='coerce', dayfirst=False)
            df_[column] = df_[column].dt.strftime('%Y-%m-%d')
            df_[column] = df_[column].apply(pd.to_datetime)
    return df_


def fill_all_na(df_):
    """
    fills missing values of df_ with None
    """
    df_ = df_.where(pd.notnull(df_), None)
    return df_


def format_std(df):
    """
    returns df_ with no accents in column names, column names all UPPERCASE and _ separated
    also drops column with all na values
    """
    df.dropna(axis='columns', how='all', inplace=True)
    df = df.replace("İ", "é", regex=True).replace("´", "ô", regex=True).replace("¨", "è", regex=True)
    df = (df.replace("à¨", "è", regex=True).replace("à®", "î", regex=True).replace("à©", "é", regex=True)
          .replace("à´", "ô", regex=True).replace("Â(?=°)", "", regex=True))
    df.columns = (df.columns.str.strip().str.replace('é', 'e').str.replace('î', 'i')
                  .str.replace('ô', 'o').str.replace('è', 'e').str.replace('ê', 'e')
                  .str.replace('ç', 'c').str.replace('à', 'a').str.upper().str.replace(' ', '_')
                  .str.replace('(', '').str.replace(')', '').str.replace('-', '_'))
    df.columns = df.columns.str.replace(".1", "_TEXTUEL", regex=True)
    return df


def push_to_elastic(df_, es, index_pattern):
    """Pushes a dataframe through an elastic connection into a specific index pattern, all of which passed as arguments

    Arguments :
    df_ : the dataframe to upload into elastic
    es  : the elasticsearch connection to upload into
    index_pattern : the index pattern to upload into
    """
    data_dict = df_.to_dict(orient="index")
    act = [
        {
            "_index": index_pattern,
            "_type": "_doc",
            "_source": value
        }
        for value in data_dict.values()
    ]
    helpers.bulk(es, act)


def push_to_elastic_from_dic(dic, es, index_pattern):
    """Pushes a dic through an elastic connection into a specific index pattern, all of which passed as arguments

    Arguments :
    df_ : the dataframe to upload into elastic
    es  : the elasticsearch connection to upload into
    index_pattern : the index pattern to upload into
    """
    act = [
        {
            "_index": index_pattern,
            "_type": "_doc",
            "_source": value
        }
        for value in dic.values()
    ]
    helpers.bulk(es, act)


def create_dict_to_replace(df_):
    """fonction qui crée un dictionnaire utilisable avec .replace depuis un dataframe"""
    dictionnary = {}
    for i in df_.index:  # pour chaque ligne du dataframe
        key = df_.iloc[i, 0]  # on stocke la valeur de la colonne 0 comme clé
        value = df_.iloc[i, 1]  # on stocke la valeur de l'autre colonne comme valeur
        dictionnary[key] = value  # ajoute la paire clé:valeur
    return dictionnary


def add_mapping(df_, map_):
    """Adds a column into df_ whose value match another column in df_ according to the correspondances in map_

    Arguments :
    df_ : the dataframe to add the column to
    map_ : a dataframe with only 2 columns, the first one being a column in df_, and the second one representing
           another column whose values must match the first column's
    """
    new_col = map_.columns[1]
    df_[new_col] = df_[map_.columns[0]].replace(create_dict_to_replace(map_))
    return df_


def unzero_days_in_date(serie):
    unzeroed = []
    for i in serie:
        if i[-1] == '0':
            unzeroed.append((i[:-1] + "1"))
        else:
            unzeroed.append(i)
    return unzeroed


def format_date_fso(df):
    for col in df.columns:
        if "Date" in str(col):
            df[col] = [df[col][x][0:4] + "/" + df[col][x][4:6] + "/" + df[col][x][7:10] for x in range(0, len(df))]
    return df


def matching_date_aecp(string):
    return re.findall(r"(?<=AE|CP)\w* [\d]{4}", string)[0]


def matching_date_cpspe(string):
    return re.findall(r"(?<=tré|uré)\w* [\d]{4}", string)[0]


def exporter2(df_input, compteur=0):
    indexed_monthly = {}  # on crée un dictionnaire vide
    for i in df_input.index:  # i va itérer sur chaque ligne du tableau
        for k in range(14, 62):  # pour chaque ligne du tableau, on découpe les colonnes au mois
            compteur += 1
            monthly = {}    # on crée un sous dictionnaire vide pour chaque colonne a découper
            value = df_input.iloc[i, k]  # on récupère la valeur
            value = float(value)
            key = "Montant"
            if "AE" in df_input.columns[k]:  # s'il y a AE dans le nom de colonne de départ
                monthly["AECP"] = "AE"  # alors on ajoute "AECP" comme nom de colonne, et on lui met comme valeur "AE"
            else:
                monthly["AECP"] = "CP"  # alors on ajoute "AECP" comme nom de colonne, et on lui met comme valeur "DP"
            monthly[key] = value
            value = df_input.columns[k]  # on élimine les deux premiers charactères du nom originel de la colonne, car il contient soit AE soit DP
            if "CP_" in value:
                value = matching_date_cpspe(value)
            else:
                value = matching_date_aecp(value)
            value = value.split()[1] + "/" + value.split()[0]
            value = (value.replace("Janvier", "01/01")
                     .replace("Février", "02/01")
                     .replace("Mars", "03/01")
                     .replace("Avril", "04/01")
                     .replace("Mai", "05/01")
                     .replace("Juin", "06/01")
                     .replace("Juillet", "07/01")
                     .replace("Août", "08/01")
                     .replace("Septembre", "09/01")
                     .replace("Octobre", "10/01")
                     .replace("Novembre", "11/01")
                     .replace("Décembre", "12/01"))
            key = "DATE"
            monthly[key] = value
            for j in range(0, 14):    # ensuite on ajoute les 14 premières colonnes telles quelles au dictionnaire
                key = df_input.columns[j]
                value = df_input.iloc[i, j]
                monthly[key] = value
            monthly = columns_formatter_dict(monthly)
            monthly = dict_parse_date(monthly)
            indexed_monthly[compteur] = monthly # on ajoute au dictionnaire indexed_monthly le dictionnaire monthly sous la clé compteur
# ce qui est conforme aux dictionnaires à passer à kibana : un dictionnaire de dictionnaires ou la clé dans le premier dictionnaire correspond à l'index des données du 2e dictionnaire (le dictionnaire imbriqué (nested))
    return indexed_monthly, compteur


def dict_parse_date(dictionnary):
    for val in dictionnary.values():  # pour chaque paire clé:valeur, on regarde si la clé contient DATE
        if "DATE" in val:
            value = pd.to_datetime(val, errors='coerce')  # si c'est le cas, on convertit la valeur en datetime
            dictionnary[val] = value
    return dictionnary


def columns_formatter_dict(dictionnary):
    for i in dictionnary.keys():  # pour chaque paire clé:valeur, on formatte la clé
        dictionnary[i.strip().replace('é', 'e')
                    .replace('î', 'i')
                    .replace('ô', 'o')
                    .replace('è', 'e')
                    .replace('ê', 'e')
                    .replace('ç', 'c')
                    .replace('à', 'a')
                    .upper()
                    .replace(' ', '_')
                    .replace('(', '')
                    .replace(')', '')
                    .replace('-', '_')] = dictionnary.pop(i)
    return dictionnary


def add_mapping_bud40(df, briques_num, briques_name):  # fonction qui prend en entrée un dataframe et deux dictionnaires
    df_tmp = df
    for i in range(0, len(df_tmp.index)):
        df_tmp.iloc[i, 2] = df["Référentiel de programmation"][i][5:]
    df_tmp["BRIQUES_NUMERO"] = df["Référentiel de programmation"]  # crée une colonne briques numéro avec les valeurs de la colonne "reférenciel de programmation" càd les activités
    df_tmp["BRIQUES_NOM"] = df["Référentiel de programmation"]  # crée une colonne briques_nom avec les même valeurs que ci dessus
    df_tmp["BRIQUES_NUMERO"] = df_tmp["BRIQUES_NUMERO"].replace(briques_num)  # remplace les valeurs clés dans le premier dictionnaire (briques_num) par les valeurs associées de ce même dictionnaire
    df_tmp["BRIQUES_NOM"] = df_tmp["BRIQUES_NOM"].replace(briques_name)  # remplace les valeurs clés dans le deuxième dictionnaire (le dictionnaire briques_names) par les valeurs associées de ce même dictionnaire
    return df_tmp


def formatbud(df):
    df_returned = df
    df_returned.iloc[:, 14:62] = df_returned.where((pd.notnull(df_returned.iloc[:, 17:62])), 0.0)
    df_returned = df_returned.where((pd.notnull(df_returned)), "NON RENSEIGNE")
    df_returned[df_returned.columns[1]] = df_returned[df_returned.columns[1]].apply(str)    # on convertit la colonne en STRING pour éviter les erreurs lors du chargement dans elastic (la valeur a pour type numpy.int64 dans le dataframe, type intraduisible en JSON)
    df_returned[df_returned.columns[0]] = df_returned[df_returned.columns[0]].apply(str)
    df_returned[df_returned.columns[4]] = df_returned[df_returned.columns[4]].apply(str)
    df_returned[df_returned.columns[2]] = df_returned[df_returned.columns[2]].apply(str)
    df_returned[df_returned.columns[9]] = df_returned[df_returned.columns[9]].apply(str)    # idem, mais pour éviter une conversion automatique au format 'date' par elastic...
    df_returned[df_returned.columns[10]] = df_returned[df_returned.columns[10]].apply(str)  # idem car elastic essaie d'importer ce champ en tant que float
    for i in range(14, 62):
        df_returned[df_returned.columns[i]] = df_returned[df_returned.columns[i]].apply(float)
    for i in df_returned.index:  # l'inf bud40 cumulant les montants de mois en mois on doit effecteur des soustractions pour récupérer les valeurs mensuelles
        for j in range(61, 51, -1):
            df_returned.iloc[i, j] = df_returned.iloc[i, j] - df_returned.iloc[i, j-1]
        for j in range(49, 39, -1):   # 3e arg : pas, pour avoir la décroissance on met un pas de -1
            df_returned.iloc[i, j] = df_returned.iloc[i, j] - df_returned.iloc[i, j-1]
        for k in range(37, 27, -1):
            df_returned.iloc[i, k] = df_returned.iloc[i, k] - df_returned.iloc[i, k-1]
        for l in range(25, 15, -1):
            df_returned.iloc[i, l] = df_returned.iloc[i, l] - df_returned.iloc[i, l-1]
    df_returned["Domaine fonctionnel"] = [x.replace("-", "") for x in df_returned["Domaine fonctionnel"]]  # on élimine les - pour éviter que kibana interprète ce champ comme une date...
    return df_returned


def document_importer(df):
    df = fill_all_na(df)
    df = format_std(df)
    df = parse_date(df)
    df = parse_date(df)
    return df.to_dict(orient="index")


def treat_new_files(list_of_files, elastic):
    for new_file in list_of_files:
        df = pd.read_excel(new_file, encoding="utf-8")
        dic = document_importer(df)
        push_to_elastic_from_dic(dic, elastic, str(new_file))
        print("Ficher ", new_file, " loadded into elastic!")
        os.remove(new_file)


def document_importer_frais_rpz(df, e, ip):
    df = df.where((pd.notnull(df)), None)
    df = df.applymap(str)
    df["prix_ttc"] = df["prix_ttc"].apply(float)
    df = format_std(df)
    df = parse_date(df)
    push_to_elastic(df, e, ip)
