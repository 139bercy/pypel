# PYPEL 
##### _A python pipeline into elasticsearch_
## Requirements
### Python modules
```
openpyxl
elasticsearch
pandas
numpy
unidecode
xlrd (only for .xls)
```
## Installation
 - Clone or fork the github repo, then install via setup.py : `python setup.py`

## PYPEL in a nutshell
 - What does it do ?
PYPEL (PYthon Pipeline into ELasticsearch) is a customizable ETL (Extract / Transform / Load) in python. It natively
extracts csv, xls & xlsx files and uploads them into elasticsearch.
 - What if my usecase slightly differs from that ?
The Extract/Transform/Load parts are separated, and you can modify each one independantly to fit your usecase.

## API summary
All functionalities are available through the pypel.processes.Process class:

- instantiate your extractor : `loader = pypel.loaders.Loader(es_conf, es_indice)`
where `es_conf` is elasticsearch's connection configuration (cf `conf_template.json`) and `es_indice` is the
elasticsearch indice in which to load. Loader takes a parameter specifying a time frequency matching the `strftime`
standard, defaulting to `_%m_%Y`. The current date in the passed format gets appended to the indice name.
- instantiate your Process : `process = pypel.processes.Process(loader=loader)`
- extract data : `df = process.extract(file_path)`
- transform data : `df = process.transform(df)`
- load data : `process.load(df)`
- for convenience, a wrap-up function exists that bundles all 3 operations in one : `process.process(file_path)`

The Process constructor takes optional Extractor, Transformer & Loader arguments. These must derive from their BaseClass.

4 subpackages are made accessible for customization :
 - extractors are located in `pypel.extractors`
 - transformers in `pypel.transformers`
 - loaders in `pypel.loaders`
 - processes in `pypel.processes`

For options, detailed usage and/or functionalities please refer to the docstrings.

### In-depth API usage
All custom extractors, transformers and loaders MUST be derived from their respective BaseClass, e.g `BaseTransformer`

The `Process` class exists for conveniance only. Complex use-cases can (and probably should) ignore it completely, but
the cli currently only instanciates & executes `Process`es.

Log-level is read from the environment variable `PYPEL_LOGS`, and it defaults to `INFO`.

#### Loader


The `Loader` constructor takes the following parameters :

- `es_conf`: Mandatory. The elasticsearch connection configuration.
- `indice`: the indice in which to load. Will be appended the current date.
- `time_freq`: `_%m_%Y` by default. A `strftime` format applied to the current date and appended to the indice name.
- `overwrite`: `False` by default, if True, when loading into elasticsearch,
the loader will trash and re-create the indice.

### Loading from the command line
Pypel allows generating & loading from the command line by executing `pypel/main.py`.
pypel.main takes several arguments :
 - `-f` or `--config-file` to specify what file to generate/load from
 - `-c` or `--clean` to clean indices
 - `-m` or `--mapping` to specify what mappings to use if -c is on

For a `--config-file` example, see `pypel/conf_template.json`.
Only json config files are currently supported.

For options, detailed usage and/or functionalities please refer to the documentation

## Tests
Move to the project's root directory `pypel` then run `make`. This generates html reports for easier reading, located
at: `./tests/reports/report.html` & `./tests/reports/coverage/index.html` for coverage details.

To try loading from a config file, start a local elasticsearch then run `make test_cli`. The test is successful if
the recipe executed without failure AND the indices `pypel_bulk_MM_DD`, `pypel_bulk_2_MM_DD`, `pypel_change_indice_MM_DD`
have been created and contain 18, 9 and 9 documents respectively.
Manually deleting these afterwars by running `DELETE <indice>` from the devtools console is advised.

### Testing the setup.py
Use the dockerfile : run `docker build -t pypel .` from the project's root
directory, then run `docker run pypel`

## Run with docker

### Build

La commande suivante permet de construire l'image docker:

```
docker build -f docker/Dockerfile -t pypel:dev .
```

Une image `pypel:dev` est ensuite disponible pour traiter dans un environement indépendant les données qui doivent être envoyées à Elasticsearch.

### Usage

L'image docker va lire tous les fichiers montés dans /data, et les lire. Chaque fichier doit être composé des 3 parties mentionnées dans la partie UTILISATION au dessus.

Par exemple, la commande ci-dessous monte le fichier `process.json` dans le dossier local, et éxécute son contenu:

```
docker run -v "$(pwd)/process.json:/data/process.json" pypel:dev
```
