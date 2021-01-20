# PYPEL 
##### _A python pipeline into elasticsearch_
## REQUIREMENTS
### Applications
```
elasticsearch v7.0.0+
kibana v7.0.0+
python3
```
### Python modules
```
openpyxl
elasticsearch
pandas
numpy
unidecode
```
## MISE EN PLACE
 - Lancer le service `elasticsearch`:
 ```sudo systemctl start elasticsearch.service```
 - Lancer le service `kibana` :
 ```sudo systemctl start kibana.service```
 - Obtenir les données des périmètres à visualiser
 - Dupliquer le fichier `/Doc/config_template.json` dans `./conf/config.json`
 - Dupliquer le fichier `/Doc/parameters_template.json` dans `./conf/config.json`
 - Dans votre navigateur, accéder à Kibana via `http://localhost:5601/app/kibana`

## UTILISATION
   - la fonction principale attend 3 dictionnaires :
     1. un premier dictionnaire qui contient la configuration :
        - Dans la variable `path_to_data`, mettre le chemin du dossier contenant les données
        - Dans la variable `path_to_kibana_exports`, mettre le chemin du dossier contenant les exports
        - Dans la variable `kibana_info`, mettre l'url et le port de kibana au format : `url:port` (`localhost:5601` par défaut)
     2. un second dictionnaire qui contient la configuration des process :
        - "Processes": un dictionnaire qui associe à chaque process ses paramètres propress :
           - "path": le nom du sous-dossier vers les données propres au process
           - "indice": le nom de l'indice elastic dans lequel charger
           - "sheetname": le nom de la feuille du excel à charger (ne pas inclure le cas échéant)
        - paramètres éventuels utilisés par des sous-classes maison
     3. le troisième contenant le mapping des indices elasticsearch
## TESTS
   - move to the project's root directory `db-planr` then run `pytest --cov=. tests/`

