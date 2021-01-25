# PYPEL 
##### _A python pipeline into elasticsearch_
## REQUIREMENTS
### Python modules
```
openpyxl
elasticsearch
pandas
numpy
unidecode
```
## UTILISATION
   - Importer le module : `import pypel`
   - la fonction principale, `pypel.process_into_elastic` attend 3 dictionnaires :
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
     3. le troisième contenant le mapping des indices elasticsearch concernés
   
   Cette Fonction va ouvrir tous les fichiers présents dans `path_to_data/path` pour tous les `path` présents dans
   `Processes` du second dictionnaire, les traiter, puis les charger dans elasticsearch.
## TESTS
   - move to the project's root directory `pypel` then run `pytest --cov=. tests/`
