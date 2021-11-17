# -*- coding: utf-8 -*-

import pymongo
from bson import json_util


#Connexion en local au serveur et à la base de données
client = pymongo.MongoClient()
# On travaille dans la base de données BigDataProject
db = client.BigDataProject

# Pointeur sur la base de données dtanetflix
netflix = db.dtanetflix

print("Connexion terminée")

########################################################
## Attention à faire une seule fois ! 

# Lecture du fichier JSON et transformation BSON
datajson = open('netflix.json').read()
data = json_util.loads(datajson)
    

# Importation des données dans la collection
if isinstance(data, list): 
    netflix.insert_many(data)   
else: 
    netflix.insert_one(data) 

print("Les données ont été importées")
########################################################



########################################################
########################################################
########################################################
########################################################


# Pointeur sur la base de données dtamazon
amazon = db.dtaamazon

print("Connexion terminée")

########################################################
## Attention à faire une seule fois ! 

# Lecture du fichier JSON et transformation BSON
datajson = open('amazon.json').read()
data = json_util.loads(datajson)
    

# Importation des données dans la collection
if isinstance(data, list): 
    amazon.insert_many(data)   
else: 
    amazon.insert_one(data) 

print("Les données ont été importées")
########################################################





