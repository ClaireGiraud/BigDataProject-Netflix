#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 11 12:32:11 2021

@author: clairegiraud
"""

import pymongo
import matplotlib.pyplot as plt
import numpy as np

#Connexion en local au serveur et à la base de données
client = pymongo.MongoClient()
# On travaille dans la base de données BigDataProject
db = client.BigDataProject

########################################################
#################### amazon ###########################
########################################################

# Pointeur sur la base de données dtaamazon
amazon = db.dtaamazon

print("Connexion terminée")

#################### Requêtes ##########################

### Comparaison séries VS films et par type 
# On récupère les films et les séries dans la base de données


nb_movies = amazon.count({'type' : 'Movie'})
nb_series = amazon.count({'type': 'TV Show'})


# Nombre de documentaires

count_docu = amazon.count({'listed_in': {'$regex': 'Documentary'}})
print("Il y a", count_docu, "documentaires sur Amazon Prime")

# Nombre classé dans Culture

count_culture = amazon.count({'listed_in': {'$regex': 'Culture'}})

print("Il y a ", count_culture, "films et séries classées dans la catégorie culture sur Amazon Prime")
        
# Nombre de films classés "interêt spécialisé"
                               
count_spe = amazon.count({'listed_in':{'$regex': 'Special Interest'}})

print("Il y a", count_spe, "films et séries classées dans la catégorie culture sur Amazon Prime.")


### Acteurs & Réalisateurs engagés : 
    # Dans combien de films David Attenborough a-t-il joué ? 
    
nb_real_davatt = amazon.count({'cast': {'$regex':'David Attenborough'}})

print ('David Attenborough, rédacteur scientifique, écrivain et naturaliste britannique, a joué dans', nb_real_davatt, 'films.')

### Recherche par mots clés dans les descriptions

    # Nombre de fois où le mot “global warming” est cité dans les descriptions ?
    
nb_mot_gw = amazon.count({'description': {'$regex':'global warming'}})
nb_mot_eco =  amazon.count({'description': {'$regex':'ecology'}})
nb_mot_env = amazon.count({'description': {'$regex':'environmental'}})
nb_mot_cc= amazon.count({'description': {'$regex':'climate change'}})
nb_mot_wr =  amazon.count({'description': {'$regex':'women rights'}})
nb_mot_har =  amazon.count({'description': {'$regex':'harassment'}})
nb_mot_rac = amazon.count({'description': {'$regex':'racism'}})

nb_mot_wandr = amazon.count(
    {'$and' : [
        {'description' : { '$regex' : 'women'}},
              {'description' : { '$regex' : 'rights'}}
              ]
        })

nb_mot_pandv = amazon.count(
    {'$and' : [
        {'description' : { '$regex' : 'police'}},
              {'description' : { '$regex' : 'violence'}}
              ]
        })
 
# Représentativité : 
    # Est-ce que de nombreux pays sont représentés et de manière équitable ?
    
pipeline_pays = [{"$project": { "country_split": {"$split": ["$country",", "]}}}, # Certains films sont réalisés par plusieurs pays 
            #--> on sépare avec un split les différents pays. 
            {'$unwind' : "$country_split"}, # On duplique les films qui sont réalisés dans plusieurs pays 
            #pour pouvoir compter le nombre de films par pays.
            {'$group' : {'_id': {'pays' : "$country_split" }, 'nb_film_pays' : { '$sum' : 1 }}}, # On groupe les pays et on compte
            # le nombre de film par pays 
            {'$sort' : { 'nb_film_pays' : -1 }}] # On tri dans l'odre décroissant
  

liste_pays = list(amazon.aggregate(pipeline_pays))

# Nombre de documents pour lesquels le champ pays est non existant :  
nb_fieldempty=amazon.count({'country' : { '$exists' : False}})


# Nombre de films par réalisateur (voir les réalisateurs qui ont fait peu de films) : 
    
pipeline_real = [{"$project": { "director_split": {"$split": ["$director",", "]}}}, 
            {'$unwind' : "$director_split"}, 
            {'$group' : {'_id': {'realisateur' : "$director_split" }, 'nb_film_real' : { '$sum' : 1 }}},
            {'$sort' : { 'nb_film_real' : -1 }}] 
         

liste_real = list(amazon.aggregate(pipeline_real))
    
#################### Graphiques ##########################


# N°1 : Contenu par type - Film vs Séries

couleur = ['turquoise','darkcyan']
fig, ax = plt.subplots()
ax.axis("equal")
ax.pie([nb_movies, nb_series],
        labels = ["Films", "Séries"],
        autopct='%1.1f%%', 
        colors = couleur)

plt.title("Répartition du contenu Amazone Prime par type")
plt.show()



# N°2 : Films et séries par types (documentaires, indépendants, etc.)

nb_doc_tot = 9668

nb_autre = nb_doc_tot - (count_culture+ count_spe + count_docu)

couleur2 = ['turquoise','darkcyan', 'skyblue', 'cadetblue']
fig, ax = plt.subplots()
ax.axis("equal")
ax.pie([nb_autre, count_docu, count_spe, count_culture],
        labels = ["Autres", "Documentaires", "Culture", "Intérêt Spécialisé"],
        autopct='%1.1f%%', 
        colors = couleur2)

plt.title("Comparaison des types de films et séries présents sur Amazon Prime")
plt.show()


# N°3 : mots clés trouvés dans les descriptions

plt.bar(range(8), 
        [nb_mot_cc, nb_mot_eco, nb_mot_env, nb_mot_har, nb_mot_pandv, nb_mot_rac, nb_mot_wandr, nb_mot_gw], 
        width = 0.5, 
        color = 'skyblue')
plt.xticks([x for x in range(8)],
           ['climate change', 'ecology', 'environmental', 'harassment', 'police & violence', 'racism', 'women & rights', 'global warming'])
plt.xticks(rotation=45)
plt.title("Nombre de fois où les différents mots clés apparaissent dans les descriptions sur Amazon Prime")
plt.legend(['occurence'])
plt.show()



# N°4 : Contenu par pays

pays_li = []
nb_contenu = []
for elem in liste_pays :
    pays_li.append((elem["_id"])["pays"])
    nb_contenu.append((elem)["nb_film_pays"])
    
    
    
# Quelques précisions et statistiques 
# nombre de pays ayant réalisés un seul film présent sur amazon
nb_pays_1film = nb_contenu.count(1)

mediane_pays = np.median(nb_contenu)
moyenne_pays = np.mean(nb_contenu)


# 4a : Histogramme 
plt.hist(nb_contenu, 
         bins = 200, 
         rwidth = 0.8, 
         color = "steelblue")
plt.ylabel('Occurence')
plt.xlabel('Nombre de films par pays')
plt.title('Distribution du nombre de films par pays sur Amazon Prime')
plt.show()
    
# 4b : 10 pays les plus représentés
plt.bar(range(10), 
        nb_contenu[0:10], 
        width = 0.5)
plt.xticks([x for x in range(10)],
           pays_li[0:10])
plt.xticks(rotation=90)
plt.xlabel('Pays')
plt.title("Les 10 pays les plus représentés dans le contenu Amazon Prime")
plt.legend(['Nombre de films'])
plt.show()


# N°5 : Contenu par réalisateur
real_li = []
nb_realisation = []
for elem in liste_real :
    real_li.append((elem["_id"])["realisateur"])
    nb_realisation.append((elem)["nb_film_real"])
    
    
nb_real_1film = nb_realisation.count(1)

mediane_real = np.median(nb_realisation)
moyenne_real = np.mean(nb_realisation)
    
# 5a : Histogramme

plt.hist(nb_realisation, 
         bins = 20, 
         rwidth = 0.5, 
         color = 'darkblue')
plt.ylabel('Occurence')
plt.xlabel('Nombre de films par réalisateur')
plt.title('Distribution du nombre de films par réalisateur sur Amazone Prime')
plt.show()

# 5b : 
plt.bar(range(10), 
        nb_realisation[0:10], 
        width = 0.5)
plt.xticks([x for x in range(10)],
           real_li[0:10])
plt.xticks(rotation=90)
plt.xlabel('Réalisateurs')
plt.title("Les 10 réalisateurs les plus représentés dans le contenu Amazon Prime")
plt.legend(['Nombre de films'])
plt.show()








