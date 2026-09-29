# Pipeline de données YouTube Trending (AWS)

Mon premier projet de data engineering : un pipeline qui va chercher automatiquement les vidéos tendances YouTube dans 10 pays, nettoie les données, vérifie leur qualité, puis produit des tableaux d'analyse prêts à être interrogés.

> Projet réalisé en suivant le tutoriel [Data Engineering Project | AWS S3, Lambda, Glue, Athena, Step Function](https://www.youtube.com/watch?v=yvAWbbQa8eE) de Darshil Parmar. J'ai reproduit son architecture pour apprendre les bases du data engineering sur AWS, voir la section "Mon cheminement".

## Pourquoi ce projet

Après un an de recherche dans le domaine du jeu vidéo, et malgré un profil jeune, qualifié, motivé et dynamique, le secteur bouché ne m'a pas donné l'occasion de faire mes preuves et ce malgré mon acharnement. J'ai donc exploré les différentes connexions possibles avec mes 5 années d'études dans le numérique, puisque je me suis perfectionné à chaque étape sur les domaines que je pouvais approfondir, pour finir par me rapprocher du métier de data engineer. Tout comme le Game Design (GD), le data engineering occupe une place centrale dans l'entreprise et dans la collaboration avec toutes les équipes. C'est à travers la rigueur et la qualité du travail, tout comme la clarté de la compréhension des données (comparable aux intentions de design des GD), que l'équipe peut avancer de façon productive, efficace et toujours sur la bonne voie. 

Après avoir appris les bases en SQL/Python sur OpenClassrooms, j'ai voulu me lancer, et rien de mieux qu'un projet aussi complet pour comprendre les enjeux et attentes de ce rôle !

## Ce que fait le pipeline

En clair : toutes les quelques heures, le pipeline récupère les 50 vidéos les plus populaires du moment dans 10 pays (US, GB, CA, DE, FR, IN, JP, KR, MX, RU), nettoie ces données brutes, vérifie qu'elles sont fiables, puis calcule des statistiques : quelles chaînes cartonnent, quelles catégories génèrent le plus de vues, etc.

## Architecture

![Architecture du pipeline](YouTube%20Trending%20Data%20Pipeline.png)

Le pipeline suit une architecture en 3 couches (appelée "médaillon"), une pratique standard en data engineering :

| Couche | Rôle | En clair |
|---|---|---|
| **Bronze** | Données brutes | Ce que l'API YouTube renvoie, stocké tel quel |
| **Silver** | Données nettoyées | Types corrigés, doublons supprimés, colonnes normalisées |
| **Gold** | Données prêtes à l'analyse | Statistiques agrégées par région, chaîne, catégorie |

Entre Silver et Gold, une étape de **contrôle qualité** bloque le pipeline si les données sont suspectes (trop peu de lignes, trop de valeurs manquantes, données trop anciennes) et envoie une alerte au lieu de laisser passer des données douteuses.

## Stack technique

| Composant | Outil AWS | Rôle |
|---|---|---|
| Récupération des données | Lambda | Appelle l'API YouTube Data v3 |
| Stockage | S3 | Stocke les données à chaque étape (Bronze/Silver/Gold) |
| Transformation | Glue (PySpark) | Nettoie et agrège les données |
| Contrôle qualité | Lambda | Valide les données avant l'étape finale |
| Orchestration | Step Functions | Enchaîne toutes les étapes automatiquement |
| Requêtes | Athena | Interroger les tables finales en SQL |
| Alertes | SNS | Notification en cas d'échec |
| Permissions | IAM | Droits d'accès entre les services |

## Mon cheminement

Au fur et à mesure de la vidéo, j'ai appris à utiliser chaque appli de la suite AWS, pour comprendre leur utilisation, leur fonctionnement et m'en imprégner.
J'ai pu mettre en pratique mes bases de Python et de SQL apprises précédemment afin de visualiser le fonctionnement d'un pipeline de données, et comment cette théorie s'appliquait dans la pratique.

## Difficultés rencontrées

Le plus dur n'est pas de comprendre comment procéder, en théorie, pour récupérer les données, les charger, les transformer, puis les utiliser. L'image est assez parlante, et le cheminement devient assez vite intuitif.
Ce qui pose réellement problème, c'est l'exécution et comment procéder pour chaque étape. Ce qui est complexe, c'est que, selon la façon de procéder, la méthode peut être différente, ce qui nécessite de comprendre les fondamentaux afin d'être parée à chaque situation.

## Reproduire ce projet

Prérequis : un compte AWS, une clé YouTube Data API v3 (Google Cloud Console), Python 3.9+.
Les commandes de déploiement (création des buckets S3, des jobs Glue, de la state machine Step Functions) sont détaillées dans le dossier `/scripts` et les dossiers `/lambdas`, `/glue_jobs`, `/step_functions`.

## Source

Architecture et code initial basés sur le tutoriel de [Darshil Parmar](https://github.com/darshilparmar) — [vidéo originale](https://www.youtube.com/watch?v=yvAWbbQa8eE&list=PLBJe2dFI4sgvQTNNkI3ETYJgNPR4CBpFd&index=22).
