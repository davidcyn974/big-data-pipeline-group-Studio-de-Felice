# big-data-pipeline-group-Studio-de-Felice

## Customer Data Analysis with Spark

### Introduction

Ce projet utilise Apache Spark pour analyser les données des clients d'une entreprise de commerce électronique. L'objectif principal est de comprendre le comportement des clients et de prédire le churn (taux d'attrition) à l'aide de modèles de machine learning.

### Technologies Utilisées

- **Apache Spark** : Utilisé pour le traitement des données à grande échelle et l'entraînement des modèles de machine learning.
- **PySpark** : Interface Python pour Spark, utilisée pour manipuler les données et construire les modèles.
- **MLflow** : Utilisé pour suivre les expériences de machine learning, y compris les paramètres, les métriques et les modèles.
- **Matplotlib & Seaborn** : Bibliothèques de visualisation pour créer des graphiques et des visualisations des données.

### Fonctionnalités Implémentées

1. **Préparation des Données** :
   - Les données sont chargées à partir d'un fichier CSV et transformées pour obtenir des caractéristiques pertinentes telles que le total des dépenses, le nombre de transactions, la valeur moyenne des transactions, et la récence des achats.

2. **Feature Engineering** :
   - Les caractéristiques sont assemblées en un vecteur de caractéristiques pour être utilisées dans les modèles de machine learning.

3. **Modélisation** :
   - Un modèle de régression logistique est entraîné pour prédire le churn des clients.
   - Un modèle de clustering K-means est utilisé pour segmenter les clients en différents groupes basés sur leurs comportements d'achat.

4. **Évaluation des Modèles** :
   - Les modèles sont évalués à l'aide de métriques telles que l'exactitude et le score de silhouette.
   - Les résultats sont visualisés à l'aide de matrices de confusion et de graphiques de distribution des classes.

5. **Suivi des Expériences avec MLflow** :
   - Les paramètres, les métriques et les modèles sont enregistrés dans MLflow pour faciliter le suivi et la gestion des expériences de machine learning.

### Idées Derrière le Projet

- **Analyse des Clients** : Comprendre le comportement des clients pour améliorer les stratégies de marketing et de rétention.
- **Prédiction du Churn** : Identifier les clients susceptibles de quitter l'entreprise afin de prendre des mesures proactives pour les retenir.
- **Segmentation des Clients** : Grouper les clients en segments significatifs pour personnaliser les offres et les communications.

### Comment Exécuter le Projet

1. Assurez-vous d'avoir Apache Spark et MLflow installés.
2. Pour lancer le serveur MLflow, lancez la commande suivante dans un terminal :

```bash
mlflow ui
```

Vous devriez voir une sortie similaire à celle-ci : 

```
INFO:waitress:Serving on http://127.0.0.1:5000
```


### Conclusion

Ce projet démontre comment utiliser PySpark et MLflow pour analyser les données des clients et construire des modèles de machine learning efficaces. Les insights obtenus peuvent aider à améliorer la satisfaction des clients et à réduire le churn.


