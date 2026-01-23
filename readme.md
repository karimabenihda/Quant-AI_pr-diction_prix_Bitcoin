# 📈 Quant-AI – Plateforme de Prédiction du Prix du Bitcoin (T+10)

## 🧠 Contexte du projet

Dans le secteur de la **Fintech** et des **hedge funds**, la capacité à traiter des flux de données massifs en quasi temps réel afin d’anticiper les mouvements de marché constitue un **avantage concurrentiel majeur**.

La société **Quant-AI**, spécialisée dans l’analyse algorithmique, souhaite valider un **prototype industriel** capable de dépasser la simple analyse statique pour devenir une **plateforme prédictive automatisée, scalable et sécurisée**.

---

## ❓ Problématique

Les traders et outils d’aide à la décision de Quant-AI ne disposent pas actuellement :

* d’une **source de données centralisée**,
* de données **nettoyées et enrichies**,
* ni d’une **vision prédictive court terme (T+10 minutes)**.

### Contraintes clés

* **Latence** : ingestion et traitement rapide depuis l’API Binance
* **Scalabilité** : gestion d’un historique croissant via le calcul distribué
* **Fiabilité** : automatisation stricte des pipelines
* **Sécurité** : protection des prédictions (propriété intellectuelle)

---

## 🎯 Objectifs techniques

### Data Engineering

* Architecture **Medallion** (Bronze / Silver)
* Pipelines ETL automatisés

### Calcul distribué

* Nettoyage et feature engineering à grande échelle avec **PySpark**

### Data Science / Machine Learning

* Modèle de **régression sur séries temporelles**
* Prédiction du prix du Bitcoin à **T+10 minutes**

### DevOps & Orchestration

* **Apache Airflow** pour l’orchestration
* **Docker & Docker Compose** pour la conteneurisation

### Backend & Sécurité

* API REST **FastAPI**
* Sécurisation via **JWT**

---

## 🏗️ Description globale du projet

Le projet consiste à développer une **plateforme end-to-end** qui :

1. Récupère les données de marché (OHLC, volumes) depuis l’API **Binance**
2. Transforme ces données en **indicateurs techniques** via PySpark
3. Entraîne un modèle de Machine Learning
4. Prédit le **prix de clôture du Bitcoin à T+10 minutes**
5. Expose les prédictions via une **API REST sécurisée**

### Logique de montée en qualité des données

* **Zone Bronze** : données brutes issues de Binance
* **Zone Silver** : données nettoyées, typées et enrichies
* **Service Layer** : modèle ML + API

---

## 👥 Organisation de l’équipe (3 personnes)

### 🧱 Data Engineer – Lead Pipeline

* Ingestion des données Binance
* Stockage Bronze / Silver
* Traitement distribué PySpark
* Orchestration via Airflow
* Construction de la cible (prix T+10)

### 🤖 Machine Learning Engineer – Lead Modèle

* Feature engineering séries temporelles
* Entraînement & évaluation (RMSE, MAE)
* Sérialisation & monitoring (MLflow)

### 🔐 Backend & Security Engineer – Lead API

* Développement API REST (FastAPI)
* Authentification JWT
* Exposition des prédictions
* Endpoints analytiques SQL
* Logs & sécurité

---

## 📡 Source de données – API Binance

Les données proviennent des **klines (OHLC)** de Binance :

```json
[
  [
    1499040000000,
    "0.01634790",
    "0.80000000",
    "0.01575800",
    "0.01577100",
    "148976.11427815",
    1499644799999,
    "2434.19055334",
    308,
    "1756.87402397",
    "28.46694368",
    "0"
  ]
]
```

### Champs exploités

* Open, High, Low, Close
* Volume
* Nombre de trades
* Taker buy volumes

---

## 🧠 Problématique IA – Régression du prix futur

### Objectif du modèle

Prédire le **prix de clôture du Bitcoin à T+10 minutes** :

```
y = close_price(t + 10)
```

### Construction de la cible (PySpark)

Décalage de la colonne `close` de 10 minutes :

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

window = Window.orderBy("open_time")

df = df.withColumn(
    "close_t_plus_10",
    F.lead("close", 10).over(window)
)
```

---

## 🧩 Feature Engineering (PySpark)

Toutes les features sont calculées **en distribué**.

### 📉 1. Variations de prix (Returns)

Variation relative du prix de clôture :

```
return(t) = (close(t) - close(t-1)) / close(t-1)
```

* Utilisation de `lag()` et `Window`

---

### 📊 2. Moyennes mobiles

* **MA_5** : moyenne des 5 dernières minutes
* **MA_10** : moyenne des 10 dernières minutes

Formules :

```
MA_5(t)  = moyenne(close(t-4) → close(t))
MA_10(t) = moyenne(close(t-9) → close(t))
```

* Utilisation de `avg()`, `over()` et `rowsBetween()`

---

### 🔄 3. Intensité de trading

Ratio de pression acheteuse :

```
taker_ratio(t) = taker_buy_base_volume / volume
```

Mesure la proportion de BTC achetée par les **takers**.

---

## 🚀 API & Sécurité

### Endpoint principal

**POST /predict**

* Entrée : features calculées
* Sortie :

```json
{ "predicted_price_t_plus_10": 43215.75 }
```

### Sécurité

* Authentification **JWT**
* Accès contrôlé aux prédictions (IP & tokens)

---

## 🔁 Orchestration & Déploiement

* **Airflow DAG** :

  * Ingestion API Binance
  * Stockage Bronze
  * Transformation Silver
  * Entraînement modèle
  * Déploiement API

* **Docker Compose** :

  * Spark
  * Airflow
  * PostgreSQL
  * FastAPI

---

## ✅ Conclusion

Ce projet démontre la mise en œuvre d’une **architecture Fintech industrielle**, combinant :

* Data Engineering distribué
* Machine Learning sur séries temporelles
* Orchestration robuste
* API sécurisée

Un cas d’usage complet et crédible pour des rôles **Data Engineer, ML Engineer ou Quant Developer**.
