#!/bin/bash
# init_airflow.sh

# Initialiser la DB
airflow db init

# Créer l'utilisateur Admin
airflow users create \
    --username admin1 \
    --password admin1 \
    --firstname Admin1 \
    --lastname User1 \
    --role Admin \
    --email admin1@example.com

