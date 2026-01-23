#!/bin/bash
# init_airflow.sh

# Initialiser la DB
airflow db init

# Créer l'utilisateur Admin
airflow users create \
    --username admin2 \
    --password admin2 \
    --firstname Admin2 \
    --lastname User2 \
    --role Admin \
    --email admin2s@example.com

