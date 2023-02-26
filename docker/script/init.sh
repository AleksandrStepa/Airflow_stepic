#!/bin/bash

# Создание БД
sleep 10
airflow upgradedb
sleep 10

mv usr/local/airflow/dag.py usr/local/airflow/dags
mv usr/local/airflow.cfg usr/local/airflow

airflow users create \          
	--username admin \
	--firstname admin \
	--lastname admin \
	--role Admin \
	--email admin@example.org \
	-p admin

# Запуск шедулера и вебсервера
airflow scheduler & airflow webserver
