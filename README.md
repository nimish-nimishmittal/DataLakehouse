# DataLakehouse

sudo docker compose build

sudo docker compose up -d

combining the above two...

sudo docker compose up -d --build

localhost:9000 - minio

minioadmin
minioadmin123

localhost:8080 - airflow
admin
admin

postgres access - sudo docker exec -it postgres_lakehouse psql -U lakehouse_user -d lakehouse_db

postgres table data show - \dt

then use normal sql for displaying data

tbd...

uiux - for user and admin
(auth for user and admin - separately by someone else)
admin - allow to put metadata (induce in postgres, column metadata)
pipelines - for ppt and scaned pdf
pipelines - for integration of iot data
pipelines - for integration of db data - mysql, sqlite, postgresql, sqlserver

