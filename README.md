### Airflow Dynamic Mysql To S3

A dynamic MySQL to S3 was created to migrate data from the Jobgram.org site to S3, which can also be used for other processes.

Aifrlow (Init and Run):

```bash
docker compose up airflow-init
```

```bash
docker compose up -d
```

S3 (minio):

```bash
docker run -p 9000:9000 -p 9090:9090 -e "MINIO_ROOT_USER=admin" -e "MINIO_ROOT_PASSWORD=password" minio/minio:latest
```
