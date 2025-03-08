### Airflow Dynamic Mysql To S3

A dynamic MySQL to S3 was created to migrate data from the Jobgram.org site to S3, which can also be used for other processes.

#### Aifrlow (Init and Run):

[Please check this link for more details](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

```bash
docker compose up airflow-init
```

```bash
docker compose up -d
```

S3 (minio):

```bash
mkdir -p ~/minio/data

docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=ROOTNAME" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   quay.io/minio/minio server /data --console-address ":9001"
```
