# prestart.sh

echo "Waiting for Elasticsearch connection"

while ! nc -z db 9200; do
    sleep 0.1
done

echo "Elasticsearch Cluster started"

exec "$@"