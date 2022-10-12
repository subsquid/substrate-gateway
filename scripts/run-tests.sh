cd ./tests/
source .env

export PGPASSWORD=$DB_PASS
export TEST_DATABASE_URL="postgres://$DB_USER:$DB_PASS@$DB_HOST:$DB_PORT/$DB_NAME"

docker-compose up -d || exit 1

# sleep until postgres is ready to acceps commands
until psql -U $DB_USER -h $DB_HOST -p $DB_PORT -c '\q' &>/dev/null; do
  sleep 1
done

psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -f db-schema.sql || exit 1
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -f data.sql || exit 1
cargo test -- --nocapture
docker-compose down || exit 1
