export PGPASSWORD=$TEST_DATABASE_PASSWORD
export TEST_DATABASE_URL="postgres://$TEST_DATABASE_USER:$TEST_DATABASE_PASSWORD@$TEST_DATABASE_HOST:$TEST_DATABASE_PORT/$TEST_DATABASE_NAME"

createdb -U $TEST_DATABASE_USER -h $TEST_DATABASE_HOST -p $TEST_DATABASE_PORT $TEST_DATABASE_NAME || exit 1
psql -U $TEST_DATABASE_USER -h $TEST_DATABASE_HOST -p $TEST_DATABASE_PORT -d $TEST_DATABASE_NAME -f tests/db-schema.sql || exit 1
psql -U $TEST_DATABASE_USER -h $TEST_DATABASE_HOST -p $TEST_DATABASE_PORT -d $TEST_DATABASE_NAME -f tests/data.sql || exit 1
cargo test -- --nocapture
dropdb -U $TEST_DATABASE_USER -h $TEST_DATABASE_HOST -p $TEST_DATABASE_PORT $TEST_DATABASE_NAME || exit 1
