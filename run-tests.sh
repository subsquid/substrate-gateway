createdb -U root -h localhost -p 30000 test-substrate-archive || exit 1
psql -U root -h localhost -p 30000 -f tests/db-schema.sql -d test-substrate-archive -q || exit 1
TEST_DATABASE_URL="postgres://root@localhost:30000/test-substrate-archive" cargo test
dropdb -U root -h localhost -p 30000 test-substrate-archive || exit 1
