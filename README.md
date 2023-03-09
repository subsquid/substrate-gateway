<div align="center">

# Substrate Gateway

### Graphql api over data from [substrate-ingest](https://github.com/subsquid/squid-sdk/tree/master/substrate/substrate-ingest)

</div>

# Configuration
`substrate-gateway` supports configuration via command line arguments.
```
OPTIONS:
    --acala-support
        Acala's EVM pallet support

    --contracts-support
        Сontracts pallet support

    --database-max-connections <DATABASE_MAX_CONNECTIONS>
        Maximum number of connections supported by pool [default: 1]

    --database-statement-timeout <DATABASE_STATEMENT_TIMEOUT>
        Abort any statement that takes more than the specified amount of ms [default: 0]

    --database-type <DATABASE_TYPE>
        Database type [default: postgres] [possible values: postgres, cockroach]

    --database-url <DATABASE_URL>
        Database connection string

    --evm-support
        EVM pallet support

    --gear-support
        Gear pallet support

    --scan-max-value <SCAN_MAX_VALUE>
        Query engine will be upper limited by this amount of blocks [default: 100000]

    --scan-start-value <SCAN_START_VALUE>
        Number of blocks to start scanning a database [default: 100]

    -h, --help
        Print help information
```

# Logging
Logging can be enabled as follows: `RUST_LOG=substrate_gateway=info`

To examine sql queries it's required to specify additional log rules: `RUST_LOG=substrate_gateway=info,sqlx=debug`

# Development
[git-cliff](https://github.com/orhun/git-cliff) is used as a changelog generator
```bash
git cliff --prepend CHANGELOG.md -u --tag <latest-tag>
```
