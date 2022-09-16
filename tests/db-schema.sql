CREATE TABLE metadata (
    id varchar primary key,
    spec_name varchar not null,
    spec_version integer,
    block_height integer not null,
    block_hash char(66) not null,
    hex varchar not null
);


CREATE TABLE block (
    id char(16) primary key,
    height integer not null,
    hash char(66) not null,
    parent_hash char(66) not null,
    state_root char(66) not null,
    extrinsics_root char(66) not null,
    timestamp timestamptz not null,
    spec_id text not null,
    validator varchar
);


CREATE TABLE extrinsic (
    id char(23) primary key,
    block_id char(16) not null references block on delete cascade,
    index_in_block integer not null,
    version integer not null,
    signature jsonb,
    success bool not null,
    error jsonb,
    call_id varchar(30) not null,
    fee numeric,
    tip numeric,
    hash char(66) not null,
    pos integer not null
);


CREATE TABLE call (
    id varchar(30) primary key,
    parent_id varchar(30) references call,
    block_id char(16) not null references block on delete cascade,
    extrinsic_id char(23) not null references extrinsic on delete cascade,
    success bool not null,
    error jsonb,
    origin jsonb,
    name varchar not null,
    args jsonb,
    pos integer not null
);


CREATE TABLE event (
    id char(23) primary key,
    block_id char(16) not null references block on delete cascade,
    index_in_block integer not null,
    phase varchar not null,
    extrinsic_id char(23) references extrinsic on delete cascade,
    call_id varchar(30) references call,
    name varchar not null,
    args jsonb,
    pos integer not null
);


CREATE TABLE gear_message_enqueued (
    event_id char(23) primary key references event,
    program varchar not null
);


CREATE TABLE gear_user_message_sent (
   event_id char(23) primary key references event,
   program varchar not null
);


CREATE TABLE contracts_contract_emitted (
    event_id char(23) primary key references event,
    contract varchar not null
);


CREATE TABLE frontier_evm_log (
    event_id char(23) primary key references event,
    contract char(42) not null,
    topic0 char(66),
    topic1 char(66),
    topic2 char(66),
    topic3 char(66)
);


CREATE TABLE frontier_ethereum_transaction (
    call_id varchar(30) primary key references call,
    contract char(42) not null,
    sighash varchar(10)
);


CREATE TABLE acala_evm_executed (
    event_id char(23) primary key references event,
    contract char(42) not null
);


CREATE TABLE acala_evm_executed_log (
    id char(23) primary key,
    event_id char(23) not null references event on delete cascade,
    event_contract char(42) not null,
    contract char(42) not null,
    topic0 char(66),
    topic1 char(66),
    topic2 char(66),
    topic3 char(66)
);
