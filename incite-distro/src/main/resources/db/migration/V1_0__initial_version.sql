create table if not exists route(
    id UUID primary key,
    xml varchar,
    yaml varchar,
    type varchar(12) not null,
    version int,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
) with "TEMPLATE=incite_default";