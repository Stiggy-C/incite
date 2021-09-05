create table if not exists route(
    id UUID primary key,
    xml varchar not null,
    yaml varchar not null,
    version int,
    createdBy varchar not null,
    createdDateTime timestamp not null,
    updatedBy varchar,
    updatedDateTime timestamp
) with "TEMPLATE=incite_default";