create table if not exists route(
    id UUID primary key,
    yaml varchar,
    createdBy varchar not null,
    createdDateTime timestamp not null,
    updatedBy varchar,
    updatedDateTime timestamp
) with "TEMPLATE=default";