create table if not exists aggregate(
    id UUID primary key,
    description varchar,
    joins varchar,
    fixedDelay bigint,
    lastRunDateTime timestamp,
    sinks varchar,
    sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
) with "template=default";

create table if not exists classification(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    sinks varchar,
    sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
)  with "template=default";

create table if not exists classification_model(
    id UUID,
    classification_id UUID,
    accuracy double,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, classification_id)
) with "template=default,affinity_key=classification_id";

create table if not exists clustering(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    sinks varchar,
    sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
)  with "template=default";

create table if not exists clustering_model(
    id UUID,
    clustering_id UUID,
    silhouette double,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, clustering_id)
) with "template=default,affinity_key=clustering_id";

create table if not exists recommendation(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    sinks varchar,
    sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
)  with "template=default";

create table if not exists recommendation_model(
    id UUID,
    recommendation_id UUID,
    root_mean_squared_error double,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, recommendation_id)
) with "template=default,affinity_key=recommendation_id";

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
) with "template=default";