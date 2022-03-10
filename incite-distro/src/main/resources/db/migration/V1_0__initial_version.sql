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

create table if not exists clustering(
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
)  with "template=default";

create table if not exists clustering_model(
    id UUID,
    cluster_analysis_id UUID,
    silhouette real,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, cluster_analysis_id)
) with "template=default,affinity_key=cluster_analysis_id";

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