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
    cluster_analysis_id UUID,
    silhouette double,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, cluster_analysis_id)
) with "template=default,affinity_key=cluster_analysis_id";

create table if not exists collaborative_filtering(
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

create table if not exists collaborative_filtering_model(
    id UUID,
    collaborative_filtering_id UUID,
    root_mean_squared_error double,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, collaborative_filtering_id)
) with "template=default,affinity_key=collaborative_filtering_id";

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