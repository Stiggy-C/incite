create table if not exists pipeline(
    id UUID primary key,
    description varchar,
    joins varchar,
    fixedDelay bigint,
    lastRunDateTime timestamp,
    -- sinks varchar,
    -- sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
) with "template=default";

create table if not exists pipelines_sinks(
    pipeline_id uuid primary key,
    sink_id uuid,
) with "template=default";

create table if not exists pipelines_sources(
    pipeline_id uuid primary key,
    source_id uuid,
) with "template=default";

create table if not exists classification(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    -- sinks varchar,
    -- sources varchar,
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
    -- sinks varchar,
    -- sources varchar,
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

create table if not exists frequent_pattern_mining(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    -- sinks varchar,
    -- sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
)  with "template=default";

create table if not exists frequent_pattern_mining_model(
    id UUID,
    frequent_pattern_mining_id UUID,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, frequent_pattern_mining_id)
) with "template=default,affinity_key=frequent_pattern_mining_id";

create table if not exists recommendation(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    -- sinks varchar,
    -- sources varchar,
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

create table if not exists regression(
    id UUID primary key,
    algorithm varchar,
    description varchar,
    fixedDelay bigint,
    joins varchar,
    lastRunDateTime timestamp,
    -- sinks varchar,
    -- sources varchar,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
)  with "template=default";

create table if not exists regression_model(
    id UUID,
    regression_id UUID,
    root_mean_squared_error double,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    primary key(id, regression_id)
) with "template=default,affinity_key=regression_id";

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

create table if not exists source(
    id UUID primary key,
    fields varchar,
    watermark varchar,
    sub_class varchar not null,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
) with "template=default";

create table if not exists file_source(
    id UUID primary key,
    streaming_read boolean,
    path varchar,
    format varchar,
    max_files_per_trigger smallint,
    latest_first boolean,
    max_file_age varchar,
    clean_source varchar,
    source_archive_directory varchar
) with "template=default";

create table if not exists jdbc_source(
    id UUID primary key,
    rdbms_database varchar,
    query varchar
) with "template=default";

create table if not exists kafka_source(
    id UUID primary key,
    streaming_read boolean,
    kafka_cluster varchar,
    starting_offset varchar,
    topic varchar
) with "template=default";

create table if not exists sink(
    id UUID primary key,
    sub_class varchar not null,
    created_by varchar(320) not null,
    created_date_time timestamp not null,
    updated_by varchar(320),
    updated_date_time timestamp
)  with "template=default";

create table if not exists file_sink(
    id UUID primary key,
    output_mode varchar,
    streaming_write boolean,
    trigger_type varchar,
    trigger_interval bigint,
    format varchar,
    path varchar
)  with "template=default";

create table if not exists jdbc_sink(
    id UUID primary key,
    createTableColumnTypes varchar,
    createTableOptions varchar,
    rdbms_database varchar,
    table_name varchar
)   with "template=default";

create table if not exists ignite_sink(
    id UUID primary key,
    createTableColumnTypes varchar,
    createTableOptions varchar,
    rdbms_database varchar,
    table_name varchar,
    primary_key_columns varchar
)   with "template=default";

create table if not exists kafka_sink(
    id UUID primary key,
    output_mode varchar,
    streaming_write boolean,
    trigger_type varchar,
    trigger_interval bigint,
    kafka_cluster varchar,
    topic varchar
)   with "template=default";

create table if not exists streaming_wrapper_sink(
    id UUID primary key,
    output_mode varchar,
    streaming_write boolean,
    trigger_type varchar,
    trigger_interval bigint,
    non_streaming_sink_id UUID not null
)   with "template=default";