# Incite RESTful APIs

## Data (streaming) aggregation

### Create (the definition of) an aggregate task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates
```

#### HTTP headers
| HTTP Header  | Value            |
|--------------|------------------|
| Content-Type | application/json |

:warning: As of April 16, 2022, if source.fields is empty, all fields read from source will be included. Otherwise, only
those declared in source.fields will be included.

#### Example
```text
curl --location --request POST 'http://localhost:8080/rs/aggregates' \
--header 'Content-Type: application/json' \
--data-raw '{
    "description": "Sample",
    "joins": [
        {
            "leftColumn": "membership_number",
            "rightColumn": "membership_number",
            "rightIndex": 1,
            "type": "INNER"
        }
    ],
    "sinks": [
        {
            "@type": "StreamingWrapper",
            "id": "c0bbbc24-ffe4-4437-b08d-41017a9ffb83",
            "outputMode": "Append",
            "streamingWrite": true,
            "triggerType": "ProcessingTime",
            "triggerInterval": 10000,
            "nonStreamingSink": {
                "@type": "IgniteSink",
                "id": "c0bbbc24-ffe4-4437-b08d-41017a9ffb83",
                "saveMode": "Append",
                "rdbmsDatabase": {
                    "driverClass": "org.apache.ignite.IgniteJdbcThinDriver",
                    "url": "jdbc:ignite:thin://localhost:10800?lazy=true&queryEngine=h2",
                    "username": "ignite",
                    "password": "ignite"
                },
                "table": "guest_transactions",
                "primaryKeyColumns": "transaction_id"
            }
        }
    ],
    "sources": [
        {
            "@type": "KafkaSource",
            "fields": [
                {
                    "function": "#field as transaction_id",
                    "name": "data.id"
                },
                {
                    "function": "#field as membership_number",
                    "name": "data.membership_number"
                },
                {
                    "function": "#field as sku",
                    "name": "data.sku"
                },
                {
                    "function": "#field as price",
                    "name": "data.price"
                },
                {
                    "function": "to_timestamp(#field, '\''yyyy-MM-dd HH:mm:ss.SSS'\'') as purchase_date_time",
                    "name": "data.created_date_time"
                }
            ],
            "watermark": {
                "eventTimeColumn": "purchase_date_time",
                "delayThreshold": "5 minutes"
            },
            "streamingRead": true,
            "kafkaCluster": {
                "servers": "PLAINTEXT://localhost:63720"
            },
            "startingOffset": "earliest",
            "topic": "transactions_1"
        },
        {
            "@type": "JdbcSource",
            "rdbmsDatabase": {
                "driverClass": "org.postgresql.Driver",
                "url": "jdbc:postgresql://localhost:63718/test?loggerLevel=OFF",
                "username": "test_user",
                "password": "test_password"
            },
            "query": "select g.id as guest_id, g.membership_number, g.created_date_time, g.last_login_date_time from guest g order by last_login_date_time desc"
        }
    ]
}'
```

### Retrieve (the definition of) an aggregate task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates/{{aggregateId}}
```

### Update (the definition of) an aggregate task
```text
PATCH    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates/{{aggregateId}}
```

#### HTTP headers
| HTTP Header  | Value            |
|--------------|------------------|
| Content-Type | application/merge-patch+json |

#### Example
```text
curl --location -g --request PATCH 'http://localhost:8080/rs/aggregates/{{aggregateId}}' \
--header 'Content-Type: application/merge-patch+json' \
--data-raw '{
    "description": "Sample updated",
    "sources": [
        {
            "@type": "KafkaSource",
            "fields": [
                {
                    "function": "#field as transaction_id",
                    "name": "data.id"
                },
                {
                    "function": "#field as membership_number",
                    "name": "data.membership_number"
                },
                {
                    "function": "#field as sku",
                    "name": "data.sku"
                },
                {
                    "function": "#field as price",
                    "name": "data.price"
                },
                {
                    "function": "to_timestamp(#field, '\''yyyy-MM-dd HH:mm:ss.SSS'\'') as purchase_date_time",
                    "name": "data.created_date_time"
                }
            ],
            "watermark": {
                "eventTimeColumn": "purchase_date_time",
                "delayThreshold": "5 minutes"
            },
            "streamingRead": true,
            "kafkaCluster": {
                "servers": "PLAINTEXT://44.15.70.13:9092"
            },
            "startingOffset": "earliest",
            "topic": "transactions"
        },
        {
            "@type": "JdbcSource",
            "rdbmsDatabase": {
                "driverClass": "org.postgresql.Driver",
                "url": "jdbc:postgresql://44.15.70.13:5432/test?loggerLevel=OFF",
                "username": "test_user",
                "password": "test_password"
            },
            "query": "select g.id as guest_id, g.membership_number, g.created_date_time, g.last_login_date_time from guest g order by last_login_date_time desc"
        }
    ]
}'
```

### Delete (the definition of) an aggregate task
```text
DELETE    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates/{{aggregateId}}
```

### Start a defined aggregate task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates/{{aggregateId}}/aggregate
```