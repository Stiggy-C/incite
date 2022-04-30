# Incite RESTful APIs

## Data (streaming) aggregation

### Create (the definition of) an aggregate task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates
```

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
    "fixedDelay": 0,
    "sinks": [
        {
            "@type": "EmbeddedIgniteSink",
            "id": "b26f1ff0-0e37-4be7-a4fe-911449f3dee4",
            "saveMode": "Append",
            "primaryKeyColumns": "transaction_id",
            "table": "guest_transactions"
        }
    ],
    "sources": [
        {
            "@type": "KafkaSource",
            "watermark": {
                "eventTimeColumn": "purchase_date_time",
                "delayThreshold": "5 minutes"
            },
            "streamingRead": true,
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
            "kafkaCluster": {
                "servers": "PLAINTEXT://localhost:49346"
            },
            "startingOffset": "earliest",
            "topic": "transactions"
        },
        {
            "@type": "JdbcSource",
            "rdbmsDatabase": {
                "driverClass": "org.postgresql.Driver",
                "url": "jdbc:postgresql://localhost:49344/test?loggerLevel=OFF",
                "username": "test_user",
                "password": "test_password"
            },
            "query": "select g.id as guest_id, g.membership_number, g.created_date_time, g.last_login_date_time from guest g order by last_login_date_time desc"
        }
    ]
}'
```

### Start a defined aggregate task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates/{{aggregateId}}/aggregate
```