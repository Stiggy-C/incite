# Incite RESTful APIs

## Data (streaming) aggregation

### Create (the definition of) an aggregate task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/aggregates
```
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

## Enterprise integration

### Create route
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/routes
```

Upon successful creation (i.e. API returns HTTP 201), the route will be started automatically.

#### Example
```text
curl --location --request POST 'http://localhost:8080/rs/routes' \
--header 'Content-Type: text/plain' \
--data-raw 'route:
    from: "ignite-messaging:sample_event_0?ignite='\''#{ignite}'\''"
    steps:
        - idempotent-consumer:
            expression: 
                header: "CamelIgniteMessagingUUID"
            message-id-repository-ref: "jdbcOrphanLockAwareIdempotentRepository"
        - unmarshal:
            json:
                library: Jackson
        - choice:
            when: 
                - expression:
                    spel: "#{request.body.type == '\''guest_complain'\''}"
                    steps:
                        - set-body:
                            simple: "insert into sample_event_0(id, guestId, content, eventType, isComplain, createdDateTime) values (UUID(), '\''${body.guestId}'\'', '\''${body.content}'\'', '\''${body.eventType}'\'', true, '\''${body.createdDateTime}'\'')"
            otherwise:
                steps:
                    - set-body:
                        simple: "insert into sample_event_0(id, guestId, content, eventType, isComplain, createdDateTime) values (UUID(), '\''${body.guestId}'\'', '\''${body.content}'\'', '\''${body.eventType}'\'', false, '\''${body.createdDateTime}'\'')"
        - to: "jdbc:igniteJdbcThinDataSource"'
```

### Delete route
```text
DELETE    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}
```

### Resume route
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}/resume
```

### Start route
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}/start
```

### Stop route
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}/stop
```

### Suspend route
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}/suspend
```

### Update route
```text
PATCH    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}
```

## Machine Learning
### Create (the definition of) a classification task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications
```

#### Example
TODO

### Build a model for a defined classification task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications/{{id}}/model
```

#### Example
TODO

### Run a defined classification task with the latest model
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications/{{id}}/predict
```

#### Example
TODO

### Create (the definition of) a clustering task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses
```

#### Example
TODO

### Build a model for a defined clustering task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}/model
```

#### Example
TODO

### Run a defined clustering task with the latest model
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}/predict
```

#### Example
TODO