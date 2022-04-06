# Incite RESTful APIs

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

### Run a defined classification task with the latest model
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications/{{id}}/predict
```

### Create (the definition of) a clustering task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses
```

#### Example
```text
curl --location --request POST 'http://localhost:8080/rs/cluster-analyses' \
--header 'Content-Type: application/json' \
--data-raw '{
    "joins": [
        {
            "leftColumn": "guest_id",
            "rightColumn": "id",
            "rightIndex": 1,
            "type": "INNER"
        }
    ],
    "fixedDelay": 0,
    "sinks": [
        {
            "@type": "EmbeddedIgniteSink",
            "id": "d692e214-77da-40f8-a85b-e5f41a016060",
            "saveMode": "Append",
            "primaryKeyColumns": "guest_id",
            "table": "guest_clustering_result"
        }
    ],
    "sources": [
        {
            "@type": "KafkaSource",
            "streamingRead": false,
            "fields": [
                {
                    "function": "cast(#field as double) as average_spending",
                    "name": "average_spending"
                },
                {
                    "function": "cast(#field as bigint) as guest_id",
                    "name": "guest_id"
                }
            ],
            "kafkaCluster": {
                "servers": "PLAINTEXT://localhost:54794"
            },
            "startingOffset": "earliest",
            "topic": "testBuildKMeansModelFromJointDatasets"
        },
        {
            "@type": "JdbcSource",
            "rdbmsDatabase": {
                "driverClass": "org.postgresql.Driver",
                "url": "jdbc:postgresql://localhost:54738/test?loggerLevel=OFF",
                "username": "test_user",
                "password": "test_password"
            },
            "query": "select g.id, g.age, g.sex from guest g"
        }
    ],
    "algorithm": {
        "@type": "BisectingKMeans",
        "k": 4,
        "maxIteration": 1,
        "featureColumns": [
            "average_spending",
            "age",
            "sex"
        ],
        "seed": 1
    },
    "models": []
}'
```

### Build a model for a defined clustering task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}/model
```

### Run a defined clustering task with the latest model
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}/predict
```

### Create (the definition of) a recommendation task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations
```

#### Example
TODO

### Build a model for a defined recommendation task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}/model
```

### Run a defined recommendation task with the latest model
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}/predict
```