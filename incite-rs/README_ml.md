# Incite RESTful APIs

## Machine Learning

### Create (the definition of) a classification task
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications
```

#### HTTP headers
| HTTP Header  | Value            |
|--------------|------------------|
| Content-Type | application/json |

#### Example
TODO

### Retrieve (the definition of) a classification task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications/{{id}}
```

### Update (the definition of) a classification task
```text
PATCH    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications/{{id}}
```

#### HTTP headers
| HTTP Header  | Value |
|--------------|-------|
| Content-Type | application/merge-patch+json   |

#### Example
```text
TODO
```

### Delete (the definition of) a classification task
```text
DELETE    {{httpProtocol}}://{{host}}:{{port}}/rs/classifications/{{id}}
```

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

#### HTTP headers
| HTTP Header  | Value            |
|--------------|------------------|
| Content-Type | application/json |

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
            "@type": "IgniteSink",
            "id": "c0bbbc24-ffe4-4437-b08d-41017a9ffb83",
            "saveMode": "Append",
            "rdbmsDatabase": {
                "driverClass": "org.apache.ignite.IgniteJdbcThinDriver",
                "url": "jdbc:ignite:thin://localhost:10800?lazy=true&queryEngine=h2",
                "username": "ignite",
                "password": "ignite"
            },
            "table": "guest_cluster_analysis",
            "primaryKeyColumns": "id"
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
    }
}'
```

### Retrieve (the definition of) a clustering task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}
```

### Update (the definition of) a clustering task
```text
PATCH    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}
```

#### HTTP headers
| HTTP Header  | Value |
|--------------|-------|
| Content-Type | application/merge-patch+json   |

#### Example
```text
TODO
```

### Delete (the definition of) a clustering task
```text
DELETE    {{httpProtocol}}://{{host}}:{{port}}/rs/cluster-analyses/{{id}}
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

### Retrieve (the definition of) a recommendation task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}
```

### Update (the definition of) a recommendation task
```text
PATCH    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}
```

#### HTTP headers
| HTTP Header  | Value |
|--------------|-------|
| Content-Type | application/merge-patch+json   |

#### Example
```text
TODO
```

### Delete (the definition of) a recommendation task
```text
DELETE    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}
```

### Build a model for a defined recommendation task
```text
GET    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}/model
```

### Run a defined recommendation task with the latest model
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/recommendations/{{id}}/predict
```