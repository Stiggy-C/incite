# Incite RESTful APIs

## Enterprise integration

### Create route
```text
POST    {{httpProtocol}}://{{host}}:{{port}}/rs/routes
```

#### HTTP headers
| HTTP Header  | Value      |
|--------------|------------|
| Content-Type | text/plain |

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

### Update route
```text
PATCH    {{httpProtocol}}://{{host}}:{{port}}/rs/routes/{{id}}
```

#### HTTP headers
| HTTP Header  | Value |
|--------------|-------|
| Content-Type | application/merge-patch+json   |

#### Example
```text
TODO
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