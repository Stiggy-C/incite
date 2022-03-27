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