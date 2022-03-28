# PREDICT LAKE

Mimic lake structure.

The idea is that metadata and Elasticsearch indices should be easy to modify and regenerate. 

Many settings are easily modified from `dev/lake_settings.yaml`. Others are hardcoded to `dev/spinnup_lake.R` and `dev/utilities/utilities.R`.

## 1. Spinn up lake:

1. Open the Rstudio-project: `PREDICT_LAKE.Rproj`
2. Run: `dev/spinnup_lake.R`

## 2. Start R-session and connect to Elasticsearch  
This can for example be done using a Docker-container:

1) Modify  the `elk/docker-compose-predict.yaml` & run `compose up`  
    * modify the `r / volumes:` value to map local path

2) Open R-studio (user: 'rstudio')

3) Modify permissions on the Elastic certificates (necessary to connect to the Elastic instance)
    * This can for example be done from the R-studio terminal, by:

```bash
# Navigate to the certificates-folder:  
`cd /usr/share/elasticsearch/`
# `chmod` the `certs` folder:
`rstudio@5556462f98de:/usr/share/elasticsearch$ sudo chmod -R 755 config/`
```

## 3. Import index-files to Elastic

From R-session with connection to Elastic:

1. Open the Rstudio-project: `PREDICT_LAKE.Rproj`
    * All filepaths are relative to the project file, and further handled with
      the R-package `here`
2. Run `dev/import_indices_to_es.R`


## 4. Kibana dataviews

Suggested Kibiana dataviews (a.k.a "index-patterns" in ES < 8.x), based on the index naming scheme used (see: `dev/lake_settings.yaml`)

* `*--analytes`
* `*--columns`
* `*--dataset`
* `*--events`
* `*--metabolon--events`
* etc...

Whole purpose is to change, evaluate, iterate over this structure, until initial settings that are useful for the project are esablished...
