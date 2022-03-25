here::i_am("dev/import_indices_to_es.R")
library(here)
library(elastic)
library(tidyverse)

# Initialize ----
# - From inside the Docker container `host = elasticsearch`, i.e. the name set 
#   within the Docker-compose file. 
# - From outside the container it's possible to connect to Elastic at 
#   `localhost:9200` (i.e. `connect()` using all defaults)


x <- elastic::connect(
  host = "es01", 
  transport_schema = "https", 
  user = "elastic", 
  pwd = "qqpp11--",
  cainfo = "/usr/share/elasticsearch/config/certs/ca/ca.crt"
)
x$ping()


# Load indices ----
# Load all indices 
es_indices <- list.files(
  here("fakelake"), 
  recursive = TRUE, 
  pattern = "index_", 
  full.names = TRUE, 
)
walk(es_indices, ~ docs_bulk(conn = x, ..1))
