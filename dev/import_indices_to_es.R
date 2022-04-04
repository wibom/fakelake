here::i_am("dev/import_indices_to_es.R")
library(here)
library(glue)
library(jsonlite)
library(yaml)
library(elastic)
library(tidyverse)

get_idx_settings <- function(abspath_bulkidx) {
  
  #' Args:
  #' `abspath_bulkidx`: absolute path to Elasticsearch formatted bulk index file
  #' 
  #' Retrieve index settings from `lake_settings.yaml`
  #' 

  lakesettings <- here("dev/lake_settings.yaml") %>% read_yaml()

  # get index-name from bulk indexfile (1st line)
  indexname <- 
    read_lines(abspath_bulkidx, n_max = 1) %>% 
    jsonlite::fromJSON() %>% 
    unlist() %>% 
    unname()
  
  # get yaml dataset identifier from bulk indexfile filepath
  main_id <- str_extract(abspath_bulkidx, "(?<=3_gold\\/)\\w+(?=\\/)")
  dataset_id <- 
    str_remove(abspath_bulkidx, glue("^.+\\/3_gold/{main_id}/")) %>% 
    dirname()

  # get yaml index settings for current index
  # get all index settings from the `index_id` specified in the bulk index file
  if (dataset_id == ".") {
    # No dataset id specified (e.g. NSHDS)
    es_indices_id <- 
      lakesettings[[main_id]]$es_indices %>% 
      map_lgl(~ ..1$index_id == indexname) %>% 
      .[.] %>% 
      names()
    idx_settings <- lakesettings[[main_id]]$es_indices[[es_indices_id]]
  } else {
    es_indices_id <- 
      lakesettings[[main_id]]$datasets[[dataset_id]]$es_indices %>% 
      map_lgl(~ ..1$index_id == indexname) %>% 
      .[.] %>% 
      names()      
    idx_settings <- 
      lakesettings[[main_id]]$datasets[[dataset_id]]$es_indices[[es_indices_id]]
  }
  
  return(idx_settings)
}

# Initialize ----
# - From inside the Docker container `host = es01`, i.e. the name set 
#   within the Docker-compose file. 
# - From outside the container it's possible to connect to Elastic at 
#   `localhost:9200` (i.e. `connect()` using all defaults)
#     - though, this would require access to the 
#       '/usr/share/elasticsearch/config/certs' created by Elasticsearch
#

x <- elastic::connect(
  host = "es01", 
  transport_schema = "https", 
  user = "elastic", 
  pwd = "qqpp11--",
  cainfo = "/usr/share/elasticsearch/config/certs/ca/ca.crt"
)
x$ping()


# Delete all indices ----
indices_all <- cat_indices(x, parse = TRUE, verbose = TRUE)$index
walk(indices_all, ~ index_delete(conn = x, index = ..1))


# Load indices ----
# Load all indices 
es_indices <- list.files(
  here("fakelake"), 
  recursive = TRUE, 
  pattern = "index_", 
  full.names = TRUE, 
)
#walk(es_indices, ~ docs_bulk(conn = x, ..1))

for (es_index in es_indices) {
  
  yaml_idx_settings <- get_idx_settings(es_index)
  
  # Create custom mapping (when defined in lake-yaml)
  if (is.null(yaml_idx_settings$index_mapping_template)) {
    
    warn_txt <- glue::glue(
      "There is no mapping template to use for index:
       {es_index}
      >>>  Falling back to Elasticsearch default dynamic mapping! <<<
      
      
      "
    )
    warning(warn_txt)
    
  } else {
    
    # Create index and add mapping (if it does not already exist)
    indices_all <- cat_indices(x, parse = TRUE, verbose = TRUE)$index
    if(!any(indices_all == yaml_idx_settings$index_id)) {
      dynamic_template <- read_file(yaml_idx_settings$index_mapping_template)
      index_create(x, index = yaml_idx_settings$index_id)
      mapping_create(
        x, 
        index = yaml_idx_settings$index_id, 
        body = dynamic_template
      )
    }    
    
  }
 
  # Load index
  docs_bulk(conn = x, es_index)
  print(es_index)
}

