here::i_am("dev/spinnup_lake.R")
library(here)
library(glue)
library(yaml)
library(tidyverse)
source(here("dev/utilities/utilities.R"))
abspath_lake_yaml <- here("dev/lake_settings.yaml")
lakesettings <- read_yaml(abspath_lake_yaml)

# input settings
dir_template_data <- lakesettings$settings$dir_template_data

# output settings
n            <- lakesettings$settings$n_tot
dir_lakedata <- lakesettings$settings$dir_lakedata
dir_bronze   <- lakesettings$settings$dir_bronze
dir_silver   <- lakesettings$settings$dir_silver
dir_gold     <- lakesettings$settings$dir_gold


# Reference ----
# Reference individuals / samples to draw from when mocking datasets
abspath_reference <- here("ref/reference_samples.tsv")
system(
  command = glue(
    "Rscript --vanilla \\
    {here('dev/utilities/generate_reference_sampleset.Rscript')} \\
    {n} \\
    {abspath_reference}"
  ), 
  show.output.on.console = FALSE
)

# NSHDS ----
# Small sample of fake NSHDS-register data
nshds_settings <- lakesettings$nshds
n_individuals              <- nshds_settings$n_individuals
filename                   <- nshds_settings$filename
meta_dataset               <- nshds_settings$meta_dataset_template
meta_columns_health        <- nshds_settings$meta_columns_health_template
meta_columns_questionnaire <- nshds_settings$meta_columns_questionnaire_template
system(
  command = glue::glue(
    "Rscript --vanilla {here('dev/utilities/generate_data_nshds.Rscript')} \\
    {abspath_lake_yaml} \\
    {abspath_reference} \\
    {dir_template_data}
    "
  ),
  show.output.on.console = FALSE
)



# Omics-data ----
# A few mock datasets
datadirs <- names(lakesettings)[str_detect(names(lakesettings), "dir_")]
for (mydir in datadirs) {
  dir_id      <- lakesettings[[mydir]]$id
  individuals <- lakesettings[[mydir]]$individuals
  datasets    <- lakesettings[[mydir]]$datasets %>% names()
  
  for (ds in datasets) {
    ds_settings  <- lakesettings[[mydir]]$datasets[[ds]]
    data_type    <- ds_settings$data_type 
    filename     <- ds_settings$filename
    meta_dataset <- ds_settings$meta_dataset_template    
    seed         <- ds_settings$seed
    samplesets   <- ds_settings$samplesets %>% names()
    es_indices   <- ds_settings$es_indices %>% names()

    # Generate omics dataset        
    n_samples_tot <- length(individuals) * length(samplesets)
    abspath_dataset <- here(dir_lakedata, dir_bronze, dir_id, ds, filename)
    absdir_template_data <- here(dir_template_data)
    generate_data_omics(abspath_dataset, n_samples_tot, data_type, seed, 
                        absdir_template_data)
    
    # Copy dataset metadata to lake (from manually created yaml-file)
    abspath_dataset_meta <- here(
      dir_lakedata, dir_silver, dir_id, ds, 
      glue("meta_file_{ds}_{data_type}.yaml")
    )
    if (!dirname(abspath_dataset_meta) %>% dir.exists()) {
      dirname(abspath_dataset_meta) %>%  dir.create(recursive = TRUE)
    }
    dataset_meta_yaml <- here(meta_dataset) %>% read_yaml()
    # enter relative file path in meta-yaml to to include in index
    dataset_meta_yaml$dataset$filename <- str_remove(abspath_dataset, here())
    write_yaml(dataset_meta_yaml, abspath_dataset_meta)


    # Generate event metadata (i.e. event = sample)
    predict_ids <- get_predictids(individuals)
    events_meta <- tibble()
    for(i in 1:length(samplesets)) {
      ss <- samplesets[i]
      dataid_idx_start <- 1 + (i-1) * length(predict_ids)
      dataid_idx_stop  <- length(predict_ids) + (i-1) * length(predict_ids)
      ss_settings <- ds_settings$samplesets[[ss]]
      vip_age <- ss_settings$vip_age
      add_freeze_thaw <- ss_settings$add_freeze_thaw
      
      ss_meta <- get_sample_metadata(
        abspath_reference, 
        predict_ids, 
        vip_age, 
        add_freeze_thaw, 
        abspath_dataset, 
        relpath_dataset = str_remove(abspath_dataset, here()),
        data_type, 
        dataid_idx = seq(from = dataid_idx_start, to = dataid_idx_stop, by = 1)
      )
      events_meta <- bind_rows(events_meta, ss_meta)
    }
    abspath_events_meta <- file.path(
      dir_lakedata, dir_silver, dir_id, ds, 
      glue("meta_event_{ds}_{data_type}.tsv")
    )
    write_tsv(x = events_meta, abspath_events_meta)    
    
    # Generate analyte metadata (typically same as columns; if wide format)
    if (str_to_lower(data_type) == "metabolon") {
      analytes_meta <- get_metabolon_chemical_annotation(abspath_dataset)
    } else if (str_to_lower(data_type) == "olink") {
      analytes_meta <- get_olink_proteins(abspath_dataset)
    }
    # enter relative file path in meta dataframe to include in index
    analytes_meta$dataset.filename <- str_remove(abspath_dataset, here())
    
    abspath_analytes_meta <- file.path(
      dir_lakedata, dir_silver, dir_id, ds, 
      glue("meta_analytes_{ds}_{data_type}.tsv")
    )    
    write_tsv(x = analytes_meta, abspath_analytes_meta)
    
    # Generate Elastic Search indices
    for (esidx in es_indices) {
      index_settings <- ds_settings$es_indices[[esidx]]
      index_file <- index_settings$index_file
      index_id   <- index_settings$index_id
      
      file_to_index <- case_when(
        esidx == "index_dataset"  ~ abspath_dataset_meta,
        esidx == "index_events"   ~ abspath_events_meta,
        esidx == "index_analytes" ~ abspath_analytes_meta,
      )
      index_path <- file.path(dir_lakedata, dir_gold, dir_id, ds, index_file)
      write_esindex(file_to_index, index_path, index_id)
    }    
  }
}

