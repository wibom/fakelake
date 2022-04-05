library(readxl)
library(openxlsx) # to modify sheets in XLSX-files
library(RcppTOML)
library(yaml)
library(elastic)
library(tidyverse)

get_predictids <- function(x) {
  #' @x: numeric array
  x %>% str_pad(width = 7, pad = '0') %>% str_c("p", .)
}
get_dataidentifiers <- function(path, data_type) {
  
  if (str_to_lower(data_type) == "metabolon") {
    dataidentifiers <- 
      readxl::read_xlsx(path, sheet = "Sample Meta Data") %>% 
      filter(str_detect(GROUP_NAME, "Block")) %>% 
      pull(PARENT_SAMPLE_NAME) %>% 
      as.character()
  }
  
  if (str_to_lower(data_type) == "olink") {
    dataidentifiers <- 
      read_delim(
        path,
        delim = ";",
        col_types = cols(.default = "c")
      ) %>% #count(SampleID) %>% print(n = 60)
      filter(!str_detect(SampleID, "^CONTROL")) %>% 
      distinct(SampleID) %>% 
      pull(SampleID) %>% 
      as.character()
  }  
  
  return(dataidentifiers)
}
lookup_reference <- function(pids, reference_column, path_reference) {
  
  reference_ds <- read_tsv(
    path_reference, 
    col_types = cols(.default = col_character())
  )
  
  enframe(pids, name = NULL, value = "predict_id") %>% 
    left_join(reference_ds, by = "predict_id") %>% 
    pull(!!rlang::sym(reference_column))
  
}
write_metabolon_mock <- function(n_samps, mock_dir, filename, seed, 
                                 absdir_template_data) {
  
  #' `n_samps`: numeric; number of samples to create
  #' `mock_dir`: absolute path to output directory
  #' `filename`: name of output file
  #' `seed`: seed to use for randomization 
  #' `absdir_template_data`: absolute path to main template data directory
    
  update_wb <- function(wb, my_sheet, old_df, new_df) {
    # Remove old data, but keep the headers (row 1)
    openxlsx::deleteData(
      wb, 
      sheet = my_sheet, 
      cols = 1:ncol(old_df), 
      rows = 2:(nrow(old_df) + 1), 
      gridExpand = TRUE
    )
    # Replace with new data
    openxlsx::writeData(
      wb, 
      sheet = my_sheet, 
      x = new_df, 
      colNames = TRUE, 
      startRow = 1
    )    
    if (ncol(old_df) > ncol(new_df)) {
      openxlsx::deleteData(
        wb, 
        sheet = my_sheet, 
        cols = (ncol(new_df) + 1):ncol(old_df), 
        rows = 1,
        gridExpand = TRUE
      )      
    }    
    return(wb)
  }
  
  metabolon_template_dir <- file.path(dir_template_data, "metabolon") 
  metabolon_template <- file.path(
    metabolon_template_dir,
    "mock_UMEA-01-19ML+ CDT_EDTA PLASMA_30JUN2020.XLSX"
  )
  
  # copy documentaion to path
  dir.create(file.path(mock_dir, "contracts"), recursive = TRUE)
  dir.create(file.path(mock_dir, "doc"), recursive = TRUE)
  file.copy(
    from = file.path(metabolon_template_dir, "contracts/some_contract.pdf"),
    to = file.path(mock_dir, "contracts/some_contract.pdf")
  )
  doc_filename <- filename %>% str_replace("\\..+$", ".DOCX")
  file.copy(
    from = file.path(
      metabolon_template_dir, 
      "doc/UMEA-01-19ML+ REPORT_30JUN2020.DOCX"
    ),
    to = file.path(mock_dir, "doc", doc_filename)
  )
  
  # copy template to path, and open workbook object
  file.copy(
    from = metabolon_template, 
    to = file.path(mock_dir, filename),
    overwrite = TRUE
  )
  wb <- file.path(mock_dir, filename) %>%  openxlsx::loadWorkbook() # slow
  
  # sheet 2: Chemical Annotation ----
  # select a few metabolites to include
  selected_metabolites_mini <- c(
    # named metabolites, with inchikey values:
    "35", "50", "55", "62", "71", 
    # named metabolites, without inchikey values:
    "100001987", "100001988", "100001989", "100001990", "100001992", 
    # unnamed metabolites:
    "999926107", "999926108", "999926109", "999926111", "999926119"
  )
  sheet_2 <- 
    openxlsx::read.xlsx(wb, sheet = "Chemical Annotation", sep.names = " ") %>% 
    as_tibble() %>% 
    mutate(across(everything(), as.character))  
  sheet_2_mini <- 
    sheet_2 %>% 
    filter(CHEM_ID %in% selected_metabolites_mini)
  
  # Update excel workbook
  wb <- update_wb(wb, my_sheet = "Chemical Annotation", sheet_2, sheet_2_mini)
  
  
  # sheet 3: Sample Meta Data ----
  sheet_3 <- 
    openxlsx::read.xlsx(wb, sheet = "Sample Meta Data", sep.names = " ") %>% 
    as_tibble() %>% 
    mutate(across(everything(), as.character))
  samps_template <- 
    sheet_3 %>% 
    filter(str_detect(GROUP_NAME, "Block")) %>% 
    pull(PARENT_SAMPLE_NAME)
  samps_qc <- 
    sheet_3 %>% 
    filter(str_detect(GROUP_NAME, "Block", negate = TRUE)) %>% 
    pull(PARENT_SAMPLE_NAME)  
  set.seed(seed)
  samps_random <- sample(samps_template, size = n_samps)
  sheet_3_mini <- 
    sheet_3 %>% 
    filter(PARENT_SAMPLE_NAME %in% c(samps_random, samps_qc))
  
  # Update excel workbook
  wb <- update_wb(wb, my_sheet = "Sample Meta Data", sheet_3, sheet_3_mini)
  
  
  # sheet 4: Peak Area Data ----
  sheet_4 <- 
    openxlsx::read.xlsx(wb, sheet = "Peak Area Data") %>% 
    as_tibble()
  sheet_4_mini <- 
    sheet_4 %>% 
    select(all_of(c("PARENT_SAMPLE_NAME", selected_metabolites_mini))) %>% 
    filter(PARENT_SAMPLE_NAME %in% c(samps_random, samps_qc))
  
  # Update excel workbook
  wb <- update_wb(wb, my_sheet = "Peak Area Data", sheet_4, sheet_4_mini)
  
  
  # sheet 5: Batch-normalized Data ----
  sheet_5 <- 
    openxlsx::read.xlsx(wb, sheet = "Batch-normalized Data") %>% 
    as_tibble()
  sheet_5_mini <- 
    sheet_5 %>% 
    select(all_of(c("PARENT_SAMPLE_NAME", selected_metabolites_mini))) %>% 
    filter(PARENT_SAMPLE_NAME %in% c(samps_random, samps_qc))
  
  # Update excel workbook
  wb <- update_wb(wb, my_sheet = "Batch-normalized Data", sheet_5, sheet_5_mini)
  
  
  # sheet 6: Batch-norm Imputed Data ----
  sheet_6 <- 
    openxlsx::read.xlsx(wb, sheet = "Batch-norm Imputed Data") %>% 
    as_tibble()
  sheet_6_mini <- 
    sheet_6 %>% 
    select(all_of(c("PARENT_SAMPLE_NAME", selected_metabolites_mini))) %>% 
    filter(PARENT_SAMPLE_NAME %in% c(samps_random, samps_qc))
  
  # Update excel workbook
  wb <- update_wb(wb, my_sheet = "Batch-norm Imputed Data", sheet_6, 
                  sheet_6_mini)  
  
  # Write to disk ----
  sheets <- c(
    "Data Key and Explanation", "Chemical Annotation", "Sample Meta Data",
    "Peak Area Data", "Batch-normalized Data", "Batch-norm Imputed Data"
  )  
  openxlsx::worksheetOrder(wb) <- match(sheets, names(wb))
  openxlsx::saveWorkbook(
    wb, 
    file = file.path(mock_dir, filename), 
    overwrite = TRUE
  )
}
write_olink_mock <- function(n_samps, mock_dir, filename, seed, 
                             absdir_template_data) {
  #' `n_samps`: numeric; number of samples to create
  #' `mock_dir`: absolute path to output directory
  #' `filename`: name of output file
  #' `seed`: seed to use for randomization 
  #' `absdir_template_data`: absolute path to main template data directory
  
  # Olink data
  # Elin's Olink-data serves as template

  # Copy documentation from template to lake
  olink_template_dir <- file.path(absdir_template_data, "olink")
  dir.create(file.path(mock_dir, "contracts"), recursive = TRUE)
  dir.create(file.path(mock_dir, "doc"), recursive = TRUE)
  file.copy(
    from = file.path(olink_template_dir, "contracts/some_contract.pdf"),
    to = file.path(mock_dir, "contracts/some_contract.pdf")
  )
  doc_filename <- filename %>% str_replace("\\..+$", ".DOCX")
  file.copy(
    from = file.path(olink_template_dir, "doc/some_documentation.docx"),
    to = file.path(mock_dir, "doc", doc_filename)
  )  
  
  # Get random data
  olink_template <- read_delim(
    file.path(olink_template_dir, "olink_mockup_NPX.csv"),
    delim = ";",
    col_types = cols(.default = "c")
  )
  set.seed(seed)
  random_samples <- 
    olink_template %>% 
    filter(!str_detect(SampleID, "^CONTROL")) %>% 
    pull(SampleID) %>% 
    sample(., n_samps)
  mock_olink <- 
    olink_template %>% 
    filter(SampleID %in% random_samples | str_detect(SampleID, "^CONTROL"))
  write_csv2(
    mock_olink,
    file = file.path(mock_dir, filename)
  )
}
get_sample_metadata <- function(path_reference, predict_ids, vip_age, 
                                add_freeze_thaw, abspath_dataset, 
                                relpath_dataset, data_type, dataid_idx) {
  
  # reference columns
  rc_date <- case_when(
    vip_age == "40yrs" ~ "vip40_date",
    vip_age == "50yrs" ~ "vip50_date",
    vip_age == "60yrs" ~ "vip60_date"
  )
  rc_freezethaw <- case_when(
    vip_age == "40yrs" ~ "vip40_freezethaw",
    vip_age == "50yrs" ~ "vip50_freezethaw",
    vip_age == "60yrs" ~ "vip60_freezethaw"    
  )
  meta_sample <- tibble(
    # generic event metadata
    predict_id = predict_ids,
    event_type = "sample",
    event_date = lookup_reference(predict_ids, rc_date, path_reference),
    
    # sample meta data
    sample_type = "plasma",
    sample_anti_coagulant = "EDTA",
    sample_setting = "VIP",
    sample_freezethaw = 
      lookup_reference(predict_ids, rc_freezethaw, path_reference) %>% 
        as.numeric() + 
        add_freeze_thaw,
    sample_fasting = 3,
    
    # individual phenotype data (optionally supplied by researcher)
    case_control = c("case", "control") %>% rep_along(predict_ids, .),
    icd_o = ifelse(case_control == "case", "9440", NA_character_),
    
    # data (link event - data)
    data_id = 
      get_dataidentifiers(abspath_dataset, data_type = data_type)[dataid_idx],
    
    dataset.filename = relpath_dataset
  )  
  
}
generate_data_omics <- function(abspath_dataset, n_samples_tot, data_type, seed, 
                                absdir_template_data) {
  
  if (file.exists(abspath_dataset)) {
    return(0)
  }
  
  if (!dirname(abspath_dataset) %>% dir.exists()) {
    dirname(abspath_dataset) %>%  dir.create(recursive = TRUE)
  }
  
  if (str_to_lower(data_type) == "olink") {
    write_olink_mock(
      n_samps = n_samples_tot, 
      mock_dir = dirname(abspath_dataset),
      filename = basename(abspath_dataset),
      seed = seed,
      absdir_template_data = absdir_template_data
    )
  } else if (str_to_lower(data_type) == "metabolon"){
    write_metabolon_mock(
      n_samps = n_samples_tot, 
      mock_dir = dirname(abspath_dataset),
      filename = basename(abspath_dataset),
      seed = seed, 
      absdir_template_data = absdir_template_data
    )
  }

}
write_esindex <- function(datafile, target_indexfile, index_id, 
                          format = "wide") {
  dir_target <- dirname(target_indexfile)
  if (!dir.exists(dir_target)) {
    dir.create(dir_target, recursive = TRUE)
  }
  
  if (str_detect(datafile, "\\.tsv")) {
    if (str_detect(datafile, "nshds")) {
      # Typing the data will result in the `docs_bulk_prep()`-function producing
      # a better bulk index-file (.ndjson), which in turn will help
      # Elasticsearch dynamic index-mapping produce a better result.
      
      if(basename(datafile) %>% str_detect("health")) {
        idx_data <- 
          datafile %>% 
          read_tsv(
            col_types = cols(
              predict_id = "c",
              pat_code = "c",
              sex = "c",
              height = "n",
              weight = "n",
              bmi = "n",
              event_setting = "c",
              event_date = "D",
              event_fasting = "c",
              midja = "n",
              skol = "n",
              skol_mo = "n",
              hdl = "n",
              ldl = "n",
              stg = "n",
              stg_mo = "n",
              blods0 = "n",
              blods2 = "n",
              event_type = "c",
              sample_type = "c",
              sample_anti_coagulant = "c",
              dataset.filename = "c",
              dataset.datatype = "c"              
            )
          )
      } else if (basename(datafile) %>% str_detect("questionnaire")) {
        idx_data <- 
          datafile %>% 
          read_tsv(
            col_types = cols(
              predict_id = "c",
              pat_code = "c",
              sex = "c",
              event_setting = "c",
              enk = "c",
              fasta_enk = "c",
              event_date = "D",
              sambo = "c",
              event_type = "c",
              dataset.filename = "c",
              dataset.datatype = "c"           
            )
          )        
      }
    } else {
      idx_data <- datafile %>% read_tsv(show_col_types = FALSE)
    }
  } else if (str_detect(datafile, "\\.toml")) {
    idx_data <- 
      datafile %>% 
      parseTOML() %>% 
      unlist(recursive = FALSE) %>% 
      enframe() %>% 
      pivot_wider(names_from = "name", values_from = value)
  } else if (str_detect(datafile, "\\.yaml")) {
      idx_data <- 
        datafile %>% 
        read_yaml() %>% 
        unlist(recursive = FALSE) %>% 
        enframe() %>% 
        pivot_wider(names_from = "name", values_from = value)
      
      if (format == "long") {
        dataset.filename <- unlist(idx_data$dataset.filename)
        idx_data <- 
          idx_data %>% 
          pivot_longer(
            everything(),
            names_to = c("varcount", ".value"),
            names_pattern = "(var\\d+)\\.(.+)", 
            values_drop_na = TRUE
            
          ) %>% 
          unnest(cols = everything()) %>% 
          select(-varcount) %>% 
          mutate(dataset.filename = dataset.filename)
      }
  }
  
  # Type specific variables ahead of writing bulk index files (.ndjson) to disk
  to_txt <- c("sample_freezethaw", "sample_fasting", "icd_o", "data_id", 
              "event_type", "event_fasting")
  for (txtvar in to_txt) {
    if (txtvar %in% names(idx_data)) {
      idx_data[[txtvar]] <- as.character(idx_data[[txtvar]])
    } else {
      next
    }
  }
  
  docs_bulk_prep(
    idx_data, 
    index = index_id,
    #doc_ids = 1:nrow(idx_data),
    path = target_indexfile, 
    quiet = TRUE,
    digits = 2
  )  
  
}
get_metabolon_chemical_annotation <- function(metabolon_file) {
  chemical_annotation <- readxl::read_excel(
    metabolon_file, 
    sheet = "Chemical Annotation", 
    col_types = "text"
  )
}
get_olink_proteins <- function(olink_file) {
  olink_unique_proteins <- 
    read_delim(olink_file, delim = ";", col_types = cols(.default = "c")) %>% 
    select(OlinkID, UniProt, Assay, Panel) %>% 
    distinct(.keep_all = TRUE)
}