# Function to make grid that covers an abritary shape, locked to closest pixel edges
make_grid_custom <- function(starting_shape,pixel_size){
  # Take bounding box of starting shape, and expand it out to lock to the closest pixel edges
  c(floor(st_bbox(starting_shape)[1:2]/pixel_size)*pixel_size, 
    ceiling(st_bbox(starting_shape)[3:4]/pixel_size*pixel_size)) %>%
    # Turn this box into a bounding box
    st_bbox(crs = st_crs(starting_shape)) %>%
    # Turn this into sfc
    st_as_sfc() %>%
    # Now make the grids
    st_make_grid(pixel_size) %>%
    # Now make sf
    st_as_sf() %>%
    # Filter pixels to only those that have some overlap with starting shape; 
    # Necessary when you start with things like the whole global ocean
    filter(!st_within(., starting_shape, sparse = FALSE)[,1]) %>%
    # Add pixel id
    mutate(pixel_id = row_number())%>% 
    # Rename geomtery for sf consistency
    rename(geometry = x) 
}

# This function takes an errdap dataset name, and returns the latitude range and longitude range of the dataset
# And only returns numbers within range of data_grid
get_spatial_boundaries <- function(dataset_name,data_grid){
  
  # Pull dataset info, which includes lat and lon extent
  dataset_info <- info(dataset_name)
  
  # Determine max lat extent in the data
  lat_range <- dataset_info$alldata$latitude%>% 
    filter(attribute_name == "actual_range") %>% 
    .$value %>% 
    str_split(", ") %>% 
    .[[1]] %>% 
    as.numeric()
  
  # Make sure lat_range stays within the y-axis of data_grid
  lat_range[1] <- pmax(lat_range[1],st_bbox(data_grid)[2])
  lat_range[2] <- pmin(lat_range[2],st_bbox(data_grid)[4])
  
  # Determine max lon extent in the data
  lon_range <- dataset_info$alldata$longitude %>% 
    filter(attribute_name == "actual_range") %>% 
    .$value %>% 
    str_split(", ") %>% 
    .[[1]] %>% 
    as.numeric()
  
  # Make sure lon_range stays within the x-axis of data_grid
  lon_range[1] <- pmax(lon_range[1],st_bbox(data_grid)[1])
  lon_range[2] <- pmin(lon_range[2],st_bbox(data_grid)[3])
  
  return(list(lat_range = lat_range,
              lon_range = lon_range))
}


# Download data one day or month at a time, in parallel if desired
download_errdap_data <- function(dataset_name,
                                 variables,
                                 # Set this to whatever the temporal resolution of the data are
                                 # e.g., dhw_5km should be "day", dhw_5km_monthly should be "month"
                                 temporal_resolution = "day",
                                 date_start,
                                 date_end,
                                 data_grid,
                                 run_parallel = TRUE){
  
  tmp_data_directory <- glue::glue("{data_directory}/raw/erddap/{dataset_name}")
  
  # Create data download folder, if it doesn't already exist
  if(!dir.exists(tmp_data_directory)) dir.create(tmp_data_directory)
  
  # Get spatial boundary for pulling data, based on the max extent of the data and our data_grid
  dataset_spatial_range <- get_spatial_boundaries(dataset_name,data_grid)
  
  # Determine the full date range from date_start to date_end
  date_range <- seq(date(date_start),date(date_end),by=temporal_resolution)
  
  # If run_parallel, run in parallel; otherwise, run sequentially
  if(run_parallel) plan(multisession) else plan(sequential)
  
  # Map over all dates
  date_range %>%
    # Download data sequentially
    future_map(function(date_tmp){
      tmp_file_name <- glue::glue("{tmp_data_directory}/{dataset_name}_{date_tmp}.csv")
      
      # Only re-download data if you need to
      if(file.exists(tmp_file_name)) return()
      
      data_tmp <- rerddap::griddap(dataset_name, 
                                   latitude = dataset_spatial_range$lat_range,
                                   longitude = dataset_spatial_range$lon_range, 
                                   time = c(date_tmp, date_tmp),
                                   fields = variables,
                                   store = rerddap::memory(),
                                   fmt = "csv")
      
      data_tmp %>% 
        # Don't save data that are just NAs
        dplyr::filter(!dplyr::if_all(-c(time,latitude,longitude),is.na)) %>%
        # Don't save time column; this will make downloaded data more compact
        dplyr::select(-time) %>%
        # Use data.table::fwrite since it's faster
        data.table::fwrite(tmp_file_name)
    }, .options = furrr_options(globals=c("data_directory","dataset_name","variables",
                                          "dataset_spatial_range"),
                                seed = 101),.progress=TRUE)
}

# This wraps the download function and tries the download several times
# This is useful, since sometimes it times out
# Adapted from Jen Raynor's code in emLab/Projects/current-projects/arnhold-bwmpa/project-materials/programs/helper-download_erddap.R
download_errdap_data_wrapper <- function(dataset_name,
                                         variables,
                                         temporal_resolution,
                                         date_start,
                                         date_end,
                                         data_grid,
                                         run_parallel = TRUE,
                                         number_tries = 10){
  # set starting parameters
  r <- NULL
  
  attempt <- 1
  
  # proceed for 3 attempts
  while(is.null(r) && attempt <= number_tries){
    
    attempt <- attempt + 1
    
    try(r <- download_errdap_data(dataset_name,
                                  variables,
                                  temporal_resolution,
                                  date_start,
                                  date_end,
                                  data_grid,
                                  run_parallel))
    
  } 
}

# Read each day's worth of data and spatially aggregate  in parallel if desired
spatially_aggregate_errdap_data_wrapper <- function(dataset_name,
                                                    spatial_aggregation,
                                                    run_parallel = TRUE){
  tmp_data_base <- glue::glue("{data_directory}/clean/erddap/{dataset_name}")
  # Create data download folder, if it doesn't already exist
  if(!dir.exists(tmp_data_base)) dir.create(tmp_data_base)
  tmp_data_directory <- glue::glue("{tmp_data_base}/spatially_aggregated_{pixel_size}_degree")
  if(!dir.exists(tmp_data_directory)) dir.create(tmp_data_directory)
  
  if(run_parallel) plan(multisession) else plan(sequential)
  # Create tibble of all files in raw directory
  list.files(glue("{data_directory}/raw/erddap/{dataset_name}")) %>%
    future_map_dfr(function(file_temp){
      
      date_tmp <- file_temp %>%
        # Extract date
        stringr::str_replace(glue::glue("{dataset_name}_"),"") %>% 
        stringr::str_remove(".csv") %>%
        lubridate::date()
      processed_file_name <- glue::glue("{tmp_data_directory}/{date_tmp}.csv")
      # If it's already processed, don't need to re-do it
      if(file.exists(processed_file_name)) return()
      # Load files one at a time
      data.table::fread(glue::glue("{data_directory}/raw/erddap/{dataset_name}/{file_temp}")) %>%
        # Now we spatially aggregate over our grid
        # Convert tabular data to sf points
        sf::st_as_sf(coords = c("longitude","latitude"),
                     crs = sf::st_crs(spatial_aggregation)) %>% 
        # Rasterize points to stars object
        stars::st_rasterize() %>% 
        # Convert stars to raster, so we can use exactextractr::exact_extract
        as("Raster") %>%
        # Let's spatially aggregate by taking the mean value for each of our pixels
        exactextractr::exact_extract(spatial_aggregation,
                                     # From the help: "mean - the mean cell value, weighted by the fraction of each cell that is covered by the polygon"
                                     "mean",
                                     # Include pixel_id column so we can match on it later
                                     append_cols = "pixel_id",
                                     progress = FALSE)%>% 
        # Don't save data that are just NAs
        dplyr::filter(!dplyr::if_all(-c(pixel_id),is.na))%>%
        # If there's only one layer of data, give mean value column a meaningful name
        dplyr::rename_with(~glue::glue("mean.{dataset_name}"), matches("^mean$")) %>%
        dplyr::mutate(date = date_tmp) %>%
        # Write aggregated data to clean data folder
        data.table::fwrite(processed_file_name)
      return()
    }, .options = furrr_options(globals=c("data_directory","dataset_name","spatial_aggregation"),
                                seed = 101),.progress=TRUE)
}

# Read in spatially aggregated data and temporally aggregate 
temporally_aggregate_errdap_data_wrapper <- function(dataset_name,
                                                     temporal_aggregation = "month",
                                                     run_parallel = TRUE){

  # Read in spatially aggregated data
  tmp_data_directory <- glue::glue("{data_directory}/clean/erddap/{dataset_name}/spatially_aggregated_{pixel_size}_degree")
  if(run_parallel) plan(multisession) else plan(sequential)
  result<-   list.files(tmp_data_directory)  %>% 
    # Only want data 2016+
    str_subset("2016|2017|2018|2019|2020|2021")%>%
    future_map_dfr(function(file_temp){

      data.table::fread(glue::glue("{tmp_data_directory}/{file_temp}"))%>%
        collapse::ftransform(date = lubridate::floor_date(date,temporal_aggregation))%>%
        # Remove mean suffix from spatial aggregation, since we will do another aggregation below
        dplyr::rename_with(~ gsub('mean.', '', .x)) %>% 
        # give sst better name
        dplyr::rename_with(~ gsub('sst', 'sst_c', .x))%>% 
        # give surface current speed better name
        dplyr::rename_with(~ gsub('vgos', 'surface_current_v_m_s', .x))%>% 
        # give surface current speed  better name
        dplyr::rename_with(~ gsub('ugos', 'surface_current_u_m_s', .x))%>% 
        # give sst anomaly better name
        dplyr::rename_with(~ gsub('anom', 'anom_sst_c', .x))%>% 
        # give sea level height better name
        dplyr::rename_with(~ gsub('nesdisSSH1day', 'slh_m', .x)) %>%
        dplyr::rename_with(~ gsub('erdMH1chlamday', 'chl_mg_per_m3', .x)) %>%
        # Remove zlev column from SST - don't need this
        dplyr::select(-one_of("zlev"))
    }, .options = furrr_options(globals=c("data_directory","dataset_name"),
                                seed = 101),.progress=TRUE)  
  result <- result %>%
    # Aggregate temporally over the aggregated date
    # Do both mean and SD
    # Use collapse functions - much faster than regualr
    collapse::fgroup_by(pixel_id,date) %>% {
      collapse::add_vars(collapse::add_stub(collapse::fmean(., keep.group_vars = TRUE),"_mean",pre=FALSE,cols=-c(1,2)),
                         collapse::add_stub(collapse::fsd(., keep.group_vars = FALSE), "_sd",pre=FALSE)) }
  # If aggregating by year, add year column and remove date column
  if(temporal_aggregation == "year") result <- result %>%
    collapse::ftransform(year = lubridate::year(date))%>%
    dplyr::select(-date)
}

determine_missing_dates <- function(dataset_name,
                                    temporal_resolution = "day",
                                    date_start,
                                    date_end){
  
  date_range <- seq(date(date_start),date(date_end),by=temporal_resolution) %>%
    lubridate::floor_date(temporal_resolution)
  
  downloaded_data_files <- list.files(glue("{data_directory}/raw/erddap/{dataset_name}"))%>%
    stringr::str_replace(glue::glue("{dataset_name}_"),"") %>% 
    stringr::str_remove(".csv") %>%
    date() %>%
    lubridate::floor_date(temporal_resolution)
  
  
  missing_dates_tibble <- tibble(date = date_range) %>%
    filter(!(date %in% downloaded_data_files))
  
  missing_dates_summary <- tibble(n_possible_dates = length(date_range),
                                  n_downloaded_dates = length(downloaded_data_files)) %>%
    mutate(fraction_downloaded_dates = signif(n_downloaded_dates/n_possible_dates,3))
  
  
  return(list(missing_dates_tibble = missing_dates_tibble,
              missing_dates_summary = missing_dates_summary))
}
