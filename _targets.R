# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline # nolint

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
library(glue)
options(tidymodels.dark = TRUE)

# Set data directory on emLab's Google Shared Drive where targets interm rds objects will live
# This path will need to be modified by each user, since everyone may have a different path to this directory
# targets_data_directory <- "/Users/Shared/nextcloud/emLab/projects/current-projects/arnhold-bwmpa-redistribution/data/"
# Set directory where target objects will be saved
# tar_config_set(store = glue("{targets_data_directory}_targets"))

# Set data directory where pre-processed data lives on GitHub repo
# Model features were processed in the files *_data_wrangling_*.Rmd
# Some model features datasets require special permissions (i.e., those from Global Fishing Watch)
# Others require very large downloads
# Therefore, the processing of these model features datasets is not included in the targets pipeline,
# They are rather included in data/model_features
# Additionally, processing the hypothetical MPA network scenario datasets is time consuming
# and requires very large files; we therefore simply save the pre-processed networks under data/mpa_sceanarios
data_directory <- here::here("data")

# Set target options:
tar_option_set(
  packages = c("tidymodels",
               "readr",
               "timetk",
               "kernelshap",
               "ranger",
               "doFuture",
               "rlang",
               "yardstick",
               "sf",
               "stars"), # packages that your targets need to run
  format = "rds" # default storage format)
)
# Run the R scripts in the R/ folder with your custom functions:
tar_source("r/_functions_modeling.R")

# How many cores should be left free for other things?
free_cores <- 0

# Set spatial analysis projection - Mollweide
analysis_projection <- "+proj=moll +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"


# Define target list
list(
  # What hypothetical network sizes should we consider?
  # Consider a range of sizes, including the 2 sizes from EBSA and Visalli et al.
  tar_target(
    name = mpa_network_scenario_sizes,
    c(0.03,0.05,0.075,0.1,0.16,0.2,0.3)
  ),
  tar_target(
    enso_index,
    wrangle_enso_data()
  ),
  tar_target(
    name = errdap_chl_file,
    glue("{data_directory}/model_features/errdap_chl.csv"),
    format = "file"
  ),
  tar_target(
    errdap_chl,
    read_csv(errdap_chl_file)
  ),
  tar_target(
    name = errdap_sst_file,
    glue("{data_directory}/model_features/errdap_sst.csv"),
    format = "file"
  ),
  tar_target(
    errdap_sst,
    read_csv(errdap_sst_file)
  ),
  # The bunker fuel price data used in our analysis are subject to restricted use and are not available for public redistribution. 
  # We therefore do not include these raw data in our repository.
  # In order to fully reproduce the analysis, you will need to obtain these data directly from Bunker Index.
  # Information on obtaining these data directly from Bunker Index can be found at https://bunkerindex.com/
  # Once you have obtained these data, simply save them locally as `data/raw/bix_world_ifo_380_index.csv` and the pipeline will process them
  # Always be mindful to follow Bunker Index's terms and conditions, which are subject to change at any time: https://bunkerindex.com/terms
  # Ensure that these are daily data for the BIX World IFO 380 index, and that the time series covers 2016-2021
  # Also ensure that the CSV has the columns 'Date' and 'Price' (where 'Price' is in USD/MT) 
  # so that the function `wrangle_fuel_price_data` can properly aggregate the daily price data to annual means and standard deviations of price
  tar_target(
    name = fuel_price_data_file,
    glue("{data_directory}/raw/bix_world_ifo_380_index.csv"),
    format = "file"
  ),
  tar_target(
    fuel_price_data_raw,
    read_csv(fuel_price_data_file)
  ),
  tar_target(
    fuel_price_data,
    wrangle_fuel_price_data(fuel_price_data_raw)
  ),
  tar_target(
    name = gfi_file,
    glue("{data_directory}/raw/Global\ Fishing\ Index\ 2021\ -\ Data\ download\ V1.1/Global\ Fishing\ Index\ 2021\ Data\ for\ Download\ V1.1.xlsx"),
    format = "file"
  ),
  # Load GFI data
  # The Global Fishing Index (GFI) is compiled by the Minderoo Foundation, and is detailed in [Spijkers et al. 2022](https://onlinelibrary.wiley.com/doi/pdf/10.1111/faf.12713)
  tar_target(
    gfi,
    readxl::read_xlsx(gfi_file, sheet = "Progress and Governance results",skip=1) %>%
      dplyr::select(eez_sovereign = `ISO Code`,
                    gfi_fisheries_governance_capacity = `Governance capacity`)
  ),
  tar_target(
    name = gfw_fishing_by_pixel_year_file,
    glue("{data_directory}/model_features/gfw_fishing_by_pixel_year.csv"),
    format = "file"
  ),
  tar_target(
    gfw_fishing_by_pixel_year,
    read_csv(gfw_fishing_by_pixel_year_file)
  ),
  tar_target(
    name = gfw_reception_quality_file,
    glue("{data_directory}/model_features/gfw_reception_quality.csv"),
    format = "file"
  ),
  tar_target(
    gfw_reception_quality,
    read_csv(gfw_reception_quality_file)
  ),
  tar_target(
    name = gfw_static_spatial_measures_file,
    glue("{data_directory}/model_features/gfw_static_spatial_measures.csv"),
    format = "file"
  ),
  tar_target(
    gfw_static_spatial_measures,
    read_csv(gfw_static_spatial_measures_file)
  ),
  tar_target(
    name = global_grid_file,
    glue("{data_directory}/model_features/global_grid.csv"),
    format = "file"
  ),
  tar_target(
    global_grid,
    read_csv(global_grid_file)
  ),
  # Project global grid into analysis projection
  tar_target(
    name = projected_global_grid,
    make_projected_global_grid(global_grid,
                               analysis_projection)
  ),
  # Create global distance rasters for each pixel id in grid to all areas in the ocean
  tar_target(
    name = distance_raster,
    make_distance_rasters_wrapper(projected_global_grid,
                                  free_cores,
                                  analysis_projection)
  ),
  # Save distance_raster as stack, which is a much more efficient data storage and analysis structure
  # Since all rasters are same extent, resolution, and projection
  # Make sure each layer is named after corresponding pixel_id
  tar_target(
    name = distance_raster_stack,
    raster::stack(distance_raster$distance_raster,
                  quick = TRUE) %>%
      setNames(distance_raster$pixel_id)
  ),
  #Find neighbors for each pixel, 1 pixel and 2 pixels away
  tar_target(
    name = neighbors,
    find_neighbors(projected_global_grid)
  ),
  # Load shapefile of NTZ MPAs
  tar_target(
    name = mpa_geopackage_file,
    glue("{data_directory}/raw/current_2020_no_take_mpa_geopackage.gpkg"),
    format = "file"
  ),
  tar_target(
    mpa,
    sf::st_read(mpa_geopackage_file)
  ),
  # Generate MPA-based features
  tar_target(
    name = mpa_features,
    make_mpa_features(mpa,
                      projected_global_grid,
                      distance_raster_stack,
                      neighbors)
  ),
  # Load shapefile of EEZs
  tar_target(
    name = eez_file,
    glue("{data_directory}/raw/World_EEZ_v11_20191118_LR/eez_v11_lowres.gpkg"),
    format = "file"
  ),
  tar_target(
    eez,
    sf::st_read(eez_file)
  ),
  # Generate EEZ-based features
  tar_target(
    name = eez_features,
    make_eez_features(eez,
                      projected_global_grid,
                      distance_raster_stack,
                      analysis_projection)
  ),
  # Load mesopelagic zone data from Marine Regions
  # https://www.marineregions.org/gazetteer.php?p=details&id=50384
  tar_target(
    name = mesopelagic_regions_file,
    glue("{data_directory}/raw/mesopelagiczones/mesopelagiczones.shp"),
    format = "file"
  ),
  tar_target(
    mesopelagic_regions,
    sf::st_read(mesopelagic_regions_file)
  ),
  # Generate mesopelagic-based features
  tar_target(
    name = mesopelagic_regions_features,
    wrangle_mesopelagic_zones(mesopelagic_regions,
                              projected_global_grid,
                              analysis_projection)),
  # Load seamount data
  # from Yassen et al. 2020 - List of seamounts in the world oceans - An update](https://doi.pangaea.de/10.1594/PANGAEA.921688
  #  This is an updated dataset based on the original [Yassen et al. 2011 - List of seamounts in the world ocean.](https://doi.org/10.1594/PANGAEA.757562)
  tar_target(
    name = seamounts_file,
    glue("{data_directory}/raw/seamounts-yesson-2019/YessonEtAl2019-Seamounts-V2.shp"),
    format = "file"
  ),
  tar_target(
    seamounts,
    sf::st_read(seamounts_file)
  ),
  # Generate mesopelagic-based features
  tar_target(
    name = seamounts_features,
    wrangle_seamounts(seamounts,
                      projected_global_grid,
                      analysis_projection)),
  tar_target(
    name = oceans_file,
    glue("{data_directory}/model_features/oceans.csv"),
    format = "file"
  ),
  tar_target(
    oceans,
    read_csv(oceans_file)
  ),
  tar_target(
    pdo_index,
    wrangle_pdo_data()
  ),
  tar_target(
    name = remss_wind_file,
    glue("{data_directory}/model_features/remss_wind.csv"),
    format = "file"
  ),
  tar_target(
    remss_wind,
    read_csv(remss_wind_file)
  ),
  # Combine all model features datasets into complete training dataset
  tar_target(
    training_dataset,
    make_training_dataset(eez_features = eez_features,
                          enso_index = enso_index,
                          errdap_chl = errdap_chl,
                          errdap_sst = errdap_sst,
                          fuel_price_data = fuel_price_data,
                          gfi = gfi,
                          gfw_fishing_by_pixel_year = gfw_fishing_by_pixel_year,
                          gfw_reception_quality = gfw_reception_quality,
                          gfw_static_spatial_measures = gfw_static_spatial_measures,
                          data_grid = projected_global_grid,
                          mesopelagic_regions = mesopelagic_regions_features,
                          mpa_features = mpa_features,
                          oceans = oceans,
                          pdo_index = pdo_index,
                          remss_wind = remss_wind,
                          seamounts = seamounts_features)
  ),
  # Load and process hypothetical MPA network data for simulations
  # Visali et al. 2020 network from Data-driven approach for highlighting priority areas for protection in marine areas beyond national jurisdiction
  # https://doi.org/10.1016/j.marpol.2020.103927
  # downloaded from here: https://github.com/BenioffOceanInitiative/bbnj/tree/master/inst/app/www/scenarios
  tar_target(
    name = visali_network_data_file,
    glue("{data_directory}/raw/visalli_et_al_mpa_network/scenarios/s04a.biofish.alltime.mol50km_sol_gcs.shp"),
    format = "file"
  ),
  tar_target(
    name = visali_network_data,
    sf::read_sf(visali_network_data_file)
  ),
  tar_target(
    name = simulation_mpa_features_visalli,
    make_mpa_network(visali_network_data,
                     mpa,
                     training_dataset,
                     distance_raster_stack,
                     neighbors,
                     projected_global_grid,
                     analysis_projection,
                     "visali")
  ),
  # Ecologically or Biologically Significant Marine Areas
  # Data are downloaded from here: https://github.com/iobis/ebsa_np
  # Areas were selected as part of an expert-driven approach at Conference of the Parties to the Convention on Biological Diversity
  # 7 criteria were evaluated: https://www.cbd.int/ebsa/about
  tar_target(
    name = ebsa_network_data_file,
    glue("{data_directory}/raw/ebsa/Global_EBSAs_Automated_Final_1104_2016_WGS84/Global_EBSAs_Automated_Final_1104_2016_WGS84.shp"),
    format = "file"
  ),
  tar_target(
    name = ebsa_network_data,
    sf::read_sf(ebsa_network_data_file)
  ),
  tar_target(
    name = simulation_mpa_features_ebsa,
    make_mpa_network(ebsa_network_data,
                     mpa,
                     training_dataset,
                     distance_raster_stack,
                     neighbors,
                     projected_global_grid,
                     analysis_projection,
                     "ebsa")
  ),
  # Priority data come from Sala et al. 2021
  # https://www.nature.com/articles/s41586-021-03371-z
  # We process 4 versions of the prioritized networks: multi-objective, biodiversity, carbon, and catch.
  tar_target(
    name = sala_multiobjective_network_data_file,
    glue("{data_directory}/raw/sala_2021_mpa_networks/doi_10.25349_D9N89M__v5/multiobjective_ranking_equal_weights.tif"),
    format = "file"
  ),
  tar_target(
    name = sala_multiobjective_network_data,
    stars::read_stars(sala_multiobjective_network_data_file)
  ),
  tar_target(
    name = simulation_mpa_features_sala_multi_objective,
    make_sala_mpa_network(sala_multiobjective_network_data,
                          training_dataset,
                          mpa,
                          distance_raster_stack,
                          neighbors,
                          projected_global_grid,
                          analysis_projection,
                          "sala_multi_objective",
                          mpa_network_scenario_sizes)
  ),
  tar_target(
    name = sala_biodiversity_network_data_file,
    glue("{data_directory}/raw/sala_2021_mpa_networks/doi_10.25349_D9N89M__v5/biodiversity_ranking.tif"),
    format = "file"
  ),
  tar_target(
    name = sala_biodiversity_network_data,
    stars::read_stars(sala_biodiversity_network_data_file)
  ),
  tar_target(
    name = simulation_mpa_features_sala_biodiversity,
    make_sala_mpa_network(sala_biodiversity_network_data,
                          training_dataset,
                          mpa,
                          distance_raster_stack,
                          neighbors,
                          projected_global_grid,
                          analysis_projection,
                          "sala_biodiversity",
                          mpa_network_scenario_sizes)
  ),
  tar_target(
    name = sala_carbon_network_data_file,
    glue("{data_directory}/raw/sala_2021_mpa_networks/doi_10.25349_D9N89M__v5/carbon_ranking.tif"),
    format = "file"
  ),
  tar_target(
    name = sala_carbon_network_data,
    stars::read_stars(sala_carbon_network_data_file)
  ),
  tar_target(
    name = simulation_mpa_features_sala_carbon,
    make_sala_mpa_network(sala_carbon_network_data,
                          training_dataset,
                          mpa,
                          distance_raster_stack,
                          neighbors,
                          projected_global_grid,
                          analysis_projection,
                          "sala_carbon",
                          mpa_network_scenario_sizes)
  ),
  tar_target(
    name = sala_food_network_data_file,
    glue("{data_directory}/raw/sala_2021_mpa_networks/doi_10.25349_D9N89M__v5/food_ranking.tif"),
    format = "file"
  ),
  tar_target(
    name = sala_food_network_data,
    stars::read_stars(sala_food_network_data_file)
  ),
  tar_target(
    name = simulation_mpa_features_sala_food,
    make_sala_mpa_network(sala_food_network_data,
                          training_dataset,
                          mpa,
                          distance_raster_stack,
                          neighbors,
                          projected_global_grid,
                          analysis_projection,
                          "sala_food",
                          mpa_network_scenario_sizes)
  ),
  tar_target(
    name = simulation_mpa_features_random,
    make_random_mpa_network(training_dataset,
                            mpa,
                            distance_raster_stack,
                            neighbors,
                            projected_global_grid,
                            analysis_projection,
                            "random",
                            mpa_network_scenario_sizes)
  ),
  tar_target(
    name = simulation_mpa_features_most_fished,
    make_most_fished_mpa_network(training_dataset,
                                 mpa,
                                 distance_raster_stack,
                                 neighbors,
                                 projected_global_grid,
                                 analysis_projection,
                                 "most_fished",
                                 mpa_network_scenario_sizes)
  ),
  tar_target(
    name = simulation_mpa_features_unfished,
    make_unfished_mpa_network(training_dataset,
                              mpa,
                              distance_raster_stack,
                              neighbors,
                              projected_global_grid,
                              analysis_projection,
                              "unfished",
                              mpa_network_scenario_sizes)
  ),
  # Combine simulation data
  tar_target(
    full_simulation_data,
    make_full_simulation_data(training_dataset = training_dataset,
                              simulation_mpa_features_unfished = simulation_mpa_features_unfished,
                              simulation_mpa_features_most_fished = simulation_mpa_features_most_fished,
                              simulation_mpa_features_ebsa = simulation_mpa_features_ebsa,
                              simulation_mpa_features_visalli = simulation_mpa_features_visalli,
                              simulation_mpa_features_sala_multi_objective = simulation_mpa_features_sala_multi_objective,
                              simulation_mpa_features_sala_biodiversity = simulation_mpa_features_sala_biodiversity,
                              simulation_mpa_features_sala_carbon = simulation_mpa_features_sala_carbon,
                              simulation_mpa_features_sala_food = simulation_mpa_features_sala_food,
                              simulation_mpa_features_random = simulation_mpa_features_random)
  ),
  #  Train the final models, using all data
  #  This is the baseline specification, using random forest and all model features
  tar_target(
    name = trained_models,
    train_all_models(training_dataset,
                     free_cores = free_cores,
                     model_type = "rf",
                     data_type = "all_data",
                     oos_split = "none")
  ),
  # Get temporal OOS performance for baseline model (splitting test/training by time)
  tar_target(
    name = oos_results_baseline,
    train_all_models(training_dataset,
                     free_cores = free_cores, 
                     model_type = "rf",
                     data_type = "all_data",
                     oos_split = "temporal")
  ),
  # Get temporal OOS performance (splitting test/training by time) 
  # This is the a robustness check, using logistic/linear regression instead of random forest
  tar_target(
    name = oos_results_lm,
    train_all_models(training_dataset,
                     free_cores = free_cores, 
                     model_type = "lm",
                     data_type = "all_data",
                     oos_split = "temporal")
  ),
  # Get temporal OOS performance (splitting test/training by time)
  # This is the a robustness check, using logistic/linear regression instead of random forest, and just lagged fishing
  tar_target(
    name = oos_results_lm_just_lagged_fishing,
    train_all_models(training_dataset,
                     free_cores = free_cores, 
                     model_type = "lm",
                     data_type = "just_lagged_fishing",
                     oos_split = "temporal")
  ),
  # Get temporal OOS performance (splitting test/training by time)
  # This is the a robustness check, using lagged effort as the only model feature
  tar_target(
    name = oos_results_just_lagged_fishing,
    train_all_models(training_dataset,
                     free_cores = free_cores, 
                     model_type = "rf",
                     data_type = "just_lagged_fishing",
                     oos_split = "temporal")
  ),
  # Get spatiotemporal OOS performance for baseline model (splitting test/training by time and ocean)
  tar_target(
    name = oos_results_baseline_spatiotemporal,
    train_all_models(training_dataset,
                     free_cores = free_cores, 
                     model_type = "rf",
                     data_type = "all_data",
                     oos_split = "spatiotemporal")
  ),
  # Run the simulations, by using the trained models to make predictions for all MPA scenarios
  tar_target(
    name = simulation_results,
    run_simulations(full_simulation_data,
                    trained_models)
  ),
  # Generate Shapley feature importance/contributions using the final trained model - Stage 1, forecast_horizon of 1
  tar_target(
    name = shapley_contributions_stage_1,
    generate_shapley_contributions(trained_models,
                                   stage_1_or_2 = 1,
                                   free_cores = free_cores)
  ),
  # Generate Shapley feature importance/contributions using the final trained model - Stage 2, forecast_horizon of 1
  tar_target(
    name = shapley_contributions_stage_2,
    generate_shapley_contributions(trained_models,
                                   stage_1_or_2 = 2,
                                   free_cores = free_cores)
  ),
  # Load Palau GFW data
  # generated in 03_data_wrangling_gfw.Rmd, which requires special permissions
  # First load spatial data for Palau EEZ, aggregated across vessels
  tar_target(
    name = palau_gfw_data_file,
    glue("{data_directory}/raw/palau_gfw_data.csv"),
    format = "file"
  ),
  tar_target(
    palau_gfw_data,
    readr::read_csv(palau_gfw_data_file)
  ),
  # Now load vessel-level data for pre-MPA fleet that fished in Palau
  # Data is global, and aggregated by EEZ
  tar_target(
    name = palau_vessel_level_gfw_data_file,
    glue("{data_directory}/raw/palau_vessel_level_gfw_data.csv"),
    format = "file"
  ),
  tar_target(
    palau_vessel_level_gfw_data,
    readr::read_csv(palau_vessel_level_gfw_data_file)
  ),
  # Render final figures and tables
  tar_render(final_figures_tables, "r/05_analysis_generate_paper_figures_tables.rmd")
  
)
