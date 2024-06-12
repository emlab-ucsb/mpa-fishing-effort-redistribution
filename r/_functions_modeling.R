# Input: The full training dataset 
# Output: A dataset that has lagged fishing hours and future fishing hours
create_future_dataset <- function(df,forecast_horizon){
  
  # Calculate minimum non-zero fishing value
  # We will add this divided by 2 to lagged fishing before logging it, so we don't get infinite values
  min_non_zero_fishing <- df %>%
    filter(fishing_hours_per_m2 > 0) %>%
    .$fishing_hours_per_m2 %>%
    min()
  
  df %>%
    mutate(fishing_hours_per_m2_lag = fishing_hours_per_m2) %>%
    group_by(pixel_id) %>%
    mutate(fishing_hours_per_m2 = dplyr::lead(fishing_hours_per_m2_lag,
                                              n = forecast_horizon,
                                              order_by = year),
           fishing_binary = dplyr::lead(fishing_binary,
                                        n = forecast_horizon,
                                        order_by = year))%>%
    ungroup() %>%
    # If there's missing lead data which will occur when we don't have enough years of future datafor the current forecast_horizon, just remove these rows
    filter(!is.na(fishing_hours_per_m2)) %>%
    # filter(year <= 2020) %>%
    # Add logged effort
    mutate(fishing_hours_per_m2_log = log(fishing_hours_per_m2),
           fishing_hours_per_m2_lag_log = log(fishing_hours_per_m2_lag + min_non_zero_fishing / 2)) %>% 
    # Make logical columns into factors
    mutate(across(where(is.logical),~as.factor(.)))
}

# Take in stage 1 probability predictions
# And using optimized cutoff, assign class
add_classification_to_stage_1 <- function(predictions_stage_1,optimized_stage_1_cutoff){
  predictions_stage_1 %>%
    mutate(.pred_class = ifelse(.pred_1 >= optimized_stage_1_cutoff, 1, 0),
           .pred_class = factor(.pred_class, levels = c("0","1")))
}

# Function takes in stage 1 classification predictions and truth
# and creates stage 1 classification performance metrics
# for both global (across all pixels), and by MPA region (inside, partial, or outside)
calculate_performance_stage_1 <- function(predictions_stage_1,multi_metric_classification){
  
  # Calculate global performance, across all pixels regardless of region
  # These first metrics use the optimized classification threshold
  global_perf <- predictions_stage_1 %>%
    multi_metric_classification(truth = fishing_binary, 
                                estimate = .pred_class) %>%
    mutate(region = "Global")
  
  # Also calculate global ROC AUC, which is agnostic to the classification threshold
  global_perf_roc_auc <- predictions_stage_1 %>%
    roc_auc(fishing_binary,
            .pred_1,
            event_level = "second") %>%
    mutate(region = "Global")
  
  
  # Do this by MPA region (inside, partial, or outside)
  region_perf <- predictions_stage_1    %>%
    group_by(region) %>%
    multi_metric_classification(truth = fishing_binary, 
                                estimate = .pred_class)
  
  # Also calculate ROC AUC by region, which is agnostic to the classification threshold
  region_perf_roc_auc <- predictions_stage_1  %>%
    group_by(region)%>%
    roc_auc(fishing_binary,
            .pred_1,
            event_level = "second")
  
  bind_rows(global_perf,
            global_perf_roc_auc,
            region_perf,
            region_perf_roc_auc)
}

# Function takes in stage 2 regression predictions and truth
# and creates stage 2 regression performance metrics
# for both global (across all pixels), and by MPA region (inside, partial, or outside)
# Specify if calculating for stage 2 or hurdle
# If stage 2, only calculate performance where fishing_hours_per_m2 >0
calculate_performance_stage_2_or_hurdle <- function(predictions_stage_2, type ,multi_metric_regression){
  
  if(type == "stage_2") predictions_stage_2 <- predictions_stage_2 %>%
      filter(fishing_hours_per_m2 >0)
  
  # Calculate global performance, across all pixels regardless of region
  global_perf <- predictions_stage_2 %>%
    multi_metric_regression(truth = fishing_hours_per_m2, 
                            estimate = .pred, 
                            data = .) %>%
    mutate(region = "Global")
  
  
  # Do this by MPA region (inside, partial, or outside)
  region_perf <- predictions_stage_2    %>%
    group_by(region) %>%
    multi_metric_regression(truth = fishing_hours_per_m2, 
                            estimate = .pred, 
                            data = .)
  
  bind_rows(global_perf,
            region_perf)
}

# Get predictions from full hurdle model
# Using stage 1 and stage 2 predictions
# So we get our predictions from stage 1 (which gives us a 0 or 1 for no-fishing or fishing), then we multiply this with numeric regression result from stage 2
calculate_hurdle_predictions <- function(predictions_stage_1,predictions_stage_2){
  
  predictions_stage_1 %>%
    inner_join(predictions_stage_2, 
               by = c("region", "year", "pixel_id")) %>%
    # Convert class prediction to a 0/1 numeric
    mutate(.pred_class = as.character(.pred_class) %>%
             as.numeric()) %>%
    # Multiply class prediction by regression prediction to get full hurdle prediction
    mutate(.pred = .pred * .pred_class) 
  
}
# For all CV folds, fit the model for all hyperparameter combinations
# And add in some columns from the original data
calculate_cv_predictions_stage_1 <- function(cv_folds,training_data,wflow,hyperparameter_grid_size){
  set.seed(101)
  
  tune::tune_grid(wflow,
                  resamples = cv_folds,
                  grid = hyperparameter_grid_size,
                  control = tune::control_grid(save_pred = TRUE,
                                               verbose = TRUE,
                                               allow_par = TRUE,
                                               parallel_over = "everything"))  %>%
    tune::collect_predictions()
}

# For all CV folds, fit the model for all hyperparameter combinations
# And add in some columns from the original data
# This is a special version of the function for stage 2
# That also back-transforms the log predictions
calculate_cv_predictions_stage_2 <- function(cv_folds,training_data,wflow,recipe_stage_2,hyperparameter_grid_size){
  set.seed(101)
  
  cv_fits <- tune::tune_grid(wflow,
                             resamples = cv_folds,
                             grid = hyperparameter_grid_size,
                             control = tune::control_grid(save_pred = TRUE,
                                                          verbose = TRUE,
                                                          allow_par = TRUE,
                                                          parallel_over = "everything",
                                                          extract = extract_fit_parsnip))
  
  
  cv_fits %>%
    # Prep and juice analysis (training) split from each fold
    mutate(analysis_data = map(splits,~analysis(.) %>%
                                 prep(recipe_stage_2,training = .) %>%
                                 juice())) %>%
    # Extract the fit
    mutate(fit = map(.extracts, ~..1$.extracts[[1]])) %>%
    # Calcualte Duan's smearing coefficient, for back-transforming logged data
    mutate(duans_smearing_coefficient = map2(analysis_data, fit,
                                             ~calculate_duans_smearing_coefficient(..1,..2))) %>%
    # Back-transform the logged prediction data
    mutate(predictions = map2(.predictions, duans_smearing_coefficient,
                              ~ ..1 %>%
                                mutate(.pred = exp(.pred) * ..2 %>%
                                         # Use the regression-based smearing coefficient
                                         filter(type == "rmse") %>%
                                         .$coefficient))) %>%
    dplyr::select(id,predictions) %>%
    unnest(predictions) %>%
    dplyr::select(-fishing_hours_per_m2_log) %>%
    # Arrange data by year, which is what time-based CV does
    # Since this is stage 2, filter to just non-zero observations
    # Then get the .row to join on
    left_join(training_data %>% 
                arrange(year) %>%
                filter(fishing_hours_per_m2>0) %>%
                dplyr::select(fishing_hours_per_m2) %>%
                mutate(.row = row_number()),
              by = ".row")
}

# Using the CV predictions from stage 1, calcualte stage 1 performance
# Only use ROC AUC scores, which is what we'll use for optimizing hyperparameters
# ROC AUC is agnostic to the cutoff threshold
calculate_cv_performance_stage_1 <- function(cv_predictions_stage_1){
  
  cv_predictions_stage_1 %>%
    group_by(across(-c(fishing_binary,.pred_1,.pred_0,.row,.pred_class,.config))) %>%
    roc_auc(fishing_binary, 
            .pred_1,
            event_level = "second") %>%
    ungroup() %>%
    group_by(across(-c(id,.metric,.estimator,.estimate)))%>%
    summarize(.estimate = mean(.estimate)) %>%
    ungroup()
}

# Now using the optimized hyperparameters
# Calculate F1 score for range of thresholds
# Calculate mean F1 score from across the 3 regions
calculate_threshold_performance_stage_1 <- function(cv_predictions_stage_1,optimized_hyperparameters_stage_1,multi_metric_classification){
  cv_predictions_stage_1 %>%
    inner_join(optimized_hyperparameters_stage_1)  %>%
    #group_by(region) %>%
    nest() %>%
    ungroup() %>%
    crossing(tibble(threshold = seq(0.1,0.9,0.001))) %>%
    mutate(data = map2(data,threshold, ~ ..1 %>%
                         mutate(.pred_class = ifelse(.pred_1 >= ..2,1,0),
                                .pred_class = factor(.pred_class, levels = c("0","1")))))%>%
    unnest(data)  %>%
    group_by(threshold) %>%
    #group_by(threshold,region) %>%
    multi_metric_classification(truth = fishing_binary,
                                estimate = .pred_class)%>%
    ungroup()
}

# Using the CV predictions from stage 2, calcualte stage 2 performance
# Only use rsq_trad, which is what we'll use for optimizing hyperparameters
calculate_cv_performance_stage_2 <- function(cv_predictions_stage_2){
  # First get rsq_trad for each hyperparameter set and fold
  cv_predictions_stage_2 %>%
    group_by(across(-c(.pred,.row,.config,fishing_hours_per_m2))) %>%
    rsq_trad(truth = fishing_hours_per_m2, 
             estimate = .pred) %>%
    ungroup() %>%
    group_by(across(-c(id,.metric,.estimator,.estimate)))%>%
    summarize(.estimate = mean(.estimate)) %>%
    ungroup()
}

# Take a yardstick confusion matrix object, and turn it into a tibble
# Maniuplate this as they do in the confusion matrix autoplot
# https://github.com/tidymodels/yardstick/blob/master/R/conf_mat.R
conf_mat_to_tibble <- function(conf_mat){
  df <- as.data.frame.table(conf_mat$table)
  
  # Force specific column names for referencing in ggplot2 code
  names(df) <- c("Prediction", "Observed", "Count")
  
  lvls <- levels(df$Prediction)
  df$Prediction <- factor(df$Prediction, levels = rev(lvls))
  
  as_tibble(df)
}

# Function to make global and regional confusion matrices
# From stage 1 predictions
make_all_conf_mat <- function(predictions_stage_1){
  
  conf_mat_global <- predictions_stage_1 %>%
    conf_mat(truth = fishing_binary,
             estimate = .pred_class) %>%
    conf_mat_to_tibble() %>%
    mutate(region = "Global") %>%
    mutate(fraction = Count / sum(Count))
  
  conf_mat_region <-predictions_stage_1  %>%
    group_by(region) %>%
    yardstick::conf_mat(truth = fishing_binary,
                        estimate = .pred_class) %>%
    ungroup() %>%
    mutate(conf_mat = map(conf_mat,~conf_mat_to_tibble(.))) %>%
    unnest(conf_mat) %>%
    mutate(fraction = Count / sum(Count))
  
  conf_mat_global %>%
    bind_rows(conf_mat_region)
  
}

# Wrapper that handles CV, hyperparameter tuning, and testing
training_model_wrapper <- function(df, 
                                   free_cores, 
                                   # Our main model specification is to use random forest ("rf")
                                   # As a test, we will just use logistic classification and linear regression ("lm")
                                   model_type = "rf", 
                                   # Our main model specification is to use all model features as predictors ("all_data")
                                   # As a test, we will just use lagged fishing ("just_lagged_fishing")
                                   data_type = "all_data",
                                   # For out-of-sample testing, should we split temporally ("temporal"),
                                   # or spatiotemporally by time and ocean ("spatiotemporal)
                                   # For building final models, we don't do any splitting and train on the full dataset ("none")
                                   oos_split = "none"){
  # Add log version of main outcome variable
  
  # Allow for novel levels if using random forest - for instance, if training data doesn't have particular EEZ or ocean in testing data
  if(model_type == "rf") hardhat::default_recipe_blueprint(allow_novel_levels = TRUE)
  # lm doesn't like novel levels
  if(model_type == "lm") hardhat::default_recipe_blueprint(allow_novel_levels = FALSE)
  
  # Define performance metrics for classification (stage 1 of hurdle model)
  multi_metric_classification <- yardstick::metric_set(precision,
                                                       recall,
                                                       f_meas)
  
  # Define performance metrics for regression (stage 2 of hurdle model)
  multi_metric_regression <- yardstick::metric_set(rsq_trad, rsq,rmse, nrmse)
  
  # Define pre-processing recipe for stage 1 / classification
  recipe_stage_1_base <- 
    recipes::recipe(formula = fishing_binary ~ ., data = df %>%
                      head()) %>%
    # Don't want fishing_hours_per_m2 on right hand side since that would cause serious data leakage
    recipes::update_role(c(pixel_id,nearest_mpa,fishing_hours_per_m2,pixel_area_m2,fishing_hours_per_m2_log,date,fishing_hours_per_m2_lag), new_role = "id") %>%
    step_impute_median(all_numeric_predictors()) %>%
    # Allow test data to handle previously unseen factor levels  - for instance, if training data doesn't have EEZ in testing data
    step_novel(all_nominal_predictors()) %>%
    step_unknown(all_nominal_predictors()) %>%
    # Create lumped other_eez factor for infrequently occuring eez
    # This will greatly speed up random forest https://stackoverflow.com/a/34721005
    step_other(c(eez_sovereign),threshold = 0.01,other = "other_eez")
  
  # For linear model, make all categorical variables dummies
  # and remove zero-variance variables
  if(model_type == "lm") recipe_stage_1_base <- recipe_stage_1_base  %>%
    step_dummy(all_nominal_predictors()) %>%
    step_zv()
  
  # Our main model specification is to use all model features as predictors
  if(data_type == "all_data") recipe_stage_1 <- recipe_stage_1_base
  
  # As a test, we will just use lagged fishing
  if(data_type == "just_lagged_fishing") recipe_stage_1 <- recipe_stage_1_base%>% 
    update_role(-c("fishing_binary","fishing_hours_per_m2_lag_log"), new_role = "dont_use")
  
  # Define pre-processing recipe for stage 2 / regression
  recipe_stage_2_base <- 
    recipes::recipe(formula = fishing_hours_per_m2_log ~ ., data = df %>%
                      head())  %>%
    # Don't want fishing_binary on right hand side since that would cause serious data leakage
    recipes::update_role(c(pixel_id,nearest_mpa,fishing_binary,pixel_area_m2,fishing_hours_per_m2,date,fishing_hours_per_m2_lag), new_role = "id") %>%
    step_impute_median(all_numeric_predictors()) %>%
    # Allow test data to handle previously unseen factor levels  - for instance, if training data doesn't have EEZ in testing data
    step_novel(all_nominal_predictors()) %>%
    step_unknown(all_nominal_predictors()) %>%
    # Create lumped other_eez factor for infrequently occuring eez
    # This will greatly speed up random forest https://stackoverflow.com/a/34721005
    step_other(c(eez_sovereign),threshold = 0.01,other = "other_eez")
  
  # For linear model, make all categorical variables dummies
  # and remove zero-variance variables
  if(model_type == "lm")recipe_stage_2_base <- recipe_stage_2_base %>%
    step_dummy(all_nominal_predictors()) %>%
    step_zv()
  
  # Our main model specification is to use all model features as predictors
  if(data_type == "all_data") recipe_stage_2 <- recipe_stage_2_base
  
  # As a test, we will just use lagged fishing
  if(data_type == "just_lagged_fishing") recipe_stage_2 <- recipe_stage_2_base %>% 
    update_role(-c("fishing_hours_per_m2_log","fishing_hours_per_m2_lag_log"), new_role = "dont_use")
  
  
  # Create base random forest specification that will use ranger package
  # We'll modify this to be classification or regression below,
  # based on whether we're in stage 1 or stage 2
  ranger_spec <- 
    parsnip::rand_forest(trees = 500,
                         mtry = tune(),
                         min_n = tune()) %>% 
    set_engine("ranger", 
               importance = "none",
               seed = 101,
               num.threads = parallelly::availableCores() - free_cores)
  
  # Our main model specification is to use random forest
  if(model_type == "rf") model_spec_stage_1 <- ranger_spec %>% 
    parsnip::set_mode("classification")
  
  if(model_type == "rf") model_spec_stage_2 <- ranger_spec %>% 
    parsnip::set_mode("regression")
  
  # As a test, we will just use logistic classification and linear regression
  if(model_type == "lm") model_spec_stage_1 <- logistic_reg() %>% 
    set_engine("glm")
  
  if(model_type == "lm") model_spec_stage_2 <- linear_reg() %>%
    set_engine("lm")
  
  # Define stage 1 (classification) workflow
  stage_1_workflow <- workflows::workflow()  %>%
    workflows::add_recipe(recipe_stage_1) %>%
    workflows::add_model(model_spec_stage_1 )
  
  # Define stage 2 (regression) workflow
  stage_2_workflow <- workflows::workflow()  %>%
    workflows::add_recipe(recipe_stage_2)%>%
    workflows::add_model(model_spec_stage_2)
  
  hyperparameter_grid_size <- 10
  
  set.seed(101)
  
  # Register parallel backend for CV hyperparameter tuning
  doFuture::registerDoFuture()
  plan(multisession, workers = parallelly::availableCores() - free_cores)
  
  test_year = max(df$year)
  
  # Here we partition our dataset for out-of-sample performance assessment by each ocean
  # And testing performance in final year (so spatiotemporal OOS testing)
  if(oos_split == "spatiotemporal") df <- df %>%
    distinct(ocean) %>%
    filter(!is.na(ocean)) %>%
    rename(testing_ocean = ocean) %>%
    mutate(training_data = map(testing_ocean,
                               ~ df %>%
                                 filter(ocean != ..1,
                                        year < test_year))) %>%
    mutate(testing_data = map(testing_ocean,
                              ~ df %>%
                                filter(ocean == ..1,
                                       year == test_year)))
  
  # Here we partition our dataset for testing performance in final year
  if(oos_split == "temporal") df <- tibble(training_data = list(df %>%
                                                                  filter(year < test_year))) %>%
    mutate(testing_data = list(df %>%
                                 filter(year == test_year)))
  
  # If we're making the final model fits, we use the entire dataset, across all oceans and time,
  # With no need for separate training or testing datasets
  if(oos_split == "none") df <- df%>%
    nest() %>%
    rename(training_data = data)
  
  # If we're not building the final model and are doing OOS testing:
  # We want to first do CV on the training dataset for hyperparameter tuning, 
  # then train the model on the training dataset and optimized hyperparameters,
  # then make predictions on the testing dataset and assess performance
  if(model_type == "rf" & oos_split != "none") results <- df %>%
    # Summarize training dataset size, by region 
    mutate(training_data_size = map(training_data, . %>%
                                      group_by(region) %>% 
                                      summarize(n_pixel_years = n(),
                                                n_distinct_mpas = n_distinct(nearest_mpa)) %>%
                                      ungroup()))  %>%
    # Summarize testing dataset size, by region
    mutate(testing_data_size = map(testing_data, . %>%
                                     group_by(region) %>% 
                                     summarize(n_pixel_years = n(),
                                               n_distinct_mpas = n_distinct(nearest_mpa)) %>%
                                     ungroup()))%>%
    # Extract unique training and testing years
    mutate(training_testing_years = map2(training_data,testing_data,
                                         ~get_training_testing_years(..1,..2)))  %>%
    # Now make stage-1 cross-validation splits for hyperparameter tuning
    # These are time-based folds, where the assessment set is always the last year, to avoid data leakage
    mutate(cv_folds_stage_1 = map(training_data, ~ make_time_based_cv_splits(.))) %>%
    # For each CV fold, extract the analysis and assessment years
    # These are the same years for both stage 1 and stage 2
    mutate(cv_years = map(cv_folds_stage_1,~get_cv_analysis_assessment_years(.))) %>%
    # Now make predictions for stage 1 CV across all hyperparameter combinations
    mutate(cv_predictions_stage_1 = map2(cv_folds_stage_1,training_data,
                                         ~calculate_cv_predictions_stage_1(..1,..2,stage_1_workflow,hyperparameter_grid_size))) %>%
    # Calculate ROC AUC performance for all hyperparameters - ROC AUC is agnostic to threshold
    mutate(cv_performance_stage_1 = map(cv_predictions_stage_1,
                                        ~calculate_cv_performance_stage_1(.)))%>%
    # Pick the hyperparamaters that maximize ROC AUC
    mutate(optimized_hyperparameters_stage_1 = map(cv_performance_stage_1,
                                                   .  %>%
                                                     slice_max(.estimate, n = 1, with_ties = FALSE))) %>%
    # Now that we have the optimized hyperparameters, we'll pick the optimal threshold for maximizing F1 score
    # First calculate F1 score for range of thresholds
    mutate(threshold_performance_stage_1 = map2(cv_predictions_stage_1, optimized_hyperparameters_stage_1,
                                                ~calculate_threshold_performance_stage_1(..1,..2,multi_metric_classification))) %>%
    # Don't need these any more
    dplyr::select(-c(cv_folds_stage_1,cv_predictions_stage_1)) %>%
    # Then pick threshold that maximizes F1 score
    mutate(optimized_threshold_stage_1 = map(threshold_performance_stage_1,
                                             . %>%
                                               filter(.metric == "f_meas") %>%
                                               slice_max(.estimate, n = 1, with_ties = FALSE))) %>%
    # Now make stage 2 cross-validation splits for hyperparameter tuning
    # Only use training data where fishing_hours_per_m2 >0, since this is stage 2
    mutate(cv_folds_stage_2 = map(training_data, ~ make_time_based_cv_splits(..1 %>%
                                                                               filter(fishing_hours_per_m2 >0)))) %>%
    # Now make predictions for stage 1 CV across all hyperparameter combinations
    mutate(cv_predictions_stage_2 = map2(cv_folds_stage_2,training_data,
                                         ~calculate_cv_predictions_stage_2(..1,..2,stage_2_workflow,recipe_stage_2,hyperparameter_grid_size)))  %>%
    # Figure out optimal hyperparameters for stage 2, to maximize rsq_trad
    mutate(cv_performance_stage_2 = map(cv_predictions_stage_2,
                                        ~calculate_cv_performance_stage_2(.)))  %>%
    # Figure out optimal hyperparameters for stage 2, to maximize rsq_trad
    mutate(optimized_hyperparameters_stage_2 = map(cv_performance_stage_2,
                                                   . %>%
                                                     slice_max(.estimate, n = 1, with_ties = FALSE))) %>%
    # Don't need these any more
    dplyr::select(-c(cv_folds_stage_2,cv_predictions_stage_2)) %>%
    # Add stage 1 fit, using optimized hyperparameters
    mutate(fit_stage_1 = map2(training_data, optimized_hyperparameters_stage_1, 
                              ~fit(stage_1_workflow  %>%
                                     finalize_workflow(..2),data=..1)) %>%
             # Can simplify fit - all we'll need to use it for is making predictions
             butcher::butcher()) %>%
    # Add stage 1 predictions. First, calculate probability that it is 0 or 1
    mutate(predictions_stage_1 = map2(fit_stage_1,testing_data, 
                                      ~predict(..1, new_data = ..2, type = "prob") %>%
                                        # Include truth columns
                                        dplyr::bind_cols(..2 %>%
                                                           dplyr::select(region,fishing_binary,year,pixel_id)))) %>%
    dplyr::select(-fit_stage_1) %>%
    # Classify stage 1 predictions, using optimized cutoff
    mutate(predictions_stage_1 = map2(predictions_stage_1,optimized_threshold_stage_1,
                                      ~add_classification_to_stage_1(..1,..2$threshold)))  %>%
    # Add stage 1 ROC curves
    mutate(roc_curve_stage_1 = map(predictions_stage_1,
                                   ~roc_curve(.,fishing_binary,
                                              .pred_1,
                                              event_level = "second"))) %>%
    # Add stage 1 confusion matrix
    mutate(confusion_matrix_stage_1 = map(predictions_stage_1,
                                          ~make_all_conf_mat(.))) %>%
    # Add stage 1 classification performance
    mutate(performance_stage_1 = map(predictions_stage_1,~calculate_performance_stage_1(.,
                                                                                        multi_metric_classification))) %>%
    # Add stage 2 fit, which only uses observations with fishing effort, using optimized hyperparameters
    # Only train using dta where fishing_hours_per_m2 >0
    mutate(fit_stage_2 = map2(training_data ,
                              optimized_hyperparameters_stage_2,
                              ~fit(stage_2_workflow %>%
                                     finalize_workflow(..2), 
                                   data= ..1 %>%
                                     filter(fishing_hours_per_m2 >0))) %>%
             # Can simplify fit - all we'll need to use it for is making predictions
             butcher::butcher()) %>%
    # Add stage 2 regression predictions
    mutate(predictions_stage_2 = map2(fit_stage_2,testing_data, 
                                      ~predict(..1, new_data = ..2) %>%
                                        # Include truth columns
                                        dplyr::bind_cols(..2 %>%
                                                           dplyr::select(region,fishing_hours_per_m2,pixel_area_m2,year,pixel_id))))  %>%
    # Calculate Duan's smearing coefficient just using training data, for back-transforming logged data
    mutate(duans_smearing_coefficient = map2(training_data, fit_stage_2,
                                             ~calculate_duans_smearing_coefficient(..1,..2)))%>%
    # Back-transform the logged data
    mutate(predictions_stage_2 = map2(predictions_stage_2, duans_smearing_coefficient,
                                      ~ ..1 %>%
                                        mutate(.pred = exp(.pred) * ..2 %>%
                                                 # Use the regression-based smearing coefficient
                                                 filter(type == "rmse") %>%
                                                 .$coefficient)))%>%
    # Don't need this anymore
    dplyr::select(-c(fit_stage_2,duans_smearing_coefficient)) %>%
    # Add stage 2 classification performance
    # Only calculate performance where fishing_hours_per_m2 > 0 
    mutate(performance_stage_2 = map(predictions_stage_2, 
                                     ~calculate_performance_stage_2_or_hurdle(.,type = "stage_2",
                                                                              multi_metric_regression)))
  
  # Same thing as above, but for logistic and linear models, where there is no need for CV and hyperparameter tuning
  if(model_type == "lm" & oos_split != "none") results <- df    %>%
    # Add stage 1 fit, using optimized hyperparameters
    mutate(fit_stage_1 = map(training_data, 
                             ~fit(stage_1_workflow, data = ..1)) %>%
             # Can simplify fit - all we'll need to use it for is making predictions
             butcher::butcher()) %>%
    # Add stage 1 predictions. First, calculate probability that it is 0 or 1
    mutate(predictions_stage_1 = map2(fit_stage_1,testing_data, 
                                      ~predict(..1, new_data = ..2, type = "prob") %>%
                                        # Include truth columns
                                        dplyr::bind_cols(..2 %>%
                                                           dplyr::select(region,fishing_binary,year,pixel_id))))%>%
    # Classify stage 1 predictions, using cutoff of 0.5
    mutate(predictions_stage_1 = map(predictions_stage_1,
                                     ~add_classification_to_stage_1(..1, 0.5))) %>%
    dplyr::select(-fit_stage_1) %>%
    # Add stage 1 classification performance
    mutate(performance_stage_1 = map(predictions_stage_1,~calculate_performance_stage_1(.,
                                                                                        multi_metric_classification))) %>%
    # Add stage 2 fit, which only uses observations with fishing effort, using optimized hyperparameters
    # Only train using dta where fishing_hours_per_m2 >0
    mutate(fit_stage_2 = map(training_data,
                             ~fit(stage_2_workflow, 
                                  data= ..1 %>%
                                    filter(fishing_hours_per_m2 >0))) %>%
             # Can simplify fit - all we'll need to use it for is making predictions
             butcher::butcher()) %>%
    # Add stage 2 regression predictions
    mutate(predictions_stage_2 = map2(fit_stage_2,testing_data, 
                                      ~predict(..1, new_data = ..2) %>%
                                        # Include truth columns
                                        dplyr::bind_cols(..2 %>%
                                                           dplyr::select(region,fishing_hours_per_m2,pixel_area_m2,year,pixel_id))))  %>%
    # Calculate Duan's smearing coefficient just using training data, for back-transforming logged data
    mutate(duans_smearing_coefficient = map2(training_data, fit_stage_2,
                                             ~calculate_duans_smearing_coefficient(..1,..2)))%>%
    # Back-transform the logged data
    mutate(predictions_stage_2 = map2(predictions_stage_2, duans_smearing_coefficient,
                                      ~ ..1 %>%
                                        mutate(.pred = exp(.pred) * ..2 %>%
                                                 # Use the regression-based smearing coefficient
                                                 filter(type == "rmse") %>%
                                                 .$coefficient)))%>%
    # Don't need this anymore
    dplyr::select(-c(fit_stage_2,duans_smearing_coefficient)) %>%
    # Add stage 2 classification performance
    # Only calculate performance where fishing_hours_per_m2 > 0 
    mutate(performance_stage_2 = map(predictions_stage_2, 
                                     ~calculate_performance_stage_2_or_hurdle(.,type = "stage_2",
                                                                              multi_metric_regression)))
  
  # Use all data for building final models
  # Don't need to assess performance now, but still need to do CV for hyperparameter tuning
  if(oos_split == "none") results <- df %>%
    # Now make stage-1 cross-validation splits for hyperparameter tuning
    # These are time-based folds, where the assessment set is always the last year, to avoid data leakage
    mutate(cv_folds_stage_1 = map(training_data, ~ make_time_based_cv_splits(.))) %>%
    # Now make predictions for stage 1 CV across all hyperparameter combinations
    mutate(cv_predictions_stage_1 = map2(cv_folds_stage_1,training_data,
                                         ~calculate_cv_predictions_stage_1(..1,..2,stage_1_workflow,hyperparameter_grid_size))) %>%
    # Calculate ROC AUC performance for all hyperparameters - ROC AUC is agnostic to threshold
    mutate(cv_performance_stage_1 = map(cv_predictions_stage_1,
                                        ~calculate_cv_performance_stage_1(.)))%>%
    # Pick the hyperparamaters that maximize ROC AUC
    mutate(optimized_hyperparameters_stage_1 = map(cv_performance_stage_1,
                                                   .  %>%
                                                     slice_max(.estimate, n = 1, with_ties = FALSE))) %>%
    # Now that we have the optimized hyperparameters, we'll pick the optimal threshold for maximizing F1 score
    # First calculate F1 score for range of thresholds
    mutate(threshold_performance_stage_1 = map2(cv_predictions_stage_1, optimized_hyperparameters_stage_1,
                                                ~calculate_threshold_performance_stage_1(..1,..2,multi_metric_classification))) %>%
    # Don't need these any more
    dplyr::select(-c(cv_folds_stage_1,cv_predictions_stage_1,cv_performance_stage_1)) %>%
    # Then pick threshold that maximizes F1 score
    mutate(optimized_threshold_stage_1 = map(threshold_performance_stage_1,
                                             . %>%
                                               filter(.metric == "f_meas") %>%
                                               slice_max(.estimate, n = 1, with_ties = FALSE))) %>%
    # Now make stage 2 cross-validation splits for hyperparameter tuning
    # Only use training data where fishing_hours_per_m2 >0, since this is stage 2
    mutate(cv_folds_stage_2 = map(training_data, ~ make_time_based_cv_splits(..1 %>%
                                                                               filter(fishing_hours_per_m2 >0)))) %>%
    # Now make predictions for stage 1 CV across all hyperparameter combinations
    mutate(cv_predictions_stage_2 = map2(cv_folds_stage_2,training_data,
                                         ~calculate_cv_predictions_stage_2(..1,..2,stage_2_workflow,recipe_stage_2,hyperparameter_grid_size)))  %>%
    # Figure out optimal hyperparameters for stage 2, to maximize rsq_trad
    mutate(cv_performance_stage_2 = map(cv_predictions_stage_2,
                                        ~calculate_cv_performance_stage_2(.)))  %>%
    # Figure out optimal hyperparameters for stage 2, to maximize rsq_trad
    mutate(optimized_hyperparameters_stage_2 = map(cv_performance_stage_2,
                                                   . %>%
                                                     slice_max(.estimate, n = 1, with_ties = FALSE))) %>%
    # Don't need these any more
    dplyr::select(-c(cv_folds_stage_2,cv_predictions_stage_2,cv_performance_stage_2)) %>%
    # Add stage 1 fit, using optimized hyperparameters
    mutate(final_fit_stage_1 = map2(training_data, optimized_hyperparameters_stage_1, 
                                    ~fit(stage_1_workflow  %>%
                                           finalize_workflow(..2),data=..1)) %>%
             # Can simplify fit - all we'll need to use it for is making predictions
             butcher::butcher()) %>%
    # Add stage 2 fit, which only uses observations with fishing effort, using optimized hyperparameters
    # Only train using dta where fishing_hours_per_m2 >0
    mutate(final_fit_stage_2 = map2(training_data ,
                                    optimized_hyperparameters_stage_2,
                                    ~fit(stage_2_workflow  %>%
                                           finalize_workflow(..2), 
                                         data= ..1 %>%
                                           filter(fishing_hours_per_m2 >0))) %>%
             # Can simplify fit - all we'll need to use it for is making predictions
             butcher::butcher()) %>%
    # Calculate Duan's smearing coefficient just using training data, for back-transforming logged data
    mutate(duans_smearing_coefficient = map2(training_data, final_fit_stage_2,
                                             ~calculate_duans_smearing_coefficient(..1,..2)))
  return(results)
}

# Function that takes our training dataset as the main input
# And then modifies the dataset for each forecast horizon
# And then uses training_model_wrapper to train the model for each forecast horizon
# The output is an object with all of the model fits, performance, etc
train_all_models <- function(training_dataset, 
                             free_cores = 0, 
                             # Our main model specification is to use random forest ("rf")
                             # As a test, we will just use logistic classification and linear regression ("lm")
                             model_type = "rf", 
                             # Our main model specification is to use all model features as predictors ("all_data")
                             # As a test, we will just use lagged fishing ("just_lagged_fishing")
                             data_type = "all_data",
                             # For out-of-sample testing, should we split temporally ("temporal"),
                             # or spatiotemporally by time and ocean ("spatiotemporal)
                             # For building final models, we don't do any splitting and train on the full dataset ("none")
                             oos_split = "none"){
  
  joined_dt <- training_dataset  %>%
    mutate(fishing_binary = factor(fishing_binary, levels = c("0","1"))) %>%
    # Add date column (which is just the first of the year) for time-based CV
    mutate(date = lubridate::date(date))
  
  # Start with full training data
  trained_models <- joined_dt %>%
    # Nest training data into a column
    nest(data = everything()) %>%
    # Now create separate rows for each forecast_horizon
    crossing(tibble(forecast_horizon = seq(1,3))) %>%
    # Now use those forecast_horizon to create training data
    mutate(data = map2(data,forecast_horizon,
                       ~create_future_dataset(..1,..2))) %>%
    mutate(training_results = map(data, ~training_model_wrapper(.,
                                                                free_cores = free_cores,
                                                                model_type = model_type,
                                                                data_type = data_type,
                                                                oos_split = oos_split),
                                  .progress = TRUE)) %>%
    unnest(training_results)
}

run_simulations <- function(simulation_scenarios, trained_models){
  
  simulation_results <- simulation_scenarios    %>%
    # Make this factor, so predictions work
    mutate(across(c(fishing_binary),~as.factor(.))) %>%
    # Make this date column, so predictions work
    mutate(date = lubridate::date(date)) %>% 
    group_by(scenario,mpa_coverage) %>%
    nest() %>% 
    ungroup() %>% 
    rename(simulation_data = data) %>%
    mutate(results = map(simulation_data,
                         ~calculate_simulation_predictions(.,trained_models))) %>%
    dplyr::select(-simulation_data) %>%
    unnest(results) %>% 
    unnest(hurdle_predictions) %>%
    # Turn area-normalized fishing_hours back to regular fishing hours
    mutate(.pred = .pred * pixel_area_m2)
}


# Function takes a simulation dataset (e.g., 1 year of data with relevant MPA network features)
# As well as the tibble of trained stage 1 and stage 2 models
# And returns stage 1 and hurdle predictions

calculate_simulation_predictions <- function(simulation_data,trained_models){
  
  trained_models %>%
    # Add stage 1 predictions. First, calculate probability that it is 0 or 1
    mutate(predictions_stage_1 = map(final_fit_stage_1, 
                                     ~predict(..1, new_data = simulation_data %>% 
                                                # Make logical columns into factors
                                                mutate(across(where(is.logical),~as.factor(.))), type = "prob") %>%
                                       # Include truth columns
                                       dplyr::bind_cols(simulation_data %>%
                                                          dplyr::select(pixel_id,region,year))))  %>%
    # Remove model fit to save memory
    dplyr::select(-final_fit_stage_1)  %>%
    # Classify stage 1 predictions, using optimized cutoff
    mutate(predictions_stage_1 = map2(predictions_stage_1,optimized_threshold_stage_1,
                                      ~add_classification_to_stage_1(..1,..2$threshold)))%>%
    # Add stage 2 regression predictions
    mutate(predictions_stage_2 = map(final_fit_stage_2, 
                                     ~predict(..1, new_data = simulation_data %>% 
                                                # Make logical columns into factors
                                                mutate(across(where(is.logical),~as.factor(.)))) %>%
                                       # Include truth columns
                                       dplyr::bind_cols(simulation_data %>%
                                                          dplyr::select(pixel_id,pixel_area_m2,region,year))))%>%
    # Back-transform the logged data
    mutate(predictions_stage_2 = map2(predictions_stage_2, duans_smearing_coefficient,
                                      ~ ..1 %>%
                                        mutate(.pred = exp(.pred) * ..2 %>%
                                                 # Use the regression-based smearing coefficient
                                                 filter(type == "rmse") %>%
                                                 .$coefficient))) %>%
    # Add hurdle predictions, which combine stage 1 and stage 2 predictions
    mutate(hurdle_predictions = map2(predictions_stage_1, predictions_stage_2,
                                     ~calculate_hurdle_predictions(..1,..2))) %>%
    dplyr::select(forecast_horizon, hurdle_predictions)
}

prep_simulation_data <- function(simulation_data,scenario_name,simulation_data_counterfactual){
  simulation_data  %>%
    # Don't need these
    dplyr::select(-c(pixel_area_m2,area_mpa_overlap_m2)) %>%
    # Create region category
    # Classify anything with full spatial overlap as inside MPAs
    # Anything with 0 spatial overlap is outside
    # Everything else is partial
    mutate(region = case_when(near(fraction_mpa_overlap, 1) ~ "Inside MPAs",
                              near(fraction_mpa_overlap, 0)  ~ "Outside MPAs",
                              TRUE ~ "Partial MPA overlap"))%>%
    mutate(one_year_before_full_mpa = FALSE, 
           two_years_before_full_mpa = FALSE, 
           one_year_before_partial_mpa = FALSE, 
           two_years_before_partial_mpa = FALSE) %>%
    inner_join(simulation_data_counterfactual %>%
                 dplyr::select(-contains("mpa"),-region),
               by = "pixel_id") %>%
    group_by(mpa_coverage) %>%
    nest() %>%
    ungroup() %>%
    rename(simulation_data = data) %>%
    mutate(scenario = scenario_name)
}

# Calculate MPA coverage for simulation data
calculate_mpa_coverage <- function(new_mpa_dt){
  new_mpa_dt  %>%
    mutate(mpa_area_m2 = pixel_area_m2 * fraction_mpa_overlap) %>%
    summarize(mpa_area_m2 = sum(mpa_area_m2,na.rm=TRUE),
              ocean_area = sum(pixel_area_m2,na.rm=TRUE))%>%
    mutate(mpa_global_coverage = mpa_area_m2/ocean_area) %>%
    .$mpa_global_coverage
}

# Calculate Duan's smearing coefficient, for back-transforming
# Logged predictions to level predictions
calculate_duans_smearing_coefficient <- function(training_data,fit_stage_2){
  
  predictions <- predict(fit_stage_2,
                         training_data)
  # First option: https://stats.stackexchange.com/a/361632
  unadjusted_prediction <- predictions %>%
    mutate(.pred = exp(.pred))
  
  smearing_coefficient_regression <- lm(fishing_hours_per_m2 ~ .pred ,
                                        data = training_data %>%
                                          bind_cols(unadjusted_prediction)%>%
                                          filter(fishing_hours_per_m2>0)) %>%
    broom::tidy() %>%
    filter(term == ".pred") %>%
    .$estimate
  
  # Second option https://stats.stackexchacalculate_simulation_predictionsnge.com/a/49868
  rmse_training <- yardstick::rmse(data = training_data %>%
                                     bind_cols(predictions) %>%
                                     filter(fishing_hours_per_m2>0),
                                   truth = fishing_hours_per_m2_log,
                                   estimate = .pred) %>%
    .$.estimate
  
  smearing_coefficient_rmse <- exp((rmse_training^2)/2)
  
  # Save both types of coefficients
  tibble(type = c("regression","rmse"),
         coefficient = c(smearing_coefficient_regression,smearing_coefficient_rmse))
}

# Make cross-validation splits based on time
# Where we always assess/test using last year of data, and use years before that for analysis/training
make_time_based_cv_splits <- function(training_data){
  # How many unique years are in training data?
  number_unique_years <- length(unique(training_data$year))
  
  # How many years should be in analysis split? At least 1, and upwards of the number of years - 5
  initial_years <- max(1,number_unique_years - 5)
  
  training_data  %>%
    time_series_cv(initial     = initial_years,
                   # Always assess using last year of data
                   assess      = "1 year",
                   skip        = "1 year",
                   cumulative  = FALSE,
                   # We want upwards of 5 folds
                   slice_limit = 5)
}

# For each CV fold, extract the unique analysis and assessment years
get_cv_analysis_assessment_years <- function(cv_folds){
  cv_folds %>%
    mutate(analysis = map(splits,~analysis(.) %>%
                            distinct(year)),
           assessement = map(splits,~assessment(.) %>%
                               distinct(year))) %>%
    dplyr::select(-splits) %>%
    pivot_longer(-id) %>%
    unnest(value)
}

# Extract unique training and testing years
get_training_testing_years <- function(training_data,testing_data){
  training_data %>%
    distinct(year) %>%
    mutate(name = "training") %>%
    bind_rows(testing_data %>%
                distinct(year) %>%
                mutate(name = "testing"))
}

make_training_dataset <- function(eez_features,
                                  enso_index,
                                  errdap_chl,
                                  errdap_sst,
                                  fuel_price_data,
                                  gfi,
                                  gfw_fishing_by_pixel_year,
                                  gfw_reception_quality,
                                  gfw_static_spatial_measures,
                                  data_grid,
                                  mesopelagic_regions,
                                  mpa_features,
                                  oceans,
                                  pdo_index,
                                  remss_wind,
                                  seamounts){
  
  gfw_effort_data <- gfw_fishing_by_pixel_year %>% 
    inner_join(data_grid,
               by = c("lat","lon")) %>%
    mutate(fishing_hours_per_m2 = fishing_hours / pixel_area_m2,
           fishing_kw_hours_per_m2 = fishing_kw_hours / pixel_area_m2) %>%
    dplyr::select(-c(pixel_area_m2,lat,lon)) %>% 
    # Complete zeros for all possible pixel-year combinations
    complete(pixel_id,year,fill=list(fishing_hours = 0,
                                     fishing_kw_hours = 0,
                                     fishing_hours_per_m2 = 0,
                                     fishing_kw_hours_per_m2 = 0))  %>%
    # Restrict analysis to 2016+, to eliminate confounding issues of increasing AIS coverage and quality in earlier years
    filter(year >= 2016)
  
  # Add columns, by pixel and year, that indicate if there is a new MPA
  # implemented in the following year or two years
  # Do this for both full and partial MPAs, creating 4 new separate columns total
  
  # For pixels that ever have a newly implemented full MPA,
  # Figure out which year the MPA is implemented, and the id of the MPA
  # Only use MPA that was implemented first
  mpa_years_first_designation_full <- mpa_features %>%
    filter(years_since_mpa_designation == 0,
           fraction_mpa_overlap == 1) %>%
    dplyr::select(pixel_id,future_full_mpa = nearest_mpa,year) %>%
    # Only take earliest MPA
    group_by(pixel_id) %>%
    slice_min(order_by = year,
              with_ties = FALSE) %>%
    ungroup()
  
  # For each pixel, now figure out year before future MPAs are implemented
  mpa_one_year_before_full <- mpa_years_first_designation_full %>%
    dplyr::select(-future_full_mpa)%>%
    mutate(year = year -1,
           one_year_before_full_mpa = TRUE)
  
  # For each pixel, now figure two years before future MPAs are implemented
  mpa_two_years_before_full <- mpa_years_first_designation_full %>%
    dplyr::select(-future_full_mpa) %>%
    mutate(year = year - 2,
           two_years_before_full_mpa = TRUE)
  
  # For pixels that ever have a newly implemented partial MPA,
  # Figure out which year the MPA is implemented, and the id of the MPA
  # Only use MPA that was implemented first
  mpa_years_first_designation_partial <- mpa_features %>%
    filter(years_since_mpa_designation == 0,
           fraction_mpa_overlap > 0 & fraction_mpa_overlap < 1) %>%
    dplyr::select(pixel_id,future_partial_mpa = nearest_mpa,year) %>%
    # Only take earliest MPA
    group_by(pixel_id) %>%
    slice_min(order_by = year,
              with_ties = FALSE) %>%
    ungroup()
  
  # For each pixel, now figure out year before future MPAs are implemented
  mpa_one_year_before_partial <- mpa_years_first_designation_partial %>%
    dplyr::select(-future_partial_mpa)%>%
    mutate(year = year -1,
           one_year_before_partial_mpa = TRUE)
  
  # For each pixel, now figure two years before future MPAs are implemented
  mpa_two_years_before_partial <- mpa_years_first_designation_partial %>%
    dplyr::select(-future_partial_mpa) %>%
    mutate(year = year - 2,
           two_years_before_partial_mpa = TRUE)
  
  # Now add this information back to the tibble
  mpa_dynamic_spatial_measures<-mpa_features  %>%
    dplyr::select(-c(pixel_area_m2,area_mpa_overlap_m2,oldest_mpa)) %>%
    left_join(mpa_one_year_before_full, by = c("pixel_id","year"))%>%
    left_join(mpa_two_years_before_full, by = c("pixel_id","year"))%>%
    left_join(mpa_one_year_before_partial, by = c("pixel_id","year"))%>%
    left_join(mpa_two_years_before_partial, by = c("pixel_id","year")) %>%
    # If there are missing values, they are FALSE
    mutate(across(c(one_year_before_full_mpa,two_years_before_full_mpa,one_year_before_partial_mpa,two_years_before_partial_mpa),~replace_na(.,FALSE)))  %>%
    # Create region category
    # Classify anything with full spatial overlap as inside MPAs
    # Anything with 0 spatial overlap is outside
    # Everything else is partial
    mutate(region = case_when(fraction_mpa_overlap == 1 ~ "Inside MPAs",
                              fraction_mpa_overlap == 0  ~ "Outside MPAs",
                              TRUE ~ "Partial MPA overlap")) %>%
    # If there is any overlap, assign nearest MPA distance to 0
    mutate(nearest_mpa_distance_m = ifelse(fraction_mpa_overlap>0,
                                           0,
                                           nearest_mpa_distance_m))%>%
    # If there is any overlap, assign nearest_years_since_mpa_designation to years_since_mpa_designation
    mutate(nearest_years_since_mpa_designation = ifelse(fraction_mpa_overlap>0,
                                                        years_since_mpa_designation,
                                                        nearest_years_since_mpa_designation)) %>%
    # Don't keep this, just nearest
    # This is because this is NA for pixels without MPAs
    dplyr::select(-years_since_mpa_designation)
  
  mpa_dynamic_spatial_measures <- mpa_dynamic_spatial_measures %>%
    # For all distinct pixels, add blank MPA measures for 2021 and 2022
    # Since we don't have mpa data for those years
    bind_rows(tibble(pixel_id = unique(mpa_dynamic_spatial_measures$pixel_id)) %>%
                crossing(tibble(year = c(2021))))
  
  mesopelagic_regions <- mesopelagic_regions %>%
    mutate(mesopelagic_zone = replace_na(mesopelagic_zone,"missing"))
  
  static_spatial_measures <- gfw_static_spatial_measures %>%
    dplyr::select(-c(lat,lon,pixel_area_m2))
  
  
  seamount_static_spatial_measures <- seamounts%>%
    dplyr::select(-nearest_seamount_id)
  
  joined_dt <- data_grid %>%
    sf::st_set_geometry(NULL) %>%
    # Only use pixels that have MPA data - this will eliminate landlocked pixels, which we don't want
    inner_join(mpa_dynamic_spatial_measures,
               by = "pixel_id") %>%
    left_join(gfw_effort_data,
              by = c("pixel_id","year")) %>%
    # Where there is missing GFW data, replace with 0. This will cover the whole ocean
    mutate(across(c(fishing_hours,fishing_kw_hours,fishing_hours_per_m2,fishing_kw_hours_per_m2),~replace_na(.,0))) %>%
    #dplyr::select(-fishing_hours) %>%
    # Add factor for whether or not there was fishing, for stage 1 / classification
    mutate(fishing_binary = ifelse(fishing_hours >0, 1, 0))%>%
    left_join(gfw_reception_quality,
              by = c("lat","lon")) %>% 
    left_join(static_spatial_measures,
              by = "pixel_id")  %>%
    left_join(oceans,
              by = "pixel_id") %>%
    left_join(eez_features,
              by = "pixel_id") %>%
    left_join(gfi,
              by = "eez_sovereign") %>%
    # If area is in high seas, give it a high seas GFI score
    # If GFI is missing, give it "no data" score
    mutate(gfi_fisheries_governance_capacity = ifelse(eez_sovereign == "high_seas","high_seas",gfi_fisheries_governance_capacity)) %>%
    mutate(gfi_fisheries_governance_capacity = as.character(gfi_fisheries_governance_capacity))%>%
    mutate(gfi_fisheries_governance_capacity = replace_na(gfi_fisheries_governance_capacity,"no_data")) %>%
    left_join(seamount_static_spatial_measures,
              by = "pixel_id") %>%
    left_join(mesopelagic_regions,
              by = "pixel_id") %>%
    left_join(errdap_sst,
              by = c("pixel_id","year"))%>%
    left_join(errdap_chl,
              by = c("pixel_id","year"))%>%
    left_join(remss_wind,
              by = c("pixel_id","year")) %>%
    left_join(enso_index,
              by = c("year")) %>%
    left_join(pdo_index,
              by = c("year")) %>%
    left_join(fuel_price_data,
              by = c("year"))%>%
    # Add date column (which is just the first of the year) for time-based CV
    mutate(date = lubridate::mdy(paste0("01-01-",year))) %>%
    # Remove these other version of the outcome variable we don't need
    dplyr::select(-c(fishing_hours,fishing_kw_hours,fishing_kw_hours_per_m2,geometry_wkt))
}

# Combine hypothetical MPA network datasets, 
# which contain only MPA-based features,
# and create full datasets with all model features
make_full_simulation_data <- function(training_dataset,
                                      simulation_mpa_features_unfished,
                                      simulation_mpa_features_most_fished,
                                      simulation_mpa_features_ebsa,
                                      simulation_mpa_features_visalli,
                                      simulation_mpa_features_sala_multi_objective,
                                      simulation_mpa_features_sala_biodiversity,
                                      simulation_mpa_features_sala_carbon,
                                      simulation_mpa_features_sala_food,
                                      simulation_mpa_features_random){
  # Load full training dataset, generated in 06_data_wranggling_combine_datasets_simplified.Rmd
  joined_dt <- training_dataset  %>%
    mutate(across(c(fishing_binary),~as.factor(.))) %>%
    # Add date column (which is just the first of the year) for time-based CV
    mutate(date = lubridate::date(date))
  
  # Calculate minimum non-zero fishing value
  # We will add this divided by 2 to lagged fishing before logging it, so we don't get infinite values
  min_non_zero_fishing <- joined_dt %>%
    filter(fishing_hours_per_m2 > 0) %>%
    .$fishing_hours_per_m2 %>%
    min()
  # Create counterfactual dataset
  # We will run predictions on this and use it as our "counterfactual"
  # against which all other simulations will be compared
  simulation_data_counterfactual <-  joined_dt %>%
    # Use 2021 model features (everything except MPA-based features)
    filter(year == 2021) %>%
    dplyr::select(-c(contains("mpa"),region)) %>%
    # Combine these with actual observed 2020 MPA features - so assume no new MPAs went in
    inner_join(joined_dt %>%
                 filter(year == 2020) %>%
                 dplyr::select(c(contains("mpa"),region,pixel_id)),
               by = "pixel_id") %>%
    # Add logged fishing columns
    mutate(fishing_hours_per_m2_lag = fishing_hours_per_m2,
           fishing_hours_per_m2_log = log(fishing_hours_per_m2),
           fishing_hours_per_m2_lag_log = log(fishing_hours_per_m2_lag + min_non_zero_fishing / 2))
  
  # Load Sala data
  # We have 4 different sala objective layers, so read and process them each
  simulation_data_sala <-  tibble(scenario_name = c("sala_multi_objective",
                                                    "sala_biodiversity",
                                                    "sala_carbon",
                                                    "sala_food")) %>%
    mutate(simulation_data = list(simulation_mpa_features_sala_multi_objective,
                                  simulation_mpa_features_sala_biodiversity,
                                  simulation_mpa_features_sala_carbon,
                                  simulation_mpa_features_sala_food)) %>%
    mutate(simulation_data = map2(simulation_data,scenario_name, ~prep_simulation_data(..1,
                                                                                       scenario_name = ..2,
                                                                                       simulation_data_counterfactual))) %>%
    unnest(simulation_data) %>%
    dplyr::select(-scenario_name)
  
  # Next load networks that cover previously unfished areas
  simulation_data_unfished <- simulation_mpa_features_unfished %>%
    prep_simulation_data(scenario_name = "unfished",
                         simulation_data_counterfactual)
  
  # Next load networks that cover previously most-fished areas
  simulation_data_most_fished <- simulation_mpa_features_most_fished %>%
    prep_simulation_data(scenario_name = "most_fished",
                         simulation_data_counterfactual)
  
  # Next load networks that are from expert-driven EBSA process
  simulation_data_ebsa <- simulation_mpa_features_ebsa %>%
    prep_simulation_data(scenario_name = "ebsa",
                         simulation_data_counterfactual)
  
  # Next load networks that are from Visalli et al.
  simulation_data_visalli <- simulation_mpa_features_visalli %>%
    prep_simulation_data(scenario_name = "visalli",
                         simulation_data_counterfactual)
  
  # Next load random networks
  simulation_data_random <- simulation_mpa_features_random %>%
    prep_simulation_data(scenario_name = "random",
                         simulation_data_counterfactual)
  
  # Make tibble of different simulation scenarios
  # Each row will have a scenario name, mpa_coverage, as well as the data necessary for the simulation
  simulation_data_counterfactual %>%
    nest(data = everything())%>%
    rename(simulation_data = data) %>%
    mutate(scenario = "counterfactual") %>%
    bind_rows(simulation_data_sala) %>%
    bind_rows(simulation_data_unfished)%>%
    bind_rows(simulation_data_most_fished)%>%
    bind_rows(simulation_data_ebsa)%>%
    bind_rows(simulation_data_visalli) %>%
    bind_rows(simulation_data_random) %>%
    unnest(simulation_data)
}


# Function to generate Shapley contribution values for each feature
generate_shapley_contributions <- function(models,
                                           # Do this for Stage 1 or 2
                                           stage_1_or_2,
                                           # Do this for 1 year forecast horizon by default
                                           which_forecast_horizon = 1,
                                           # How many free cores to use when running in parallel?
                                           free_cores = 0)
{
  
  # First, select the relevant model based on the forecast_horizon and stage 1 or 2
  if(stage_1_or_2 == 1)  models <- models  %>%
      filter(forecast_horizon == which_forecast_horizon)%>%
      dplyr::select(final_fit_stage_1,
                    data) %>%
      dplyr::select(fit = 1, data)
  
  if(stage_1_or_2 == 2)  models <- models  %>%
      filter(forecast_horizon == which_forecast_horizon)%>%
      dplyr::select(final_fit_stage_2,
                    data)%>%
      dplyr::select(fit = 1, data)
  
  # Extract the model fit
  model_fit <- models$fit[[1]]
  
  # Extract the data pre-processing recipe
  preprocessor <- models$fit[[1]] %>%
    extract_recipe()
  
  # Process data according to the recipe
  df <- preprocessor %>%
    bake(models$data[[1]])
  
  # We extract these from the model pre-processing
  predictors <- preprocessor %>%
    .$var_info %>% 
    filter(role == "predictor") %>% 
    .$variable
  
  # Get outcome variable
  outcome <- preprocessor %>%
    .$var_info %>% 
    filter(role == "outcome") %>% 
    .$variable
  
  # If doing Stage 2 explainer, filter to only observations with >0 lagged effort
  if(stage_1_or_2 == 2) df <- df %>%
    filter(fishing_hours_per_m2 > 0)
  
  # Set up parallel backend - see https://stackoverflow.com/a/76773377
  options(doFuture.rng.onMisuse = "ignore")  # To suppress some warning on random seeds
  
  doFuture::registerDoFuture()
  plan(multisession, workers = parallelly::availableCores() - free_cores)
  #cl <- parallel::makeCluster(parallelly::availableCores() - free_cores)
  #plan(cluster, workers = cl)
  
  set.seed(101)
  
  # Step 1: Calculate Kernel SHAP values.  
  kernelshap(model_fit, 
             # Select subset of rows to create Shapley values for
             df %>%
               dplyr::select(-any_of(outcome)) %>%
               slice_sample(n = 1000),
             # Select rows to represent background
             bg_X = df %>%
               dplyr::select(-any_of(outcome)) %>%
               slice_sample(n = 500),
             feature_names = predictors,
             # For stage 1, prediction type is probability. Otherwise, numeric
             type = ifelse(stage_1_or_2 == 1, "prob", "numeric"),
             parallel = TRUE,
             verbose = TRUE,
             parallel_args = list(.packages = c("tidymodels",
                                                "ranger")))
  
}

# Create custom nrmse function to be used with yardstick
# https://yardstick.tidymodels.org/articles/custom-metrics.html
# Normalized RMSE is simply RMSE divided by the SD of the observed data
nrmse_vec <- function(truth, estimate, na_rm = TRUE, ...) {
  
  nrmse_impl <- function(truth, estimate) {
    rmse_vec(truth, estimate) / sd(truth)
  }
  
  metric_vec_template(
    metric_impl = nrmse_impl,
    truth = truth, 
    estimate = estimate,
    na_rm = na_rm,
    cls = "numeric",
    ...
  )
  
}

nrmse <- function(data, ...) {
  UseMethod("nrmse")
}

nrmse <- yardstick::new_numeric_metric(nrmse, direction = "minimize")

nrmse.data.frame <- function(data, truth, estimate, na_rm = TRUE, ...) {
  
  metric_summarizer(
    metric_nm = "nrmse",
    metric_fn = nrmse_vec,
    data = data,
    truth = !! enquo(truth),
    estimate = !! enquo(estimate), 
    na_rm = na_rm,
    ...
  )
  
}

# For each pixel in data grid, calculate distance raster to all points in ocean
make_distance_rasters <- function(pixel_id_tmp,data_grid){
  
  # Create raster of ocean
  ocean_raster <- terra::ext(data_grid)%>% 
    # make a raster with the same extent, and assign all cells to value of 1
    terra::rast(nrows = 180, ncols = 360, crs = sf::st_crs(data_grid)[1]$input, vals = 1) %>% 
    # set all cells that are land as a value of NA
    terra::mask(terra::vect(data_grid), updatevalue = NA)
  
  # Get centroid coordinates of pixel
  test_pixel <- data_grid %>% 
    dplyr::filter(pixel_id == pixel_id_tmp) %>%
    sf::st_centroid() %>%
    sf::st_coordinates()
  
  # now, find the ocean raster cell that cooresponds to that pixel's centroid
  test_pixel_cell <- ocean_raster %>%
    terra::cellFromXY(test_pixel)
  
  # assign that cell a value of 3
  terra::values(ocean_raster)[test_pixel_cell] <- 3
  
  # Calculate distances
  result <- ocean_raster %>%
    terra::gridDist(target = 3, scale=1) %>%
    # Convert to raster, for saving to disc
    raster::raster()
  
  return(result)
  
}

make_projected_global_grid <- function(data_grid,analysis_projection){
  data_grid %>%
    sf::st_as_sf(wkt = "geometry_wkt",
                 crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")%>% 
    # Wrap around dataline
    sf::st_wrap_dateline(options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"), quiet = TRUE) %>% 
    # Project to analysis projection
    sf::st_transform(analysis_projection) %>%
    # Add area of each pixel
    mutate(pixel_area_m2 = sf::st_area(geometry_wkt)%>%
             units::drop_units())
}

make_distance_rasters_wrapper <- function(data_grid, free_cores, analysis_projection){
  
  plan(multisession, workers = parallelly::availableCores() - free_cores)
  
  # Create distance raster for each pixel id
  data_grid %>%
    dplyr::select(pixel_id) %>%
    sf::st_set_geometry(NULL) %>%
    mutate(distance_raster = furrr::future_map(pixel_id,
                                               ~make_distance_rasters(.,data_grid), 
                                               .options = furrr::furrr_options(seed = 101),.progress=TRUE))
}

make_mpa_features <- function(mpa,
                              data_grid,
                              distance_raster,
                              neighbors){
  
  tibble(year = seq(2016,2020)) %>%
    mutate(results = map(year,function(year_tmp){
      
      # Filter to just those MPAs that have been implemented before this year
      mpa %>%
        dplyr::filter(status_yea <= year_tmp)  %>%
        dplyr::mutate(years_since_mpa_designation = year_tmp - status_yea) %>%
        generate_mpa_data(data_grid,
                          distance_raster,
                          neighbors)
    })) %>%
    unnest(results)
}

make_eez_features <- function(eez,
                              data_grid,
                              distance_raster_stack,
                              analysis_projection){
  
  eez <- eez %>%
    filter(POL_TYPE !=  "Overlapping claim") %>%
    dplyr::select(eez_sovereign = ISO_SOV1) %>%
    # Don't include Antarctica - classify it as high seas instead
    filter(eez_sovereign != "ATA")  %>% 
    sf::st_wrap_dateline(options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"), quiet = TRUE) %>% 
    sf::st_transform(analysis_projection) %>%
    sf::st_make_valid() %>%
    mutate(eez_area_m2 = sf::st_area(geom)%>%
             units::drop_units())
  
  # Getworld back regions for each sovereign EEZ
  eez_region_features <- eez %>%
    sf::st_set_geometry(NULL) %>%
    mutate(eez_region_world_bank_7 = countrycode::countrycode(eez_sovereign,"iso3c","region"))%>%
    distinct(eez_sovereign,eez_region_world_bank_7) 
  
  # Now intersect our grids with EEZs, calculate fraction each grid overlaps with EEZ
  # And only pick one EEZ per grid
  data_grid_eez <- data_grid %>% 
    sf::st_intersection(eez) %>%
    mutate(area_eez_overlap_m2 = sf::st_area(geometry_wkt)%>%
             units::drop_units())  %>%
    sf::st_drop_geometry() %>%
    mutate(fraction_eez_overlap = (area_eez_overlap_m2 / pixel_area_m2) %>%
             pmax(0) %>%
             pmin(1))%>%
    group_by(pixel_id) %>%
    slice_max(area_eez_overlap_m2, n = 1, with_ties = FALSE)%>%
    # In cases when there are ties, take largest EEZ
    ungroup() %>%
    dplyr::select(pixel_id,eez_sovereign,fraction_eez_overlap)
  
  # Which pixel ids have no overlap, i.e. are on the high-seas? We only't need to calculate nearest neighbors for these
  high_seas_pixels <- data_grid %>%
    filter(!(pixel_id %in% data_grid_eez$pixel_id)) %>%
    .$pixel_id
  
  high_seas_pixels_vec <- glue::glue("X{high_seas_pixels}")
  
  
  
  # Calculate distance between each grid centroid and nearest eez
  # Only do this for grids on the high seas that don't already overlap with EEZs - this is all that's necessary, and speeds things up computationally as well
  distance_to_nearest_eez <- find_shortest_distance_from_pixel_to_shapefile(distance_raster_stack[[high_seas_pixels_vec]],
                                                                            eez,
                                                                            "eez_sovereign") %>%
    rename(nearest_eez_sovereign = eez_sovereign,
           nearest_eez_distance_m = distance_m)
  
  data_grid %>%
    dplyr::select(pixel_id) %>%
    sf::st_set_geometry(NULL) %>%
    left_join(data_grid_eez,
              by = "pixel_id") %>%
    left_join(distance_to_nearest_eez,
              by = "pixel_id")%>%
    left_join(eez_region_features,
              by = "eez_sovereign")%>%
    # Replace missing values with high_seas
    mutate(across(c(nearest_eez_distance_m,fraction_eez_overlap),~replace_na(.,0)))%>%
    # Replace missing values with high_seas
    mutate(across(c(eez_sovereign,eez_region_world_bank_7),~replace_na(.,"high_seas")))
}

# Input is a distance rasterStack, a shapefile over which to aggregate,
# And a column ID from the shapefile for identifying the polygon that is closest
find_shortest_distance_from_pixel_to_shapefile <- function(distance_raster_stack, shapefile, column_id){
  # Let's spatially aggregate by taking the min value from the rasterStack for each of our polygons 
  distance_raster_stack %>%
    # Raster stack can then be fed to exact_extract
    exactextractr::exact_extract(shapefile,
                                 "min",
                                 append_cols = column_id,
                                 progress = TRUE) %>%
    tibble::as_tibble() %>%
    pivot_longer(-c(column_id)) %>%
    rename(pixel_id = name,
           distance_m = value)%>%
    mutate(pixel_id = stringr::str_remove_all(pixel_id,"min.X") %>%
             as.numeric()) %>%
    group_by(pixel_id) %>%
    # Select the specific polygon that has the minimum distance
    dplyr::slice_min(order_by = distance_m, n = 1, with_ties = FALSE) %>%
    ungroup()
}

find_neighbors <- function(data_grid){
  # For each pixel in data_grid, find first neighboring pixels
  first_neighbors <- st_join(data_grid %>%
                               dplyr::select(pixel_id = pixel_id,
                                             lat = lat,
                                             lon = lon),
                             data_grid%>%
                               dplyr::select(neighbor_1 = pixel_id,
                                             lat_neighbor_1 = lat,
                                             lon_neighbor_1 = lon),
                             join=st_touches) %>%
    sf::st_set_geometry(NULL) %>%
    as_tibble()
  
  # For each pixel in data_grid, find second neighboring pixels (neighbors of first neighbors)
  second_neighbors <- first_neighbors %>%
    left_join(first_neighbors %>%
                dplyr::select(neighbor_1 = pixel_id,
                              lat_neighbor_1 = lat,
                              lon_neighbor_1 = lon,
                              neighbor_2 = neighbor_1,
                              lat_neighbor_2 = lat_neighbor_1,
                              lon_neighbor_2 = lon_neighbor_1),
              by = c("neighbor_1", "lat_neighbor_1", "lon_neighbor_1"))
  
  # For each pixel, just take distinct neighbor 1 and neighbor 2
  neighbor_df <- second_neighbors %>%
    distinct(pixel_id,neighbor_1,neighbor_2)
}

generate_mpa_data <- function(mpa_shapefile,data_grid,distance_raster_stack,neighbors,old_mpa_distances=NULL){
  
  data_grid_mpa <- data_grid %>% 
    sf::st_intersection(mpa_shapefile) %>%
    # Get area of each MPA overlap
    dplyr::mutate(area_mpa_overlap_m2 = sf::st_area(geometry_wkt)%>%
                    units::drop_units())  %>%
    tibble::as_tibble()
  
  # Figure out oldest MPA per pixel
  # That will be our MPA, and give us years_since_mpa_designation
  oldest_mpa_per_pixel <- data_grid_mpa %>%
    dplyr::group_by(pixel_id) %>%
    slice_min(n = 1,
              order_by = status_yea,
              with_ties = FALSE) %>%
    ungroup() %>%
    dplyr::select(pixel_id,oldest_mpa = mpa_id, years_since_mpa_designation) %>%
    # Add fraction of year with MPA for oldest MPA
    left_join(mpa_shapefile %>%
                dplyr::select(oldest_mpa = mpa_id,
                              fraction_year_with_mpa) %>%
                sf::st_set_geometry(NULL) ,
              by = "oldest_mpa")
  
  data_grid_mpa_summary <- data_grid_mpa %>%
    group_by(pixel_id,pixel_area_m2) %>%
    summarize(# Now, since some MPAs overlap, take union to get total area of overlapping MPA
      area_mpa_overlap_m2 = st_union(geometry_wkt) %>%
        sf::st_area()%>%
        units::drop_units()) %>%
    ungroup()%>%
    dplyr::mutate(fraction_mpa_overlap = (area_mpa_overlap_m2 / pixel_area_m2) %>%
                    # Ensure it's not greater than 1 and not less than 0
                    pmin(1) %>%
                    pmax(0)) %>%
    left_join(oldest_mpa_per_pixel, by ="pixel_id") %>%
    # We'll want to generate MPA features for all pixels, not just those with MPAs
    full_join(data_grid %>%
                st_set_geometry(NULL) %>%
                distinct(pixel_id,pixel_area_m2),
              by = c("pixel_id","pixel_area_m2")) %>%
    # For pixels without MPAs, fill in these columns with 0s
    mutate(across(c(area_mpa_overlap_m2,fraction_mpa_overlap),
                  ~replace_na(.,0)))
  
  
  # If we don't have access to old MPA distances, do exact_extract across all polygons
  # Otherwise, just do exact_extract calculation for new MPA polygons
  if(is.null(old_mpa_distances)) mpa_shapefile$new_mpa <- TRUE
  
  # Calculate distance between each grid centroid and nearest MPA
  distance_to_mpas_df <- find_shortest_distance_from_pixel_to_shapefile(distance_raster_stack,
                                                                        mpa_shapefile %>%
                                                                          # Only need to do this calculation for new MPAs
                                                                          filter(new_mpa),
                                                                        c("mpa_id","years_since_mpa_designation")) %>%
    rename(nearest_mpa = mpa_id,
           nearest_mpa_distance_m = distance_m,
           nearest_years_since_mpa_designation = years_since_mpa_designation)
  
  # If old distances are available, can just take minimum distance for each pixel to nearest MPA
  # Comparing old and new distances
  if(!is.null(old_mpa_distances)) distance_to_mpas_df <- distance_to_mpas_df %>%
    bind_rows(old_mpa_distances) %>%
    group_by(pixel_id) %>%
    # Select the specific polygon that has the minimum distance
    dplyr::slice_min(order_by = nearest_mpa_distance_m, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  
  # Find fraction_mpa_overlap in nearest_1 set of neighboring pixels
  neighbor_mpa_1 <- neighbors %>%
    left_join(data_grid_mpa_summary %>% dplyr::rename(neighbor_1 = pixel_id),
              by = "neighbor_1") %>%
    dplyr::mutate(area_mpa_overlap_m2 = pixel_area_m2*fraction_mpa_overlap) %>%
    dplyr::group_by(pixel_id) %>%
    dplyr::summarize(pixel_area_m2 = sum(pixel_area_m2,na.rm=TRUE),
                     area_mpa_overlap_m2 = sum(area_mpa_overlap_m2,na.rm=TRUE)) %>%
    dplyr::ungroup()%>%
    dplyr::mutate(fraction_mpa_overlap_neighbor_1 = area_mpa_overlap_m2 / pixel_area_m2,
                  # Ensure it's not greater than 1
                  fraction_mpa_overlap_neighbor_1 = pmin(fraction_mpa_overlap_neighbor_1,1)) %>% 
    dplyr::mutate(across(everything(), ~replace_na(.x, 0))) %>%
    dplyr::select(-c(pixel_area_m2,area_mpa_overlap_m2))
  
  # Find fraction_mpa_overlap in nearest_2 set of neighboring pixels
  neighbor_mpa_2 <- neighbors %>%
    dplyr::left_join(data_grid_mpa_summary %>% dplyr::rename(neighbor_2 = pixel_id),
                     by = "neighbor_2") %>%
    dplyr::mutate(area_mpa_overlap_m2 = pixel_area_m2*fraction_mpa_overlap) %>%
    dplyr::group_by(pixel_id) %>%
    dplyr::summarize(pixel_area_m2 = sum(pixel_area_m2,na.rm=TRUE),
                     area_mpa_overlap_m2 = sum(area_mpa_overlap_m2,na.rm=TRUE)) %>%
    dplyr::ungroup()%>%
    dplyr::mutate(fraction_mpa_overlap_neighbor_2 = area_mpa_overlap_m2 / pixel_area_m2,
                  # Ensure it's not greater than 1
                  fraction_mpa_overlap_neighbor_2 = pmin(fraction_mpa_overlap_neighbor_2,1)) %>% 
    dplyr::mutate(across(everything(), ~replace_na(.x, 0))) %>%
    dplyr::select(-c(pixel_area_m2,area_mpa_overlap_m2))
  
  new_mpa_dt <- data_grid_mpa_summary  %>%
    left_join(distance_to_mpas_df, by = "pixel_id") %>%
    left_join(neighbor_mpa_1, by = "pixel_id") %>%
    left_join(neighbor_mpa_2, by = "pixel_id") %>%
    mutate(fraction_mpa_overlap = replace_na(fraction_mpa_overlap,
                                             0))%>%
    # When fraction_year_with_mpa==1 (e.g., Jan 1 implementation) keep this as 1
    # When there's any MPA coverage and it's been implemented at least a year, give this a 1
    # Otherwise  when there's any MPA coverage,  use actual value
    # Otherwise, give it a 0
    mutate(fraction_year_with_mpa = case_when(fraction_year_with_mpa == 1 ~ 1,
                                              fraction_mpa_overlap > 0 & years_since_mpa_designation > 0 ~ 1,
                                              fraction_mpa_overlap > 0 & years_since_mpa_designation == 0~ fraction_year_with_mpa,
                                              TRUE ~ 0))
  
}

# Pull and wrangle ENSO data from NOAA
# https://psl.noaa.gov/data/correlation/oni.data
# Calculate annual mean and SD for our time period of interest
wrangle_enso_data <- function(){
  read.table(url("https://psl.noaa.gov/data/correlation/oni.data"),skip=1,nrows=72) %>%
    as_tibble() %>%
    rename(year = V1) %>%
    pivot_longer(-year) %>%
    mutate(month = stringr::str_remove_all(name,"V") %>%
             as.numeric() - 1,
           date = lubridate::ymd(glue::glue("{year}-{month}-1")),
           year = lubridate::year(date)) %>%
    dplyr::select(year,enso_index = value) %>%
    filter(year >= 2016,
           year <= 2021)%>%
    group_by(year) %>%
    summarize(across(everything(),list(mean = ~mean(.,na.rm=TRUE),
                                       sd = ~sd(.,na.rm=TRUE)))) %>%
    ungroup()
}

# Pull and wrangle PDO data from NOAA
# ttps://psl.noaa.gov/data/climateindices/list/
# Calculate annual mean and SD for our time period of interest
wrangle_pdo_data <- function(){
  read.table(url("https://psl.noaa.gov/data/correlation/pdo.data"),skip=1,nrows=74) %>%
    as_tibble() %>%
    rename(year = V1) %>%
    pivot_longer(-year) %>%
    mutate(month = stringr::str_remove_all(name,"V") %>%
             as.numeric() - 1,
           date = lubridate::ymd(glue::glue("{year}-{month}-1")),
           year = lubridate::year(date)) %>%
    dplyr::select(year,pdo_index = value) %>%
    filter(year >= 2016,
           year <= 2021)%>%
    mutate(pdo_index = ifelse(pdo_index == -9.9,NA_real_,pdo_index))%>%
    group_by(year) %>%
    summarize(across(everything(),list(mean = ~mean(.,na.rm=TRUE),
                                       sd = ~sd(.,na.rm=TRUE)))) %>%
    ungroup()
}

# Wrange fuel price data
#We download these data from [Bunker Index](https://bunkerindex.com/prices/bix-world.php). We focus our analysis on The BIX World IFO 380, which "is the calculated daily average for IFO 380 worldwide, covering all ports with IFO 380 prices listed in the Bunker Index prices section. Prices are in US$ per metric tonne." We aggregate the data to the mean fuel price per month.

#According to [Sala et al. 2018](https://www.science.org/doi/10.1126/sciadv.aat2504), "96% of the world’s fishing fleet installed engine power uses MDO [marine diesel oil]". And according to [Mabanaft](https://www.mabanaft.com/en/news-info/glossary/details/term/marine-diesel-oil-mdo-intermediate-fuel-oil-ifo.html), "Marine diesel oil is sometimes also used synonymously with the term intermediate fuel oil (IFO). Intermediate fuel oil (IFO): Marine diesel with higher proportions of heavy fuel oil."

wrangle_fuel_price_data <- function(fuel_price_data_raw){
  fuel_price_data_raw %>%
    mutate(date = lubridate::mdy(Date),
           year = lubridate::year(date)) %>%
    dplyr::select(year,
                  fuel_price_usd_mt = Price) %>%
    filter(year>=2016,
           year<=2021)%>%
    group_by(year) %>%
    summarize(across(everything(),list(mean = ~mean(.,na.rm=TRUE),
                                       sd = ~sd(.,na.rm=TRUE)))) %>%
    ungroup()
}

# Wrangle mesopelagic zones from Marine Regions
# https://www.marineregions.org/gazetteer.php?p=details&id=50384
wrangle_mesopelagic_zones <- function(mesopelagic_regions,
                                      global_grid,
                                      analysis_projection){
  mp_zones <- mesopelagic_regions %>%
    dplyr::select(mesopelagic_zone = provname) %>% 
    st_wrap_dateline(options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"), quiet = TRUE) %>% 
    st_transform(analysis_projection) %>%
    st_make_valid()
  
  mp_zones_data_grid <- global_grid %>%
    st_join(mp_zones,
            largest = TRUE) %>%
    st_set_geometry(NULL) %>%
    dplyr::select(pixel_id,mesopelagic_zone) %>%
    as_tibble()
}

# Wrangle seamounts data
# Calculate nearest distance from the centroid of each pixel to each seamount
wrangle_seamounts <- function(seamounts,
                              global_grid,
                              analysis_projection){
  seamounts <- seamounts %>%
    dplyr::select(seamount_id = PeakID,
                  geometry)%>% 
    st_wrap_dateline(options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"), quiet = TRUE) %>% 
    st_transform(analysis_projection)
  
  nngeo::st_nn(global_grid %>%
                 st_centroid(),
               seamounts, 
               # Only select single nearest eez
               k = 1, 
               returnDist = T,
               parallel = 1) %>%# floor(parallel::detectCores()/4))  %>% 
    as_tibble() %>% 
    mutate(nearest_seamount_id =  seamounts$seamount_id[as.numeric(nn)], 
           nearest_seamount_distance_m = as.numeric(dist)) %>%
    dplyr::select(-nn,-dist) %>%
    bind_cols(global_grid %>%
                st_set_geometry(NULL) %>%
                dplyr::select(pixel_id))
}

# Make MPA network data for hypothetical MPA shapefile
make_mpa_network <- function(new_mpa_shapefile,
                             existing_mpas,
                             training_dataset,
                             distance_raster_stack,
                             neighbors,
                             data_grid,
                             analysis_projection,
                             network_name){
  
  # Take 2020 network as counterfactual BAU starting point
  network_2020 <- training_dataset %>%
    filter(year == 2020) %>%
    dplyr::select(pixel_id,nearest_mpa_distance_m,nearest_mpa)
  
  new_mpa_shapefile %>% 
    # Wrap around dataline
    sf::st_wrap_dateline(options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180")) %>%
    sf::st_transform(analysis_projection)%>%
    sf::st_make_valid() %>%
    # Union separate polygons into a single polygon
    sf::st_union() %>%
    # Turn it back into sf, which must be done after unioning
    sf::st_as_sf() %>%
    #These are new networks, so set columns accordingly
    mutate(years_since_mpa_designation = 0,
           fraction_year_with_mpa = 1,
           status_yea = 2021,
           mpa_id = network_name) %>%
    rename(geometry = x) %>%
    # We will calcualte new distances for the new network
    mutate(new_mpa = TRUE) %>%
    # Add existing 2020 MPAs
    # These are included when generating features 
    bind_rows(existing_mpas %>%
                filter(status_yea <= 2020) %>%
                mutate(years_since_mpa_designation = 2021 - status_yea) %>%
                mutate(new_mpa = FALSE) %>%
                rename(geometry = geom)) %>%
    # We will compare these with distances from the old network
    generate_mpa_data(data_grid, 
                      distance_raster_stack,
                      neighbors,
                      network_2020) %>%
    nest(data = everything()) %>%
    # Add calculated global MPA coverage
    mutate(mpa_coverage = map_dbl(data,~calculate_mpa_coverage(.)) %>%
             round(2)) %>%
    unnest(data)
  
}

# Function specific for Sala networks
# Start by ranking priotity pixels
# Then select which pixels will become MPAs for a certain global size
# This wraps around make_mpa_network
make_sala_mpa_network <- function(priority_data_stars,
                                  training_dataset,
                                  existing_mpas,
                                  distance_raster_stack,
                                  neighbors,
                                  data_grid,
                                  analysis_projection,
                                  network_name,
                                  mpa_network_scenario_sizes){
  # Take 2020 network as counterfactual BAU starting point
  network_2020 <- training_dataset %>%
    filter(year == 2020)
  
  total_area <- sum(network_2020$pixel_area_m2)
  
  tibble(
    mpa_coverage = mpa_network_scenario_sizes
  ) %>%
    mutate(results = map(mpa_coverage,function(mpa_coverage){
      
      priority_data_stars  %>%
        sf::st_as_sf() %>%
        sf::st_transform(analysis_projection) %>%
        # Arrange raster in descending priority, to close them off from highest to lowest priority
        dplyr::select(priority = 1) %>%
        arrange(-priority)%>%
        mutate(pixel_area_m2 = sf::st_area(geometry)%>%
                 units::drop_units())%>%
        # If we protect pixels one at a time started from the top,
        # what is the cumulative area covered
        mutate(area_covered_fraction = cumsum(pixel_area_m2) / total_area) %>%
        # Keep labeled pixels as new_mpa until we hit the desired mpa_coverage
        mutate(new_mpa = ifelse(area_covered_fraction<=mpa_coverage,TRUE,FALSE)) %>%
        filter(new_mpa) %>%
        make_mpa_network(existing_mpas,
                         training_dataset,
                         distance_raster_stack,
                         neighbors,
                         data_grid,
                         analysis_projection,
                         network_name)%>%
        # Don't need the calculated coverage column, can just use nominal coverage
        dplyr::select(-mpa_coverage)
    })) %>%
    unnest(results)
  
}

# Make random MPA networks
make_random_mpa_network <- function(training_dataset,
                                    existing_mpas,
                                    distance_raster_stack,
                                    neighbors,
                                    data_grid,
                                    analysis_projection,
                                    network_name,
                                    mpa_network_scenario_sizes){
  # Generate datasets for numbers mpa_coverage
  set.seed(101)
  # Take 2020 network as counterfactual BAU starting point
  network_2020 <- training_dataset %>%
    filter(year == 2020) %>%
    inner_join(data_grid %>%
                 dplyr::select(pixel_id),by="pixel_id") %>%
    sf::st_as_sf()
  
  mpa_area_2020_m2 <- sum(network_2020$fraction_mpa_overlap * network_2020$pixel_area_m2)
  
  total_area <- sum(network_2020$pixel_area_m2)
  
  network_2020 %>%
    # Close pixels that don't have any MPA coverage
    filter(fraction_mpa_overlap == 0) %>%
    # Randomly shuffle unfished/no-MPA rows, and then randomly close them off as MPAs
    slice_sample(n = nrow(.), replace = FALSE) %>%
    # If we protect pixels one at a time started from the top,
    # what is the cumulative area covered
    mutate(area_covered_fraction = (cumsum(pixel_area_m2) + mpa_area_2020_m2) / total_area) %>%
    nest(data = everything()) %>%
    crossing(mpa_coverage = mpa_network_scenario_sizes) %>%
    mutate(results = map2(data,mpa_coverage,function(data,mpa_coverage){
      data  %>%
        # Keep labeled pixels as new_mpa until we hit the desired mpa_coverage
        mutate(new_mpa = ifelse(area_covered_fraction<=mpa_coverage,TRUE,FALSE)) %>%
        filter(new_mpa)  %>%
        make_mpa_network(existing_mpas,
                         training_dataset,
                         distance_raster_stack,
                         neighbors,
                         data_grid,
                         analysis_projection,
                         network_name) %>%
        dplyr::select(-mpa_coverage)})) %>%
    dplyr::select(-data) %>%
    unnest(results)
}

# Make most-fished MPA networks
make_most_fished_mpa_network <- function(training_dataset,
                                         existing_mpas,
                                         distance_raster_stack,
                                         neighbors,
                                         data_grid,
                                         analysis_projection,
                                         network_name,
                                         mpa_network_scenario_sizes){
  # Generate datasets for numbers mpa_coverage
  set.seed(101)
  # Take 2020 network as counterfactual BAU starting point
  network_2020 <- training_dataset %>%
    filter(year == 2020) %>%
    inner_join(data_grid %>%
                 dplyr::select(pixel_id),by="pixel_id") %>%
    sf::st_as_sf()
  
  total_area <- sum(network_2020$pixel_area_m2)
  
  # Find pixels that are unfished or partial MPAs
  # We will close other pixels, based on most fished
  unfished_or_partial_mpa_areas <- network_2020 %>%
    filter(fishing_hours_per_m2 == 0 | fraction_mpa_overlap>0)
  
  # What is unfished area or area partially covered by MPAs?
  unfished_or_partial_mpa_area_m2 <- sum(unfished_or_partial_mpa_areas$pixel_area_m2 * unfished_or_partial_mpa_areas$fraction_mpa_overlap)
  
  tibble(
    mpa_coverage = mpa_network_scenario_sizes
  ) %>%
    mutate(results = map(mpa_coverage,function(mpa_coverage){
      network_2020 %>%
        # Close most-fished areas that don't have any MPA coverage, in order of how fished they were in 2020
        filter(fishing_hours_per_m2 > 0 & fraction_mpa_overlap == 0) %>%
        arrange(-fishing_hours_per_m2) %>%
        # If we protect pixels one at a time started from the top,
        # what is the cumulative area covered
        mutate(area_covered_fraction = (cumsum(pixel_area_m2) + unfished_or_partial_mpa_area_m2) / total_area) %>%
        # Keep labeled pixels as new_mpa until we hit the desired mpa_coverage
        mutate(new_mpa = ifelse(area_covered_fraction<=mpa_coverage,TRUE,FALSE)) %>%
        filter(new_mpa) %>%
        make_mpa_network(existing_mpas,
                         training_dataset,
                         distance_raster_stack,
                         neighbors,
                         data_grid,
                         analysis_projection,
                         network_name) %>%
        dplyr::select(-mpa_coverage)})) %>%
    unnest(results)
}

# Make unfished MPA networks
make_unfished_mpa_network <- function(training_dataset,
                                      existing_mpas,
                                      distance_raster_stack,
                                      neighbors,
                                      data_grid,
                                      analysis_projection,
                                      network_name,
                                      mpa_network_scenario_sizes){
  # Generate datasets for numbers mpa_coverage
  set.seed(101)
  # Take 2020 network as counterfactual BAU starting point
  network_2020 <- training_dataset %>%
    filter(year == 2020) %>%
    inner_join(data_grid %>%
                 dplyr::select(pixel_id),by="pixel_id") %>%
    sf::st_as_sf()
  
  total_area <- sum(network_2020$pixel_area_m2)
  
  fished_or_partial_mpa_areas <- network_2020 %>%
    filter(fishing_hours_per_m2 > 0 | fraction_mpa_overlap>0)
  
  # What is area either fished or partially covered by MPAs?
  fished_or_partial_mpa_m2 <- sum(fished_or_partial_mpa_areas$pixel_area_m2 * fished_or_partial_mpa_areas$fraction_mpa_overlap)
  
  tibble(
    mpa_coverage = mpa_network_scenario_sizes
  ) %>%
    mutate(results = map(mpa_coverage,function(mpa_coverage){
      network_2020 %>%
        filter(fishing_hours_per_m2 == 0 & fraction_mpa_overlap == 0) %>%
        # Randomly shuffle unfished/no-MPA rows, and then close them off as MPAs until we reach desired target
        slice_sample(n = nrow(.), replace = FALSE) %>%
        # If we protect pixels one at a time started from the top,
        # what is the cumulative area covered
        mutate(area_covered_fraction = (cumsum(pixel_area_m2) + fished_or_partial_mpa_m2) / total_area) %>%
        # Keep labeled pixels as new_mpa until we hit the desired mpa_coverage
        mutate(new_mpa = ifelse(area_covered_fraction<=mpa_coverage,TRUE,FALSE))  %>%
        filter(new_mpa) %>%
        make_mpa_network(existing_mpas,
                         training_dataset,
                         distance_raster_stack,
                         neighbors,
                         data_grid,
                         analysis_projection,
                         network_name) %>%
        dplyr::select(-mpa_coverage)})) %>%
    unnest(results)
}