library(readr)
library(dplyr)  
library(fs) 
library(readxl)
library(tidyverse)

# Set the main directory path
main_dir <- "C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/Trade_margin"

# Set the output directory where processed CSV files will be saved
output_dir <- "C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/Trade_margin/Processed_Margin_Data"
dir_create(output_dir)

# Import the labels file (containing column and row names)
labels <- read_excel("C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/Index.xlsx")

# Define the column to use as column names
sector_column <- "EORA 26 label"

if (!sector_column %in% colnames(labels)) {
  stop(paste("The specified column", sector_column, "is not found in the labels file."))
}
colnames_list <- labels[[sector_column]]

process_csv_files <- function(file_path) {
  # Read CSV file
  data <- read_csv(file_path, col_names = FALSE)
  
  # Assign proper column names using labels
  colnames(data) <- paste0(labels$Country, "_", colnames_list) 
  
  file_name <- basename(file_path)
  
  # Select and filter relevant data
  data <- data %>%
    bind_cols(labels) %>% 
    select(Country, CountryA3, Entity, `EORA 26 label`, `Industry OECD code`, 
           matches("Agriculture|Fishing|Food & Beverages|Agriculture, Fishing")) %>%
    filter(`EORA 26 label` %in% c("Agriculture", "Fishing", "Food & Beverages", "Agriculture, Fishing"))
  
  # Summarize data by EORA 26 label, Country, and CountryA3
  summarized_data <- data %>%
    group_by(`EORA 26 label`, Country, CountryA3) %>%
    summarize(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop")
  
  # Rename column for clarity
  summarized_data <- summarized_data %>%
    rename(EORA_26_Label = `EORA 26 label`)
  
  # Combine Agriculture and Fishing into a single category per country
  agriculture_fishing_data <- summarized_data %>%
    filter(EORA_26_Label %in% c("Agriculture", "Fishing", "Agriculture, Fishing")) %>%
    group_by(Country, CountryA3) %>%
    summarize(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(EORA_26_Label = "Agriculture and Fishing") %>% 
    select(EORA_26_Label, Country, CountryA3, everything())
  
  # Filter Food & Beverages into its own category per country
  food_beverages_data <- summarized_data %>%
     filter(EORA_26_Label == "Food & Beverages")
  
  # Combine the Agriculture + Fishing data with Food & Beverages data
  final_data <- bind_rows(
     agriculture_fishing_data,
    food_beverages_data) %>% 
    arrange(Country)
  
  # Extract unique country names from column names
  unique_countries <- unique(gsub("_.*", "", colnames(final_data)))
  
  # Iterate over each country
  for (country in unique_countries) {
    # Identify columns for "Agriculture" and "Food & Beverages" for this country
    agriculture_cols <- grep(
      paste0("^", country, "_.*(Agriculture|Fishing|Agriculture, Fishing)"),
      colnames(final_data),
      value = TRUE
    )
    food_beverages_cols <- grep(
      paste0("^", country, "_.*Food & Beverages"),
      colnames(final_data),
      value = TRUE
    )
    
    # Sum the columns for "Agriculture" and update the dataframe
    if (length(agriculture_cols) > 0) {
      # Calculate the sum for "Agriculture"
      agriculture_sum <- rowSums(select(final_data, all_of(agriculture_cols)), na.rm = TRUE)
      
      # Drop old "Agriculture" columns
      final_data <- final_data %>% select(-all_of(agriculture_cols))
      
      # Insert the new "Agriculture" column at the appropriate position
      country_positions <- grep(paste0("^", country, "_"), colnames(final_data))
      insert_position <- if (length(country_positions) > 0) min(country_positions) else ncol(final_data) + 1
      final_data <- add_column(
        final_data,
        !!paste0(country, "_Agriculture and Fishing") := agriculture_sum,
        .before = insert_position
      )
    }
    
    # Sum the columns for "Food & Beverages" and update the dataframe
    if (length(food_beverages_cols) > 0) {
      # Calculate the sum for "Food & Beverages"
      food_beverages_sum <- rowSums(select(final_data, all_of(food_beverages_cols)), na.rm = TRUE)
      
      # Drop old "Food & Beverages" columns
      final_data <- final_data %>% select(-all_of(food_beverages_cols))
      
      # Insert the new "Food & Beverages" column at the appropriate position
      country_positions <- grep(paste0("^", country, "_"), colnames(final_data))
      insert_position <- if (length(country_positions) > 0) min(country_positions) else ncol(final_data) + 1
      final_data <- add_column(
        final_data,
        !!paste0(country, "_Food & Beverages") := food_beverages_sum,
        .before = insert_position
      )
    }
  }
  
  # Identify unique countries from the `country` column
  unique_countries <- unique(final_data$Country)
  Year <- as.numeric(str_extract(file_name, "\\d{4}"))
  
  final_data <- final_data %>%
    mutate(YEAR = Year) %>% 
    rowwise() %>%
    mutate(across(-c(EORA_26_Label, Country, CountryA3, YEAR), ~ ifelse(str_detect(cur_column(), Country), ., 0)))
  final_data <- as.data.frame(final_data)
  
  #Pivot long the dataset and apply share
  final_data <- final_data %>%
    pivot_longer(
      cols = -c(
        EORA_26_Label,
        Country,
        CountryA3,
        YEAR),
      names_to = "COL",
      values_to = "VAR5"
    ) %>%
    rename(ROW = EORA_26_Label,
          ) %>%
    select(YEAR, Country, ROW, COL, VAR5) %>%
    mutate(VAR5 = round(VAR5, 2)) %>%
    filter(VAR5 != 0) %>%
    rowwise() %>%
    ungroup() 
  
  if ("Agriculture and Fishing" %in% unique(final_data$ROW)) {
    filtered_data <- final_data %>%
      filter(ROW == "Agriculture and Fishing" & str_detect(COL, "Agriculture and Fishing"))
   
    #import share data
    share_data_agric <- read_excel("C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/Agriculture_share.xlsx")
    
    # Merge with `share data` based on `country` and `year`
    final_data_with_shares <- filtered_data %>%
      left_join(share_data_agric, by = c("Country" = "country", "YEAR" = "year")) %>%
      mutate(
        Z01T03_a = ifelse(!is.na(Z01T03_a), (Z01T03_a/100)*VAR5, NA),
        Z03 = ifelse(!is.na(Z03), (Z03/100)*VAR5, NA),
        Z01T03_b = ifelse(!is.na(Z01T03_b), (Z01T03_b/100)*VAR5, NA)
      )
    
    output_final <- file.path(output_dir, paste0(Year, "_", "Agricultural_Margin.csv"))
    write_csv(final_data_with_shares, output_final)
  }

  if ("Food & Beverages" %in% unique(final_data$ROW)) {
    filtered_data <- final_data %>%
      filter(ROW == "Food & Beverages" & str_detect(COL, "Food & Beverages"))
    
    #import share data
    share_data_food <- read_excel("C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/food_beverages_share.xlsx")
    
    # Merge with `share data` based on `country` and `year`
    final_data_with_shares <- filtered_data %>%
      left_join(share_data_food, by = c("Country" = "country", "YEAR" = "year")) %>%
      mutate(
        Z10T12_a = ifelse(!is.na(Z10T12_a), (Z10T12_a/100)*VAR5, NA),
        Z10T12_b = ifelse(!is.na(Z10T12_b), (Z10T12_b/100)*VAR5, NA),
        Z10T12_c = ifelse(!is.na(Z10T12_c), (Z10T12_c/100)*VAR5, NA)
      )
    
    output_final <- file.path(output_dir, paste0(Year, "_", "Food_and_Beverages_Margin.csv"))
    write_csv(final_data_with_shares, output_final)
  }

  
  # Write processed data to the output directory
  #write_csv(final_data, file.path(output_dir, file_name))
}

# Get all CSV files in the main directory
csv_files <- dir_ls(main_dir, glob = "*.csv")

# Process each CSV file
walk(csv_files, process_csv_files)

print("All files have been processed and saved to the output directory.")
