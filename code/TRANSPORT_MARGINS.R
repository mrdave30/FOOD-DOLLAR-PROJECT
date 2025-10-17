library(readr)
library(dplyr)  
library(fs) 
library(readxl)
library(tidyverse)
library(Matrix)
library(purrr)
library(data.table)

# =======================================================
# Paths to where files are stored and where to save output
# ========================================================
main_dir <- "C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/Transport_margin"
output_dir <- file.path(main_dir, "Processed_files_Transaport_Margin")
dir_create(output_dir)
final_output_dir <- file.path(main_dir, "Final_Transaport_Margin")
dir_create(final_output_dir)

# ============================
# Preprocessing
# ============================
CT <- read_xlsx(file.path(main_dir, "..", "Concordance_Table.xlsx"), sheet = 1)
countries <- unique(CT$Column1)
industries <- unique(CT$`_3`)

# Build index list for each country
country_index <- split(1:nrow(CT), CT$Column1)

#List all trade margin files for processing
trade_files <- list.files(main_dir, pattern = "\\.csv$", full.names = TRUE)

# ============================
# Loop over trade files
# ============================
for (file in trade_files) {
  cat("Processing:", basename(file), "\n")
  
  # Load full Trade matrix
  T <- as.matrix(read_csv(file, col_names = FALSE))
  rownames(T) <- paste0(CT$Column1, "_", CT$`_3`)
  colnames(T) <- rownames(T)
  
  # Storage for blocks
  block_list <- list()
  
  # Process each country block
  for (cc in countries) {
    idx <- country_index[[cc]]
    
    # Extract sub-matrix (26 × 26 block for cc)
    T_cc <- T[idx, idx]
    
    # Build C for this country (just identity if mapping is direct)
    C_cc <- as.matrix(CT[idx, sapply(CT, is.numeric)])
    C_cc <- as(C_cc, "dgCMatrix")
    C_cc_t <- t(C_cc)
    
    # Aggregation
    T1 <- C_cc_t %*% T_cc %*% C_cc
    
    # Store
    block_list[[cc]] <- as.matrix(T1)
  }
  
  # Join all blocks into a big block-diagonal matrix
  # To check if we can have the interaction between countries=== TO BE DONE
  T2 <- Matrix::bdiag(block_list)
 
  # Force into a dense matrix first
  T2_mat <- as.matrix(T2)

  # Build expanded labels
  categories <- c("Agriculture", "Fishing", "Mining and Quarrying", "Food & Beverages",  # nolint
                          "Textiles and Wearing Apparel", "Wood and Paper",  # nolint
                          "Petroleum, Chemical and Non-Metallic Mineral Products", "Metal Products",  # nolint
                          "Electrical and Machinery", "Transport Equipment", "Other Manufacturing",  # nolint
                          "Recycling", "Electricity, Gas and Water", "Construction", "Maintenance and Repair",  # nolint
                          "Wholesale Trade", "Retail Trade", "Hotels and Restaurants", "Transport",  # nolint
                          "Post and Telecommunications", "Financial Intermediation and Business Activities",  # nolint
                          "Public Administration", "Education, Health and Other Services", "Private Households",  # nolint
                          "Others", "Re-export & Re-import")

  row_labels <- paste0(rep(countries, each = length(categories)), "_", categories)
  col_labels <- paste0(rep(countries, each = length(categories)), "_", categories) # nolint
  rownames(T2) <- row_labels
  colnames(T2) <- col_labels

  # ============================
  # Split rownames → Country + Industry
  # ============================
  row_split <- do.call(rbind, strsplit(row_labels, "_", fixed = TRUE))
  colnames(row_split) <- c("Country", "Industry")
  row_info <- as.data.frame(row_split, stringsAsFactors = FALSE)
  
  # Bind row info with the data
  T2_df <- cbind(row_info, as.data.frame(T2_mat))

  # Fix column names (first two from row_info + industries)
  colnames(T2_df) <- c("Country", "Industry", col_labels)
  
  # Save
  year <- gsub("\\D", "", basename(file))
  write.csv(T2_df,
            file.path(output_dir, paste0("T2_", year, ".csv")),
            row.names = FALSE)
}

# ============================
# 2. Process each CSV file to calculate trade margins
# ============================

# Define the column to use as column names
#sector_column <- "EORA 26 label" # nolint

#if (!sector_column %in% colnames(labels)) {
#stop(paste("The specified column", sector_column, "is not found in the labels file.")) # nolint
#}
#colnames_list <- labels[[sector_column]] # nolint

process_csv_files <- function(file_path) {
  # Read CSV file
  data <- read_csv(file_path, col_names = TRUE)
  
  # Define variables to avoid referencing undefined objects
  final_data_with_shares_agric <- NULL
  final_data_with_shares_food <- NULL
  
  # Assign proper column names using labels
  #colnames(data) <- paste0(labels$Country, "_", colnames_list) 
  
  file_name <- basename(file_path)
  
  # Select and filter relevant data
  data <- data %>%
    select(Country, Industry, matches("Agriculture|Fishing|Food & Beverages")) %>%  # nolint
    filter(Industry %in% c("Agriculture", "Fishing", "Food & Beverages"))  # nolint
  
  # Summarize data by EORA 26 label, Country, and CountryA3a
    summarized_data <- data %>%
     group_by(Country, Industry) %>%
     summarize(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") # nolint
   
  # Rename column for clarity
  summarized_data <- data 

  # Combine Agriculture and Fishing into a single category per country
  agriculture_fishing_data <- summarized_data %>%
    filter(Industry %in% c("Agriculture", "Fishing")) %>%
    group_by(Country) %>%
    summarize(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(Industry = "Agriculture and Fishing") %>% 
    select(Industry, Country, everything())
  
  # Filter Food & Beverages into its own category per country
  food_beverages_data <- summarized_data %>%
     filter(Industry == "Food & Beverages")
  
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
      paste0("^", country, "_.*(Agriculture|Fishing)"),
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
    mutate(across(-c(Industry, Country, YEAR), ~ ifelse(str_detect(cur_column(), Country), ., 0)))
  final_data <- as.data.frame(final_data)

  #Pivot long the dataset and apply share
  final_data <- final_data %>%
    pivot_longer(
      cols = -c(
        Industry,
        Country,
        YEAR),
      names_to = "COL",
      values_to = "VAR5"
    ) %>%
    rename(ROW = Industry,
          ) %>%
    select(YEAR, Country, ROW, COL, VAR5) %>%
    mutate(VAR5 = round(VAR5, 2)) %>%
    filter(VAR5 != 0) %>%
    rowwise() %>%
    ungroup()
  

  # Process Agricultural Margin
  if ("Agriculture and Fishing" %in% unique(final_data$ROW)) {
    filtered_data <- final_data %>%
      filter(ROW == "Agriculture and Fishing" & str_detect(COL, "Agriculture and Fishing"))

    #import share data
    share_data_agric <- read_csv("C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/Agriculture_share.csv")


    # Merge and calculate margins
    final_data_with_shares_agric <- filtered_data %>%
      left_join(share_data_agric, by = c("Country" = "country", "YEAR" = "year")) %>%
      mutate(
        Z01T03_a = ifelse(!is.na(Z01T03_a), (Z01T03_a / 100) * VAR5, NA),
        Z03 = ifelse(!is.na(Z03), (Z03 / 100) * VAR5, NA),
        Z01T03_b = ifelse(!is.na(Z01T03_b), (Z01T03_b / 100) * VAR5, NA),
        Z01T03 = ifelse(!is.na(Z01T03_b), (VAR5 - Z01T03_b), NA)
      ) %>%
      mutate(TRADE = Z01T03,
             code = "Z01T03")

    # output_final <- file.path(output_dir, paste0(Year, "_", "Agricultural_Margin.csv"))
    # write_csv(final_data_with_shares_agric, output_final)
  }

  # Process Food & Beverages Margin
  if ("Food & Beverages" %in% unique(final_data$ROW)) {
    filtered_data <- final_data %>%
      filter(ROW == "Food & Beverages" & str_detect(COL, "Food & Beverages"))

    #import share data
    share_data_food <- read_csv("C:/Users/Nkoro/Food and Agriculture Organization/ESSDC - Data Generation and Capacity Building WorkSpace - EORA data/Full Eora/food_beverages_share.csv")

    # Merge and calculate margins
    final_data_with_shares_food <- filtered_data %>%
      left_join(share_data_food, by = c("Country" = "country", "YEAR" = "year")) %>%
      mutate(
        Z10T12_a = ifelse(!is.na(Z10T12_a), (Z10T12_a / 100) * VAR5, NA),
        Z10T12_b = ifelse(!is.na(Z10T12_b), (Z10T12_b / 100) * VAR5, NA),
        Z10T12_c = ifelse(!is.na(Z10T12_c), (Z10T12_c / 100) * VAR5, NA),
        Z10T12 = ifelse(!is.na(Z10T12_c), (VAR5 - Z10T12_c), NA)
      ) %>%
      mutate(TRADE = Z10T12,
             code = "Z10T12")

    # output_final <- file.path(output_dir, paste0(Year, "_", "Food_and_Beverages_Margin.csv"))
    # write_csv(final_data_with_shares_food, output_final)
  }

  # Combine Agriculture and Food & Beverages margins into a single dataset
  final_data <- bind_rows(
    if (!is.null(final_data_with_shares_agric)) final_data_with_shares_agric %>% select(Country, YEAR, TRADE, code),
    if (!is.null(final_data_with_shares_food)) final_data_with_shares_food %>% select(Country, YEAR, TRADE, code)
  )
  # Save the final merged dataset
  output_final <- file.path(final_output_dir, paste0(Year, "_", "Transport_Margin.csv"))
  write_csv(final_data, output_final)

    
}

# Get all CSV files in the main directory
csv_files <- dir_ls(output_dir, glob = "*.csv")

# Process each CSV file
walk(csv_files, process_csv_files)
