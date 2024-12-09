library(fs)
library(zip)
library(readr)
library(tools)
library(dplyr)
library(stringr)
library(tidyverse)
library(janitor)

# Define the directory
base_dir <- "C:\\Users\\Nkoro\\Food and Agriculture Organization\\ESSDC - Data Generation and Capacity Building WorkSpace - EORA data\\Eora26\\test"

# List all folders in the directory
folders <- dir_ls(base_dir, type = "directory")

# Function to process each folder
process_folder <- function(folder) {
  message("Processing folder: ", folder)
  
  # Extract year from the folder name
  folder_name <- basename(folder)
  year <- str_extract(folder_name, "\\d{4}")
  
  if (is.na(year)) {
    message("No year found in folder name: ", folder_name)
    return(NULL)
  }
  
  # List all zip files in the folder with "bp" or "pp" in the name
  zip_files <- dir_ls(folder, regexp = ".*(bp|pp).*\\.zip$", type = "file")
  
  if (length(zip_files) == 0) {
    message("No zip files found in: ", folder)
    return(NULL)
  }
  
  lapply(zip_files, function(zip_file) {
    message("Processing zip file: ", zip_file)
    
    # Determine if the zip file is bp or pp
    zip_type <- ifelse(grepl("bp", zip_file), "bp", "pp")
    
    # Create a temporary directory to unzip files
    temp_dir <- tempfile()
    dir_create(temp_dir)
    
    # Unzip the folder
    unzip(zip_file, exdir = temp_dir)
    
    # List all txt files in the unzipped folder
    txt_files <- dir_ls(temp_dir, regexp = "\\.txt$", type = "file")
    
    if (length(txt_files) == 0) {
      message("No txt files found in: ", zip_file)
      return(NULL)
    }
    
    # Filter txt files for specific acronyms
    txt_files <- txt_files[grepl("VA|FD|T", basename(txt_files))]
    
    # Process txt files
    process_txt_files(txt_files, folder, year, zip_type)
    
    # Clean up temporary directory
    dir_delete(temp_dir)
  })
}

# Function to process txt files
process_txt_files <- function(txt_files, original_folder, year, zip_type) {
  message("Processing txt files: ", paste(txt_files, collapse = ", "))
  
  # Find EORA26 files and label files
  eora_files <- txt_files[grepl("^Eora26", basename(txt_files))]
  label_files <- txt_files[grepl("labels", basename(txt_files))]
  
  if (length(eora_files) == 0) {
    message("No EORA26 files found.")
    return(NULL)
  }
  
  if (length(label_files) == 0) {
    message("No label files found.")
    return(NULL)
  }
  
  lapply(eora_files, function(eora_file) {
    message("Processing EORA26 file: ", eora_file)
    
    eora_data <- read_delim(eora_file, col_names = FALSE, delim = "\t")
    eora_title <- tools::file_path_sans_ext(basename(eora_file))
    
    lapply(label_files, function(label_file) {
      message("Processing label file: ", label_file)
      
      label_data <- read_delim(label_file, col_names = FALSE, delim = "\t")
      label_title <- tools::file_path_sans_ext(basename(label_file))
      
      # Check if last acronyms in the titles are the same
      eora_acronym <- tail(strsplit(eora_title, "_")[[1]], 1)
      label_acronym <- tail(strsplit(label_title, "_")[[1]], 1)
      
      if (eora_acronym == label_acronym) {
        message("Matching acronyms found: ", eora_acronym)
        
        if (grepl("VA", basename(eora_file))) {
          message("Processing VA file: ", eora_file)
          
          va_data <- read_delim(eora_file, col_names = FALSE, delim = "\t")
          labels_data <- read_delim(label_file, col_names = FALSE, delim = "\t") %>%
            rename("Industry" = X1, sector = X2)
          
          # Extract primary input labels from labels_data
          primary_input_labels <- labels_data$Industry
          
          # Get the dimensions of va_data
          num_rows <- nrow(va_data)
          num_columns <- ncol(va_data)
          
          country_names <- read_delim("C:\\Users\\Nkoro\\OneDrive - Food and Agriculture Organization\\Food value chain\\FVC\\labels_FD.txt", col_names = FALSE, delim = "\t") %>%
            rename("country" = X1) %>%
            distinct(country)%>%
            pull(country)
          
          # Define sector names
          sector_names <- c("Compensation of employees D.1", "Taxes on production D.29", 
                            "Subsidies on production D.39", "Net operating surplus B.2n", 
                            "Net mixed income B.3n", "Consumption of fixed capital K.1")
          
          # Initialize empty list to store reshaped data
          reshaped_list <- list()
          
          # Define variables for column and row increments
          col_increment <- 26
          
          for (i in seq_along(country_names)) {
            # Calculate the column start index
            start_col <- ((i - 1) * col_increment) + 1
            end_col <- min(start_col + col_increment - 1, num_columns)
            
            # Ensure end_col does not exceed the number of columns in va_data
            if (start_col > num_columns) {
              next
            }
            
            # Extract the values for the current country
            values <- va_data[, start_col:end_col]
            
            # Ensure values length matches col_increment
            if (ncol(values) < col_increment) {
              values <- cbind(values, matrix(NA, nrow = nrow(values), ncol = col_increment - ncol(values)))
            }
            
            for (j in seq_along(sector_names)) {
              # Create a data frame with country and sector information
              df <- data.frame(
                Industry = primary_input_labels,
                country = rep(country_names[i], each = nrow(values)),
                sector = rep(sector_names, each = nrow(values) / length(sector_names)),
                values
              )
              
              # Append the country data frame to the list
              reshaped_list[[i]] <- df
            }
          }
          
          # Combine all data frames into one
          reshaped_data <- bind_rows(reshaped_list)
          
          # Ensure the column names match the number of columns in reshaped_data
          column_names <- c("Industry", "country", "sector", rep(country_names, each = col_increment))
          if (length(column_names) > ncol(reshaped_data)) {
            column_names <- column_names[1:ncol(reshaped_data)]
          } else if (length(column_names) < ncol(reshaped_data)) {
            column_names <- c(column_names, rep("Extra", ncol(reshaped_data) - length(column_names)))
          }
          
          # Set column names
          colnames(reshaped_data) <- column_names
          
          reshaped_data <- reshaped_data %>%
            set_names(make.unique(names(.))) %>% 
            # mutate(
            #   Year = as.character(year)
            # ) %>% 
            select(
              Industry, sector, country, everything()
            )
          categories <- c("Agriculture", "Fishing", "Mining and Quarrying", "Food & Beverages", 
                          "Textiles and Wearing Apparel", "Wood and Paper", 
                          "Petroleum, Chemical and Non-Metallic Mineral Products", "Metal Products", 
                          "Electrical and Machinery", "Transport Equipment", "Other Manufacturing", 
                          "Recycling", "Electricity, Gas and Water", "Construction", "Maintenance and Repair", 
                          "Wholesale Trade", "Retail Trade", "Hotels and Restaurants", "Transport", 
                          "Post and Telecommunications", "Financial Intermediation and Business Activities", 
                          "Public Administration", "Education, Health and Other Services", "Private Households", 
                          "Others", "Re-export & Re-import")
          
          colnames(reshaped_data)[-(1:3)] <- paste(colnames(reshaped_data)[-(1:3)], "TZ", sep = "_")
          
          reshaped_data[is.na(reshaped_data)] <- 0
          
          # Write the output to a CSV file
          output_file <- file.path(getwd(), paste0(eora_title, "_", zip_type, "_labeled.csv"))
          write_csv(reshaped_data, file = output_file)
          
        } else {
          colnames(eora_data) <- make.names(label_data[[1]], unique = TRUE)
          colnames(eora_data)[is.na(colnames(eora_data)) | colnames(eora_data) == ""] <- "Unnamed"
          
          categories <- c("Agriculture", "Fishing", "Mining and Quarrying", "Food & Beverages", 
                          "Textiles and Wearing Apparel", "Wood and Paper", 
                          "Petroleum, Chemical and Non-Metallic Mineral Products", "Metal Products", 
                          "Electrical and Machinery", "Transport Equipment", "Other Manufacturing", 
                          "Recycling", "Electricity, Gas and Water", "Construction", "Maintenance and Repair", 
                          "Wholesale Trade", "Retail Trade", "Hotels and Restaurants", "Transport", 
                          "Post and Telecommunications", "Financial Intermediation and Business Activities", 
                          "Public Administration", "Education, Health and Other Services", "Private Households", 
                          "Others", "Re-export & Re-import")
          
          # Define categories for FD
          categories_fd <- c("Final_demand_P.3h", "Final_demand_P.3n", 
                             "Final_demand_P.3g", "Final_demand_P.51", 
                             "Final_demand_P.52", "Final_demand_P.53")
          
          
          row_increment <- 26
          total_rows <- nrow(eora_data)
          
          # Repeat each category to match the total number of rows
          repeated_categories <- rep(categories, time = row_increment, length.out = total_rows)
          
          countryname <- data.frame(colnames(eora_data)) %>%
            filter(!str_detect(colnames.eora_data., "\\d")) %>%
            rename(country = colnames.eora_data.)
          
          # Repeat each country to match the total number of rows
          repeated_countries <- rep(countryname$country, each = row_increment, length.out = total_rows)
          
          eora_data <- eora_data %>%
            
            mutate(
              Industry = ifelse(grepl("T", basename(eora_title)), "Industries", "Final Demand"),
              sector = repeated_categories, 
              country = repeated_countries
              #Year = as.character(year)
            ) %>%
            select(Industry,sector, country, 
                   everything())
          
          # Append categories to the appropriate columns
          if(grepl("T", basename(eora_title))) {
            colnames(eora_data)[-(1:3)] <- paste(colnames(eora_data)[-(1:3)], "TZ", sep = "_")
          } else {
            colnames(eora_data)[-(1:3)] <- paste(colnames(eora_data)[-(1:3)], categories_fd, sep = "_")
          }
          
          
          output_file <- file.path(getwd(), paste0(eora_title, "_", zip_type, "_labeled.csv"))
          write_csv(eora_data, file = output_file)
          
          message("Saved: ", output_file)
        }
      } else {
        message("Acronyms do not match: ", eora_acronym, " != ", label_acronym)
      }
    })
  })
  
  # List all CSV files in the directory
  csv_files <- dir_ls(getwd(), regexp = "Eora26_\\d{4}_(bp|pp)_(VA|T|FD)_(bp|pp)_labeled\\.csv$", type = "file")
  
  # Initialize lists to store datasets
  va_data_list <- list(bp = list(), pp = list())
  t_data_list <- list(bp = list(), pp = list())
  fd_data_list <- list(bp = list(), pp = list())
  
  # Process each CSV file
  for (csv_file in csv_files) {
    message("Processing file: ", csv_file)
    
    # Determine if the file is bp or pp
    file_type <- ifelse(grepl("_bp_", csv_file), "bp", ifelse(grepl("_pp_", csv_file), "pp", NA))
    
    if (is.na(file_type)) {
      message("File type is neither bp nor pp: ", csv_file)
      next
    }
    
    # Read the CSV file
    data <- read_csv(csv_file)
    message("Read data with dimensions: ", dim(data))
    
    # Check if the filename contains "VA", "T", or "FD"
    if (grepl("_VA_", csv_file)) {
      message("Found VA data for ", file_type)
      va_data_list[[file_type]] <- append(va_data_list[[file_type]], list(data))
    } else if (grepl("_T_", csv_file)) {
      message("Found T data for ", file_type)
      t_data_list[[file_type]] <- append(t_data_list[[file_type]], list(data))
    } else if (grepl("_FD_", csv_file)) {
      message("Found FD data for ", file_type)
      fd_data_list[[file_type]] <- append(fd_data_list[[file_type]], list(data))
    } else {
      message("File does not match VA, T, or FD: ", csv_file)
    }
  }
  
  # Merge and save data for each type
  for (type in c("bp", "pp")) {
    # Check if the data frames are not empty
    if (length(va_data_list[[type]]) > 0 && length(t_data_list[[type]]) > 0) {
      # Row-bind VA and T data
      merged_va_t <- bind_rows(t_data_list[[type]], va_data_list[[type]])
      
      
      # Save merged VA and T data
      output_merged_va_t <- file.path(base_dir, paste0(year, "_", type, "_merged_va_t.csv"))
      write_csv(merged_va_t, output_merged_va_t)
      message("Merged VA-T dataset saved as: ", output_merged_va_t)
    } 
    
    # Column-bind VA-T data with FD data if FD data is available
    if (length(fd_data_list[[type]]) > 0) {
      # Determine the number of rows in each dataset
      num_rows_va_t <- nrow(merged_va_t)
      fd_data <- bind_rows(fd_data_list[[type]])
      num_rows_fd <- nrow(fd_data)
      
      if (num_rows_va_t > num_rows_fd) {
        # Add rows with NA values to fd_data
        for (i in seq_len(num_rows_va_t - num_rows_fd)) {
          fd_data <- add_row(fd_data)
        }
      }
      
      
      # Combine fd_data with merged_va_t
      merged_va_t_fd <- cbind(merged_va_t, fd_data)
      
      # Make column names unique manually
      colnames(merged_va_t_fd) <- make.unique(colnames(merged_va_t_fd))
      
      # Calculate gross output with adjustment
      merged_va_t_fd <- merged_va_t_fd %>%
        select(-Industry.1, -sector.1, -country.1) %>%
        mutate(gross_output_xout = rowSums(across(where(is.numeric)), na.rm = TRUE)) %>%
        mutate(gross_output_xout = ifelse(row_number() > 4915, 0, gross_output_xout))

      # Identify TZ and Final Demand (FD) columns
      tz_columns <- grep("TZ", colnames(merged_va_t_fd), value = TRUE)
      final_demand_columns <- grep("Final_demand", colnames(merged_va_t_fd), value = TRUE)

      # Adjust merged data (set final demand columns to zero after row 26)
      merged_va_t_fd[4916:nrow(merged_va_t_fd), final_demand_columns] <- 0

      #Define the RAS algorithm
      ras_algorithm <- function(matrix, target_row_sums, target_col_sums,
                                max_iter = 3, tol = 1e-6, verbose = TRUE) {
        # Initialize row scaling factors
        R <- rep(1, nrow(matrix))
        # Initialize column scaling factors
        S <- rep(1, ncol(matrix))

        for (i in 1:max_iter) {
          # Update rows
          row_sums <- rowSums(matrix)
          row_sums[row_sums == 0] <- 1  
          R <- target_row_sums / row_sums
          matrix <- diag(R) %*% matrix

          # Update columns
          col_sums <- colSums(matrix)
          col_sums[col_sums == 0] <- 1  
          S <- target_col_sums / col_sums

          # Ensure conformable matrices
          matrix <- matrix %*% diag(S, length(S))

          # Check for convergence
          if (all(abs(rowSums(matrix) - target_row_sums) < tol, na.rm = TRUE) &&
              all(abs(colSums(matrix) - target_col_sums) < tol, na.rm = TRUE)) {
            break
          }

        }

        return(matrix)
      }

      
      #Apply RAS to the TZ columns
      #Extract the TZ columns and convert to matrix
      TZ_matrix <- as.matrix(merged_va_t_fd[, tz_columns])

      # Target row sums are the original row sums (merged_va_t_fd$gross_output_xout)
      # and column sums are the first n TZ columns of gross_output_xout
      target_row_sums <- merged_va_t_fd$gross_output_xout
      target_col_sums_TZ <- merged_va_t_fd$gross_output_xout[1:ncol(TZ_matrix)]

      # Apply the RAS algorithm to adjust TZ columns
      adjusted_TZ_matrix <- ras_algorithm(TZ_matrix, rowSums(TZ_matrix), target_col_sums_TZ)

      # Update the dataframe with adjusted TZ columns
      merged_va_t_fd[, tz_columns] <- adjusted_TZ_matrix

      #Adjust the FD columns to match the residuals after TZ adjustment
      FD_matrix <- as.matrix(merged_va_t_fd[, final_demand_columns])

      # Residual row sums after adjusting TZ matrix
      residual_row_sums <- target_row_sums - rowSums(adjusted_TZ_matrix)

      # Adjust FD matrix proportionally based on the residuals
      FD_row_sums <- rowSums(FD_matrix)
      scaling_factors <- ifelse(FD_row_sums == 0, 0, residual_row_sums / FD_row_sums)
      adjusted_FD_matrix <- diag(scaling_factors) %*% FD_matrix
      
      # Update the dataframe with adjusted FD columns
      merged_va_t_fd[, final_demand_columns] <- adjusted_FD_matrix
      
      #Difference between original and adjusted matrices
      # Calculate the difference for TZ columns
      TZ_diff_matrix <- TZ_matrix - adjusted_TZ_matrix
      
      # Calculate the difference for FD columns
      FD_diff_matrix <- FD_matrix - adjusted_FD_matrix
      
      # Create dataframes for differences
      TZ_diff_df <- as.data.frame(TZ_diff_matrix)
      FD_diff_df <- as.data.frame(FD_diff_matrix)
      
      # Assign appropriate column names
      colnames(TZ_diff_df) <- colnames(merged_va_t_fd[, tz_columns])
      colnames(FD_diff_df) <- colnames(merged_va_t_fd[, final_demand_columns])
      
      # Combine the differences into the original dataframe structure
      difference_df <- merged_va_t_fd
      difference_df[, tz_columns] <- TZ_diff_df
      difference_df[, final_demand_columns] <- FD_diff_df
      
      #Save the difference dataframe as a CSV file
      output_diff_file <- file.path(base_dir, paste0(year, "_", type, "_IOT_difference.csv"))
      write_csv(difference_df, output_diff_file)
      message("Difference file saved as: ", output_diff_file)
      
      #rename column names with sector_countrycode
      primary_input_rows <- merged_va_t_fd %>% filter(Industry == "Primary input")
      non_primary_input_rows <- merged_va_t_fd %>% filter(Industry != "Primary input")
      country_codes <- unique(non_primary_input_rows$country)
      sector <- unique(non_primary_input_rows$sector)
      repeated_country_codes <- rep(country_codes, each = 26, length.out = 4915)
      repeated_industries <- rep(sector, length.out = 4915)
      new_column_names <- paste0(repeated_country_codes, "_", repeated_industries)
      
      colnames(non_primary_input_rows)[4:4918] <- new_column_names
      colnames(primary_input_rows)[4:4918] <- new_column_names
      merged_va_t_fd <- bind_rows(non_primary_input_rows, primary_input_rows)
      
      #join agriculture and fishing columns together
      agric_fish_columns <- grep("Agriculture|Fishing", colnames(merged_va_t_fd), value = TRUE)
      country_codes <- unique(sub("_.*", "", agric_fish_columns))
      summed_agric_fish <- lapply(country_codes, function(country) {
        country_columns <- agric_fish_columns[grep(paste0("^", country, "_"), agric_fish_columns)]
        rowSums(merged_va_t_fd[, country_columns, drop = FALSE], na.rm = TRUE)
      })
      summed_agric_fish_df <- as.data.frame(do.call(cbind, summed_agric_fish))
      colnames(summed_agric_fish_df) <- paste0(country_codes, "_Agriculture")
      merged_va_t_fd <- merged_va_t_fd[, !colnames(merged_va_t_fd) %in% agric_fish_columns]
      
      #loop through country code to insert calculated columns in their positions
      for (i in seq_along(country_codes)) {
        country <- country_codes[i]
        new_col <- summed_agric_fish_df[[i]]
        country_positions <- grep(paste0("^", country, "_"), colnames(merged_va_t_fd))
        insert_position <- if (length(country_positions) > 0) min(country_positions) else ncol(merged_va_t_fd) + 1
        merged_va_t_fd <- add_column(merged_va_t_fd, !!paste0(country, "_Agriculture") := new_col, .before = insert_position)
      }
      
      #Separate "Primary input" rows before the loop
       primary_input_rows <- merged_va_t_fd %>% filter(Industry == "Primary input")
       non_primary_input_rows <- merged_va_t_fd %>% filter(Industry != "Primary input")
       
      final_merged_df <- data.frame()
      unique_countries <- unique(non_primary_input_rows$country)
       
      for (countryname in unique_countries) {
         country_data <- non_primary_input_rows %>% filter(country == countryname)
         
      # Sum up agriculture and fishing data for the current country
         combined_fish_agric <- country_data %>%
           filter(sector %in% c("Agriculture", "Fishing")) %>%
           summarise(across(where(is.numeric), sum))
         combined_fish_agric$sector <- "Agriculture, forestry and fisheries"
         combined_fish_agric$Industry <- "Industries"
         combined_fish_agric$country <- countryname
         country_data <- bind_rows(combined_fish_agric, country_data %>% filter(!sector %in% c("Agriculture", "Fishing")))
         final_merged_df <- bind_rows(final_merged_df, country_data) %>% 
          select(sector, Industry, country, everything())
       }
      merged_va_t_fd <- bind_rows(final_merged_df, primary_input_rows)
      
      
      #join wholesale and retail columns together
      wholesale_retail_columns <- grep("Wholesale Trade|Retail Trade", colnames(merged_va_t_fd), value = TRUE)
      country_codes <- unique(sub("_.*", "", wholesale_retail_columns))
      summed_wholesale_retail <- lapply(country_codes, function(country) {
        country_columns <- wholesale_retail_columns[grep(paste0("^", country, "_"), wholesale_retail_columns)]
        rowSums(merged_va_t_fd[, country_columns, drop = FALSE], na.rm = TRUE)
      })
      summed_wholesale_retail_df <- as.data.frame(do.call(cbind, summed_wholesale_retail))
      colnames(summed_wholesale_retail_df) <- paste0(country_codes, "_Wholesale and retail trade")
      merged_va_t_fd <- merged_va_t_fd[, !colnames(merged_va_t_fd) %in% wholesale_retail_columns]
      
      #loop through country code to insert calculated columns in their positions
      for (i in seq_along(country_codes)) {
        country <- country_codes[i]
        new_col <- summed_wholesale_retail_df[[i]]
        country_positions <- grep(paste0("^", country, "_"), colnames(merged_va_t_fd))
        insert_position <- if (length(country_positions) > 0) min(country_positions) else ncol(merged_va_t_fd) + 1
        merged_va_t_fd <- add_column(merged_va_t_fd, !!paste0(country, "_Wholesale and retail trade") := new_col, .before = insert_position)
      }
      
      #Separate "Primary input" rows before the loop
      primary_input_rows <- merged_va_t_fd %>% filter(Industry == "Primary input")
      non_primary_input_rows <- merged_va_t_fd %>% filter(Industry != "Primary input")
      
      final_merged_df <- data.frame()
      unique_countries <- unique(non_primary_input_rows$country)
      
      for (countryname in unique_countries) {
        country_data <- non_primary_input_rows %>% filter(country == countryname)
        
        # Sum up Wholesale and retail trade data for the current country
        combined_fish_agric <- country_data %>%
          filter(sector %in% c("Wholesale Trade", "Retail Trade")) %>%
          summarise(across(where(is.numeric), sum))
        combined_fish_agric$sector <- "Wholesale and retail trade"
        combined_fish_agric$Industry <- "Industries"
        combined_fish_agric$country <- countryname
        country_data <- bind_rows(combined_fish_agric, country_data %>% filter(!sector %in% c("Wholesale Trade", "Retail Trade")))
        final_merged_df <- bind_rows(final_merged_df, country_data) %>% 
          select(Industry, sector, country, everything())
      }
      merged_va_t_fd <- bind_rows(final_merged_df, primary_input_rows)
      
      insert_position <- which(colnames(merged_va_t_fd) == "ZWE_Re-export & Re-import")
      row_agriculture_data <- merged_va_t_fd$ROW_Agriculture
      merged_va_t_fd <- merged_va_t_fd %>% select(-ROW_Agriculture)
      merged_va_t_fd <- add_column(merged_va_t_fd, ROW_Agriculture = row_agriculture_data, .after = insert_position)
      
      # Define industry codes
      sector_code <- c("Z45T47", "Z01T03", "Z05T06_Z09", "Z10T12", "Z13T15", "Z16_Z17T18", "Z19_Z23",
                       "Z24_Z25", "Z26_Z29", "Z30", "Z31T33", "ZC37", "Z35T39", "Z41T43", "Z41T45",
                       "Z49T52", "Z55T56", "Z58T60_Z62T63", "Z64T66_Z69T82", "Z84",
                       "Z85_Z86T88", "Z97T98", "ZS94_96", "Z99")
      
      # Add `code` column, repeating as needed
      merged_va_t_fd <- merged_va_t_fd %>%
        mutate(code = rep(sector_code, length.out = nrow(merged_va_t_fd))) %>%
        select(Industry, sector, country, code, everything())
      
      # Adjust column names to avoid duplicates
      new_colnames <- rep(sector_code, length.out = 4540)
      colnames(merged_va_t_fd)[5:4540] <- paste0(merged_va_t_fd$country,"_", new_colnames)
      
      # Separate `Primary input` and non-primary input rows
      primary_input_rows <- merged_va_t_fd %>% filter(Industry == "Primary input")
      non_primary_input_rows <- merged_va_t_fd %>% filter(Industry != "Primary input")
      
      # Process adjustments for VA rows
      adjusted_VA <- data.frame()
      unique_countries <- unique(primary_input_rows$country)
     
      for (countryname in unique_countries) {
        country_data <- primary_input_rows %>% filter(country == countryname)
        
        # Sum up Consumption of fixed capital and Net operating surplus
        combined_gos <- country_data %>%
          filter(sector %in% c("Consumption of fixed capital K.1", "Net operating surplus B.2n")) %>%
          summarise(across(where(is.numeric), sum, na.rm = TRUE))
        combined_gos$sector <- "Gross Operating Surplus"
        combined_gos$Industry <- "Primary input"
        combined_gos$country <- countryname
        combined_gos$code <- "VGOPS"
        
        # Add combined GOS data back to `country_data`
        country_data <- bind_rows(combined_gos, country_data %>% filter(
          !sector %in% c("Consumption of fixed capital K.1", "Net operating surplus B.2n")))
        
        # minus Subsidies on production from Taxes on production
        combined_taxes <- country_data %>%
          filter(sector %in% c("Taxes on production D.29", "Subsidies on production D.39")) %>%
          summarise(across(where(is.numeric), ~ diff(.x, na.rm = TRUE))) 
        combined_taxes$sector <- "Output taxes"
        combined_taxes$Industry <- "Primary input"
        combined_taxes$country <- countryname
        combined_taxes$code <- "VOTXSplus"
        
        # Add combined taxes data back to `country_data`
        country_data <- bind_rows(combined_taxes, country_data %>% filter(
          !sector %in% c("Taxes on production D.29", "Subsidies on production D.39")))
        
        adjusted_VA <- bind_rows(adjusted_VA, country_data) %>%
          select(Industry, sector, country, code, everything())
      }
      
      # Combine adjusted VA with non-primary rows and assign `code` for specific sectors
      merged_va_t_fd <- bind_rows(non_primary_input_rows, adjusted_VA) %>%
        mutate(code = ifelse(sector == "Compensation of employees D.1", "VLABR", code)) %>%
        filter(sector != "Net mixed income B.3n")
      
      #...................Reapply RAS algorithm to the adjusted IOT...............................
      
      #Extract the TZ columns and convert to matrix
      pattern <- paste(sector_code, collapse = "|")
      tz_columns <- grep(pattern, colnames(merged_va_t_fd), value = TRUE)
      TZ_matrix <- as.matrix(merged_va_t_fd[, tz_columns])
      target_row_sums <- merged_va_t_fd$gross_output_xout
      target_col_sums_TZ <- merged_va_t_fd$gross_output_xout[1:ncol(TZ_matrix)]
      
      # Apply the RAS algorithm to adjust TZ columns
      adjusted_TZ_matrix <- ras_algorithm(TZ_matrix, rowSums(TZ_matrix), target_col_sums_TZ)
      merged_va_t_fd[, tz_columns] <- adjusted_TZ_matrix
      
      #Adjust the FD columns to match the residuals after TZ adjustment
      FD_matrix <- as.matrix(merged_va_t_fd[, final_demand_columns])
      residual_row_sums <- target_row_sums - rowSums(adjusted_TZ_matrix)
      FD_row_sums <- rowSums(FD_matrix)
      scaling_factors <- ifelse(FD_row_sums == 0, 0, residual_row_sums / FD_row_sums)
      adjusted_FD_matrix <- diag(scaling_factors) %*% FD_matrix
      merged_va_t_fd[, final_demand_columns] <- adjusted_FD_matrix
      
      #......................Difference between original and adjusted matrices....................
      
      # Calculate the difference for TZ columns
      TZ_diff_matrix <- TZ_matrix - adjusted_TZ_matrix
      
      # Calculate the difference for FD columns
      FD_diff_matrix <- FD_matrix - adjusted_FD_matrix
      
      # Create dataframes for differences
      TZ_diff_df <- as.data.frame(TZ_diff_matrix)
      FD_diff_df <- as.data.frame(FD_diff_matrix)
      
      # Assign appropriate column names
      colnames(TZ_diff_df) <- colnames(merged_va_t_fd[, tz_columns])
      colnames(FD_diff_df) <- colnames(merged_va_t_fd[, final_demand_columns])
      
      # Combine the differences into the original dataframe structure
      difference_df <- merged_va_t_fd
      difference_df[, tz_columns] <- TZ_diff_df
      difference_df[, final_demand_columns] <- FD_diff_df
      
      #Save the difference dataframe as a CSV file
      output_diff_file <- file.path(base_dir, paste0(year, "_", type, "_IOT2_difference.csv"))
      write_csv(difference_df, output_diff_file)
      message("Difference file saved as: ", output_diff_file)
 
      #...................calculate total import and export.......................................................................
      
      # Function to calculate sum for TZ matrix and Final_demand excluding country-specific columns
      calculate_sum <- function(row, country_col, sum_cols) {
        country <- row[[country_col]]
        selected_cols <- sum_cols[!grepl(country, sum_cols, ignore.case = TRUE)]
        sum(as.numeric(row[selected_cols]), na.rm = TRUE)
      }

      # Calculate from_TZ matrix and from_Final_demand matrix
      merged_va_t_fd <- merged_va_t_fd %>%
        rowwise() %>%
        mutate(export_to_companies = calculate_sum(cur_data(), "country", tz_columns),
               export_to_foreign_Consumers = calculate_sum(cur_data(), "country", final_demand_columns),
               Total_Export = export_to_companies + export_to_foreign_Consumers,
               gross_output = gross_output_xout + Total_Export) %>%
        ungroup()

      # Calculate the column sums for TZ columns excluding the rows that match the country specified in the column name
      import_from_companies <- sapply(tz_columns, function(colname) {
        col_country <- gsub("([a-zA-Z]+)_.*", "\\1", colname)
        indices_to_include <- which(merged_va_t_fd$country != col_country)
        sum(merged_va_t_fd[indices_to_include, colname], na.rm = TRUE)
      })

      # Add zeros to the column 'import_from_y' after row 4538
       merged_va_t_fd <- merged_va_t_fd %>%
         mutate(import_from_companies = ifelse(row_number() > 4538, 0, import_from_companies))

      #  Function to calculate imports from final consumers
      calculate_imports <- function(df) {
      # Initialize a column to store import results
         df$Import_from_Final_Consumers <- 0
         unique_sectors <- unique(df$sector)
         unique_countries <- unique(df$country)
      
        # Loop through each sector
        for (Sector in unique_sectors) {

          # Filter the dataframe for the current sector
          sector_data <- df %>% filter(sector == Sector)

          # Loop through each unique country in the data
          for (country in unique_countries) {

            # Get all columns that correspond to the current country and contain "Final_demand"
            country_columns <- grep(paste0("^", country, ".*Final_demand"), colnames(sector_data), value = TRUE)

            # Initialize the import sum
            import_sum <- 0

            # Loop through other countries and sum their values for the same sector
            for (other_country in unique_countries) {
              if (other_country != country) {
                # Calculate the row sums for other countries' columns related to the current country
                other_country_sum <- rowSums(sector_data[sector_data$country == other_country, 
                                              country_columns, drop = FALSE], na.rm = TRUE)

                # Add the sum to the import sum
                import_sum <- import_sum + sum(other_country_sum, na.rm = TRUE)
              }
            }

            # Assign the import sum to the Import_from_Final_Consumers column for the corresponding rows
            df$Import_from_Final_Consumers[df$country == country & df$sector == Sector] <- import_sum
          }
        }

        return(df)
      }

      # Apply the function to calculate imports and add the column to the dataframe
      merged_va_t_fd <- calculate_imports(merged_va_t_fd)

      # Add zeros after row 4538
      merged_va_t_fd <- merged_va_t_fd %>%
        mutate(Import_from_Final_Consumers = ifelse(row_number() > 4538, 0, Import_from_Final_Consumers))

      # Calculate total import and modify dataframe
      merged_va_t_fd <- merged_va_t_fd %>%
        mutate(
          total_import = Import_from_Final_Consumers + import_from_companies,
          Year = year
        ) %>%
        select(
          Industry, sector, country, Year, everything()
        ) %>% 
        # Rename and sum final demand columns
        select(-matches("Final_demand_P.53")) %>% 
        rename_with(
          ~ str_replace(., "Final_demand_P.3g", "YGOV"),
          matches("Final_demand_P.3g")
        ) %>% 
        rename(YEXP=Total_Export,
               YIMP=total_import) %>% 
        select(-c(
                  gross_output_xout,
                  export_to_companies,
                  export_to_foreign_Consumers,
                  gross_output,
                  import_from_companies,
                  Import_from_Final_Consumers
                )
               )
      
        # Extract unique country names from column names
        unique_countries <- unique(gsub("_.*", "", colnames(merged_va_t_fd)))
   
      for (country in unique_countries) {
        YINV_cols <- grep(
          paste0("^", country, ".*_(Final_demand_P\\.51|Final_demand_P\\.52)"),
          colnames(merged_va_t_fd),
          value = TRUE
        )
        YPCE_cols <- grep(
          paste0("^", country, ".*_(Final_demand_P\\.3h|Final_demand_P\\.3n)"),
          colnames(merged_va_t_fd),
          value = TRUE
        )
        
        # Sum the columns for "YINV_cols" and update the dataframe
        if (length(YINV_cols) > 0) {
          # Calculate the sum for "YINV"
          YINV_sum <- rowSums(select(merged_va_t_fd, all_of(YINV_cols)), na.rm = TRUE)
          
          # Drop old "YINV" columns
          merged_va_t_fd <- merged_va_t_fd %>% select(-all_of(YINV_cols))
          
          # Insert the new "YINV" column at the appropriate position
          country_positions <- grep(paste0("^", country, "_"), colnames(merged_va_t_fd))
          insert_position <- if (length(country_positions) > 0) min(country_positions) else ncol(merged_va_t_fd) + 1
          merged_va_t_fd <- add_column(
            merged_va_t_fd,
            !!paste0(country, "_YINV") := YINV_sum,
            .before = insert_position
          )
        }
        
        # Sum the columns for "YPCE" and update the dataframe
        if (length(YPCE_cols) > 0) {
          # Calculate the sum for "YPCE"
          YPCE_sum <- rowSums(select(merged_va_t_fd, all_of(YPCE_cols)), na.rm = TRUE)
          
          # Drop old "YPCE" columns
          merged_va_t_fd <- merged_va_t_fd %>% select(-all_of(YPCE_cols))
          
          # Insert the new "YPCE" column at the appropriate position
          country_positions <- grep(paste0("^", country, "_"), 
                                    colnames(merged_va_t_fd))
          
          insert_position <- if (length(country_positions) > 0) 
            min(country_positions) else 
              ncol(merged_va_t_fd) + 1
          
          merged_va_t_fd <- add_column(
            merged_va_t_fd,
            !!paste0(country, "_YPCE") := YPCE_sum,
            .before = insert_position
          )
        }
      }
      
      # Identify unique countries from the `country` column
      unique_countries <- unique(merged_va_t_fd$country)
      merged_va_t_fd <- merged_va_t_fd %>%
      rowwise() %>%
      mutate(across(-c(Industry, sector, country, Year, code, YIMP, YEXP), ~ ifelse(str_detect(cur_column(), country), ., 0)))
      merged_va_t_fd <- as.data.frame(merged_va_t_fd)
      
      #Save IOT table
      output_final <- file.path(base_dir, paste0(year, "_", type, "_IOT.csv"))
      write_csv(merged_va_t_fd, output_final)
      message("Updated input-output table with country and industry column names saved as: ", output_final)
        
      merged_va_t_fd <- merged_va_t_fd %>%
        # select(-c(ROW_YPCE,
        #           ROW.1_YPCE,
        #           ROW.2_YGOV,
        #           ROW.3_YINV,
        #           ROW.4_YINV)) %>% 
        
        
        #remove rest of the world
      pivot_longer(
          cols = -c(
            Industry,
            sector, 
            country, 
            Year, 
            code), 
            names_to = "COL",                              
            values_to = "var5"                             
          ) %>% 
        rename(ROW = code,
               YEAR = Year,
               COUNTRY = country
               ) %>%
        select(YEAR, COUNTRY, ROW, COL, var5) %>%
        mutate(var5 = round(var5, 2)) %>% 
        filter(var5 != 0) %>% 
        rowwise() %>%
        mutate(
          COL = case_when(
            str_detect(COL, "\\.\\d+_") ~ str_sub(COL, 7),  
            str_detect(COL, "_") ~ str_sub(COL, 5),         
            TRUE ~ COL          
          )
        ) %>%
        ungroup()
   
      
      
      #................Apply RAS to balance total export and total import....................
      
      #Extract the TZ columns and convert to matrix
      # pattern <- paste(sector_code, collapse = "|")
      # tz_columns <- grep(pattern, colnames(merged_va_t_fd), value = TRUE)
      # TZ_matrix <- as.matrix(merged_va_t_fd[, tz_columns])
      # target_row_sums <- merged_va_t_fd$Total_Export
      # target_col_sums_TZ <- merged_va_t_fd$Total_Export[1:ncol(TZ_matrix)]
      # 
      # # Apply the RAS algorithm to adjust TZ columns
      # adjusted_TZ_matrix <- ras_algorithm(TZ_matrix, rowSums(TZ_matrix), target_col_sums_TZ)
      # merged_va_t_fd[, tz_columns] <- adjusted_TZ_matrix
      # 
      # #Adjust the FD columns to match the residuals after TZ adjustment
      # FD_matrix <- as.matrix(merged_va_t_fd[, final_demand_columns])
      # residual_row_sums <- target_row_sums - rowSums(adjusted_TZ_matrix)
      # FD_row_sums <- rowSums(FD_matrix)
      # scaling_factors <- ifelse(FD_row_sums == 0, 0, residual_row_sums / FD_row_sums)
      # adjusted_FD_matrix <- diag(scaling_factors) %*% FD_matrix
      # merged_va_t_fd[, final_demand_columns] <- adjusted_FD_matrix

      # Save the final dataset with the updated column names
      output_final <- file.path(base_dir, paste0(year, "_", type, "_INPUT_OUTPUT.csv"))
      write_csv(merged_va_t_fd, output_final)
      message("Updated input-output table with country and industry column names saved as: ", output_final)
      
      # #................Difference between original and adjusted matrices..........................
      # 
      # # Calculate the difference for TZ columns
      # TZ_diff_matrix <- TZ_matrix - adjusted_TZ_matrix
      # 
      # # Calculate the difference for FD columns
      # FD_diff_matrix <- FD_matrix - adjusted_FD_matrix
      # 
      # # Create dataframes for differences
      # TZ_diff_df <- as.data.frame(TZ_diff_matrix)
      # FD_diff_df <- as.data.frame(FD_diff_matrix)
      # 
      # # Assign appropriate column names
      # colnames(TZ_diff_df) <- colnames(merged_va_t_fd[, tz_columns])
      # colnames(FD_diff_df) <- colnames(merged_va_t_fd[, final_demand_columns])
      # 
      # # Combine the differences into the original dataframe structure
      # difference_df <- merged_va_t_fd
      # difference_df[, tz_columns] <- TZ_diff_df
      # difference_df[, final_demand_columns] <- FD_diff_df
      # 
      # #Save the difference dataframe as a CSV file
      # output_diff_file <- file.path(base_dir, paste0(year, "_", type, "_IOT3_difference.csv"))
      # write_csv(difference_df, output_diff_file)
      # message("Difference file saved as: ", output_diff_file)


      #.... This part of code was generated to compute the Supply use table but not used as Silvia decided to use margins from EORA..
      # # Function to calculate the row sum of selected columns
      # calculate_selected_sum <- function(row, selected_cols) {
      #   sum(as.numeric(row[selected_cols]), na.rm = TRUE)
      # }
      #
      # # Select columns that contain "TZ"
      # UseTable_interm_cons <- merged_va_t_fd %>%
      #   select(matches("TZ", ignore.case = TRUE)) %>%
      #   select(-TZA_Final_demand_P.3h,
      #          -TZA.1_Final_demand_P.3n,
      #          -TZA.2_Final_demand_P.3g,
      #          -TZA.3_Final_demand_P.51,
      #          -TZA.4_Final_demand_P.52,
      #          -TZA.5_Final_demand_P.53)
      #
      # # Calculate the row sum for the "TZ" columns
      # UseTable_interm_cons$Total_interm_cons <- apply(UseTable_interm_cons, 1, calculate_selected_sum,
      #                                                 selected_cols = colnames(UseTable_interm_cons))
      #
      # # Select and calculate the rows for the first part of final consumption
      # UseTable_final_cons1 <- merged_va_t_fd %>%
      #   select(matches("Final_demand_P.3h|Final_demand_P.3n|Final_demand_P.3g", ignore.case = TRUE))
      #
      # UseTable_final_cons1$Total_final_cons1 <- apply(UseTable_final_cons1, 1, calculate_selected_sum,
      #                                                 selected_cols = colnames(UseTable_final_cons1))
      # UseTable_final_cons1 <- UseTable_final_cons1 %>%
      # mutate(Total_final_cons1 = ifelse(row_number(UseTable_interm_cons) > 4915, 0, Total_final_cons1))
      #
      # # Select and calculate the rows for the second part of final consumption
      # UseTable_final_cons2 <- merged_va_t_fd %>%
      # select(matches("Final_demand_P.51|Final_demand_P.52|Final_demand_P.53", ignore.case = TRUE))
      #
      # UseTable_final_cons2$Total_final_cons2 <- apply(UseTable_final_cons2, 1, calculate_selected_sum,
      #                                                 selected_cols = colnames(UseTable_final_cons2))
      #
      # UseTable_final_cons2 <- UseTable_final_cons2 %>%
      # mutate(Total_final_cons2 = ifelse(row_number(UseTable_interm_cons) > 4915, 0, Total_final_cons2))
      #
      # # Combine the results into a final use table
      # usetable <- UseTable_interm_cons %>%
      #   cbind(UseTable_final_cons1, UseTable_final_cons2,
      #         Industry = merged_va_t_fd$Industry,
      #         sector = merged_va_t_fd$sector,
      #         country = merged_va_t_fd$country,
      #         Year = merged_va_t_fd$Year,
      #         Total_Export = merged_va_t_fd$Total_Export) %>%
      #   as.data.frame() %>%
      #   mutate(total_use = Total_interm_cons + Total_final_cons1 + Total_final_cons2 +
      #            Total_Export) %>%
      #   select(Industry, country, sector, Year, everything())
      
      # # Save the final merged dataset with FD
      # output_final <- file.path(base_dir, paste0(year, "_", type, "_UseTs.csv"))
      # write_csv(usetable, output_final)
      # message("Use tables saved as: ", output_final) #remove the this code chunk
      
  
      
    } else {
      message("FD data for ", type, " is not available.")
    }
  }
  
  # Delete CSV files to avoid merging all together
  file_delete(csv_files)
  message("Deleted files: ", paste(csv_files, collapse = ", "))
}

lapply(folders, process_folder)


