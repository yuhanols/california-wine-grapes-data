# Author: Yuhan Wang <onewang@ucdavis.edu>

# install.packages('writexl', repos="https://cloud.r-project.org")
# install.packages('readxl', repos="https://cloud.r-project.org")
library(writexl)
library(readxl)
getwd()

# Year range of non-acreage data
FIRST_YEAR <- as.numeric(1991)
LAST_YEAR <- as.numeric(2023)

# Year range of acreage data
FIRST_YEAR_ACREAGE <- as.numeric(1994)
LAST_YEAR_ACREAGE <- as.numeric(2022)



## Root data directory from Stage 1's data  output
## For example crush_data_crawler/20240412
DATA_ROOT <- "" # REPLACE ME

## Output Excel filename
## For example 20240412.xlsx
OUTPUT_FILENAME <- "" # REPLACE ME

## Use command line arguments when they are available
args <- commandArgs()
if (length(args) == 7) {
  DATA_ROOT <- args[6]
  OUTPUT_FILENAME <- args[7]
}

## Add Total All Varieties for Total Acreage, Bearing Acreage and Non-bearing Acreage

# Mapping district indices to display names of California wine crush districts
DISTRICT_NAMES <- c("1:Mendocino",
                    "2:Lake",
                    "3:Sonoma/Marin", # "3:Sonoma",
                    "4:Napa",
                    "5:Solano",
                    "6:Bay Area",
                    "7:Monterey/S. Ben", # "7:Monterey",
                    "8:S. Barbara/SLO/Ven", # "8:Santa Barbara",
                    "9:North",
                    "10:Sierra Foothills", # "10:Sierra",
                    "11:Sacramento/S. Jqn", # "11:Sacramento",
                    "12:Merced/Stan./S. Jqn", # "12:Stanislaus",
                    "13:Fresno+", # "13:Fresno",
                    "14:Kern+", # "14:Kern",
                    "15:Los Angeles/S. Ber", # "15:Los Angeles",
                    "16:South",
                    "17:Yolo",
                    "California")

reshape_one_file <- function(my_path,i,unit_string,var_name) {
  current_path <- sprintf("%s/%d.csv", my_path, i)
  
  current <- read.csv(current_path)
  
  colnames(current)[1]<-"variety"
  colnames(current)[2]<-"category"
  district_id <- 1
  while (district_id <=18) {
    colnames(current)[district_id + 2] <-DISTRICT_NAMES[district_id]
    colnames(current)[district_id + 2] <-DISTRICT_NAMES[district_id]
    district_id  <- district_id + 1
  }
  
  current_L<- reshape(data=current, idvar="variety", 
                      varying = DISTRICT_NAMES,
                      v.name=c("value"),
                      timevar = "district",
                      times = DISTRICT_NAMES,
                      new.row.names = 1:1000,
                      direction="long")
  
  current_L$unit <- unit_string
  current_L$year <- sprintf("%d",i)
  current_L$variable <-var_name
  return(current_L)
}

reshape_one_var <- function(data_folder_path,start_year,end_year,unit_string,var_name){
  total <- c()
  year <- start_year
  while (year <= end_year) {
    current_L <- reshape_one_file(data_folder_path,year,unit_string,var_name)
    total <- rbind(total, current_L)
    year <- year + 1
  }
  return(total)
}

add_total_variety_to_one_acreage_data_file <- function(input_path, output_path, year) {
  acreage <- read.csv(sprintf("%s/%d.csv", input_path, year))
  rownames(acreage) <- acreage[, 1]
  if ("total raisin" %in% rownames(acreage)) {
    acreage <- rbind(acreage, data.frame(Type.and.Variety = "total all varieties", Wine.Category = "na", acreage["total raisin", -1:-2] + acreage["total wine", -1:-2] + acreage["total table", -1:-2]))
  } else {
    acreage <- rbind(acreage, data.frame(Type.and.Variety = "total all varieties", Wine.Category = "na", acreage["total wine", -1:-2] + acreage["total table", -1:-2]))
  }
  rownames(acreage) <- c(rownames(acreage)[-1 * nrow(acreage)], c("total all varieties"))
  write.csv(acreage, sprintf("%s/%d.csv", output_path, year), row.names = FALSE)
}

add_total_variety_to_acreage_data <- function(input_path, output_path, start_year,end_year) {
  dir.create(output_path, showWarnings = TRUE)
  year <- start_year
  while (year <= end_year) {
    current_L <- add_total_variety_to_one_acreage_data_file(input_path, output_path, year)
    year <- year + 1
  }
}

## Step 1 Add total all varieties for bearing/non bearing/total acreage

dir.create(sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal"), showWarnings = TRUE)
add_total_variety_to_acreage_data(sprintf("%s/%s", DATA_ROOT, "Acreage/total"), sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal/total"), FIRST_YEAR_ACREAGE, LAST_YEAR_ACREAGE)
add_total_variety_to_acreage_data(sprintf("%s/%s", DATA_ROOT, "Acreage/bearing"), sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal/bearing"), FIRST_YEAR_ACREAGE, LAST_YEAR_ACREAGE)
add_total_variety_to_acreage_data(sprintf("%s/%s", DATA_ROOT, "Acreage/non_bearing"), sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal/non_bearing"), FIRST_YEAR_ACREAGE, LAST_YEAR_ACREAGE)

# Step 2 Re-shape 

price_folder <- sprintf("%s/%s", DATA_ROOT, "Price")
price_total <- reshape_one_var(price_folder,FIRST_YEAR,LAST_YEAR,"$/ton","price")


volume_crushed_folder <- sprintf("%s/%s", DATA_ROOT, "Volume")
volume_crushed_total <- reshape_one_var(volume_crushed_folder,FIRST_YEAR,LAST_YEAR,"tons","crushed volume")


volume_purchased_folder <- sprintf("%s/%s", DATA_ROOT, "PurchasedVolume")
volume_purchased_total <- reshape_one_var(volume_purchased_folder,FIRST_YEAR,LAST_YEAR,"tons","purchased volume")


average_brix_purchased_folder <- sprintf("%s/%s", DATA_ROOT, "PurchasedDegreeBrix")
average_brix_purchased_total <- reshape_one_var(average_brix_purchased_folder,FIRST_YEAR,LAST_YEAR,"degree brix","average brix purchased")


average_brix_crushed_folder <- sprintf("%s/%s", DATA_ROOT, "DegreeBrix")
average_brix_crushed_total <- reshape_one_var(average_brix_crushed_folder,FIRST_YEAR,LAST_YEAR,"degree brix","average brix crushed")


bearing_acreage_folder <-sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal/bearing")
bearing_acreage_total <- reshape_one_var(bearing_acreage_folder,FIRST_YEAR_ACREAGE,LAST_YEAR_ACREAGE,"acres","bearing acreage")


non_bearing_acreage_folder <-sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal/non_bearing")
non_bearing_acreage_total <- reshape_one_var(non_bearing_acreage_folder,FIRST_YEAR_ACREAGE,LAST_YEAR_ACREAGE,"acres","non-bearing acreage")

total_bearing_acreage_folder <-sprintf("%s/%s", DATA_ROOT, "AcreageWithTotal/total")
total_bearing_acreage_total <- reshape_one_var(total_bearing_acreage_folder,FIRST_YEAR_ACREAGE,LAST_YEAR_ACREAGE,"acres","total acreage")

combined_data <- rbind(price_total,volume_crushed_total,volume_purchased_total,average_brix_purchased_total,average_brix_crushed_total,bearing_acreage_total,non_bearing_acreage_total,total_bearing_acreage_total)
## View data in RStudio only
## View(combined_data)

filename <- sprintf("%s/%s", DATA_ROOT, OUTPUT_FILENAME)
write_xlsx(combined_data, filename)
df <- read_xlsx(filename)

## View data in RStudio only
## View(df)

