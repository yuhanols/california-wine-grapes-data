# California Wine Grap Data Processing

This is the data processing pipeline backing the [California Wine Grapes](https://asmith.ucdavis.edu/data/ca-wine-grapes) data visualization

The library that processes California Wine Grape data from USDA website

USDA's National Agricultural Statistics Service
California Field Office

* [Grape Crush Reports Listing](https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush/Reports/index.php)

* [Grape Acreage Reports Listing](https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Acreage/Reports/)

The data processing pipeline is split into 2 stages

### Stage 1: A Python script to download and preprocess each type of data (Acreage, Price, Purchased Volume, Purchased Degree Brix, Volume Crushed, Degree Brix)

1. Download the USDA web page, and parse the web page source code to get a list of URLs to each year's dataset, if errata presents for a year, use the errata dataset instead
2. Download all ZIP files directly from USDA website, extract Excel spreadsheets from the ZIP files, and save them in a set of temporary folders divided by year
3. Iterate over each year's Excel spreadsheets and extract data from them into a Python in-memory list only for a list of "Interested Grape Names"
   * USDA's data format differs slightly from year-to-year and hence a lot of pattern matching is required to make Python script work for every year's data
   * Adding more "Interested Grape Names" may become tricky. Since a new type of grape may expose another format irregularities in USDA's Excel spreadsheet and require some special code to handle it.
4. Save the extracted data into a list of CSV files named by year into a dedicated folder for each data type (e.g. acreage/2001.csv)

### Stage 2: An R script to reshape individual CSV into a format suitable for our website

1. For each type of data, read CSV files for each year and save them as R variables
2. Flatten the data with "type of data" as a column
3. Row combine all data into a flattened dataset containing all six types of data mentioned above
4. Save it as an Excel spreadsheet for graphing and downloading needs

## More detailed steps to run the scripts using macOS

### Step 1 Install Git, Python and R

If you are using macOS, recommend to use Homebrew to install them instead of doing it manually 

Install Homebrew using macOS Terminal:

```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Install python

```shell
brew install python
```

Install git

```shell
brew install git
```

Install R

```shell
brew install r
```

Install pip

```shell
python -m ensurepip --upgrade
```

### Step 2 Clone this repository

Open your Terminal, cd into your working directory

```shell
cd <working-directory>
```

Clone this repository

```shell
git clone https://github.com/yuhanols/california-wine-grapes-data.git
```

CD into it

```shell
cd california-wine-grapes-data
```

### Step 3 Create Virtual Environment and Install Python Dependencies

Following command will create a local virtual environment under `venv/` directory

```shell
python3 -m venv ./venv
```

Use the virtual env

```shell
source ./venv/bin/activate
```

Install dependencies

```shell
python3 -m pip install setuptools beautifulsoup4 bs4 openpyxl urllib3 pandas numpy requests et-xmlfile xlrd
```

### Step 4 Let's crawl some data!

All scripts following the same command line argument style

```shell
script.py <start_year> <end_year> <raw_data_output_directory> <True|False - whether to skip downloading raw data>
```

Create an output directory using YYYYMMDD format so that you can save today's copy and revise later

```shell
mkdir -p ./output/YYYYMMDD
```

2_volume

```shell
crush_data_crawler/2_volume.py 1991 2023 ./output/YYYYMMDD True
```

This will create

* `output/YYYYMMDD/VolumeRaw` that contains raw data from USDA website
* `output/YYYYMMDD/Volume` that contains standardized volume crushed data for each year for each California district

Similarly, do that for other types of data

```shell
crush_data_crawler/2_volume.py 1991 2023 ./output/YYYYMMDD True
```

3_degree_brix

```shell
crush_data_crawler/3_degree_brix.py 1991 2023 ./output/YYYYMMDD True
```
4_purchased_volume

```shell
crush_data_crawler/4_purchased_volume.py 1991 2023 ./output/YYYYMMDD True
```
5_purchased_degree_brix

```shell
crush_data_crawler/5_purchased_degree_brix.py 1991 2023 ./output/YYYYMMDD True
```

6_price

```shell
crush_data_crawler/6_price.py 1991 2023 ./output/YYYYMMDD True
```

All above data set shared the same processing library [crush_data_crawler_lib.py](crush_data_crawler/crush_data_crawler_lib.py)
as they are all downloaded from the same USDA data set. 

For 12_acreage data, a new crawler is needed as it is downloaded from alternative source

```shell
crush_data_crawler/12_acreage.py 1991 2023 ./output/YYYYMMDD True
```

### Step 5 Reshape data into a single dataset

This step was written in R, hence `Rscript` is needed to run it, 

For first time only, install packages for R, this will take some time

```shell
Rscript -e 'install.packages(c("writexl", "readxl"), repos="https://cloud.r-project.org")'
```

Then run the actual script for subsequent times

```shell
Rscript crush_data_reshape/reshape_total.R ./output/YYYYMMDD YYYYMMDD.xlsx
```

This will use the data in `output/YYYYMMDD`, flatten all data into a single dataframe, and write it to `output/YYYYMMDD.xlsx`
which can be used for further data analysis

### Tips for debugging data pipeline

Avoid downloading raw data multiple times by setting last argument to `crush_data_crawler` as `False`, such as

```shell
crush_data_crawler/6_price.py 1991 2023 ./output/YYYYMMDD False
```

This will reuse `./output/YYYYMMDD/PriceRaw` and avoid downloading it multiple times

### Copyright

This is first written by [Yuhan Wang](https://are.ucdavis.edu/people/grad-students/phd/yuhan-wang/) of University of California, Davis in 2022