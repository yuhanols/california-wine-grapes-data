# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import sys
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from collections import defaultdict
from collections import OrderedDict
import os
import zipfile
import csv
import pandas as pd
from pathlib import Path

USDA_NASS_CA_CRUSH_REPORT_URL = "https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush/Reports/index.php"
CRUSH_ZIP_RELATIVE_URL_REGEX_PATTERN = r"\.\./(?P<type>.*)/(?P<year>[0-9]{4})/.*\.zip"
ERRATA_TYPE = "Errata"
FINAL_TYPE = "Final"
RAW_DATA_DIR = "VolumeRaw"
OUTPUT_DIR = "Volume"
FILE_POSTFIX = "02"

MAX_REGION_ID = 100
VARIETY = "VARIETY"
TYPE_AND_VARIETY = "Type and Variety"
WINE_CATEGORY = "Wine Category"
INTERESTED_GRAPE_NAMES = OrderedDict({
    "Chardonnay": "White",
    "Cabernet Sauvignon": "Red",
    "French Colombard": "White",
    "Zinfandel": "Red",
    "Pinot Gris": "White",
    "Pinot Noir": "Red",
    "Rubired": "Red",
    "Muscat of Alexandria": "White",
    "Merlot": "Red",
    "Sauvignon Blanc": "White",
    "Petite Sirah": "Red",
    "Syrah": "Red",
    "Barbera": "Red",
    "Grenache": "Red",
    "Chenin Blanc": "White",
    "Malbec": "Red",
    "Ruby Cabernet": "Red",
    "White Riesling": "White",
    "Petit Verdot": "Red",
    "Symphony": "White",
    "Total Raisin": "NA",
    "Total Red": "NA",
    "Total White": "NA",
    "Total Wine": "NA",
    "Total Table": "NA",
    "Total All Varieties": "NA",
})
INTERESTED_GRAPE_NAMES = {k.lower(): v.lower() for k, v in INTERESTED_GRAPE_NAMES.items()}



def get_all_zip_file_paths(url_containing_zips):
    # print(zips_page)
    zips_source = requests.get(url_containing_zips).text
    zip_soup = BeautifulSoup(zips_source, "html.parser")
    # in year -> (type, url) format
    # 创建一个空的字典，默认从一个东西映射到一个list，如果这个东西不存在，自动创造一个空的list还给你
    zip_url_dict = defaultdict(list)
    # 创建一个空的字典，没有默认的映射格式，可以从任何东西映射到任何东西，如果东西不存在你还非要拿，会crash
    # zip_url_dict = {}
    for zip_file_clickable_button in zip_soup.select('a[href*=".zip"]'):
        zip_relative_url = zip_file_clickable_button['href']
        zip_url_matched = re.match(CRUSH_ZIP_RELATIVE_URL_REGEX_PATTERN, zip_relative_url)
        report_type = zip_url_matched.group('type')
        year = int(zip_url_matched.group('year'))
        zip_url = urljoin(url_containing_zips, zip_relative_url)
        print('found_zip_file_path:[{}]year={},type={},url={}'.format(zip_file_clickable_button.text, year, report_type,
                                                                      zip_url))
        zip_url_dict[year].append((report_type, zip_url))

    # 2021 : [
    #    (Final, <USDA_URL>/Final/2021/gc_2021_final.zip),
    #],
    # 2020 : [
    #    (Final, <USDA_URL>/Final/2020/gc_2020_final.zip),
    #    (Errata, <USDA_URL>/Errata/2020/gc_2020_errata_xls.zip),
    #],
    # 2019 : [
    #    (Final, <USDA_URL>/Final/2019/gc_2019_final.zip),
    #    (Errata, <USDA_URL>/Errata/2019/gc_2019_errata_xls.zip),
    #]
    return zip_url_dict


def download_zip(target_path, year, zip_url, skip_download=False):
    filename = os.path.join(target_path, "{}.zip".format(year))
    if skip_download:
        return filename
    r = requests.get(zip_url)
    with open(filename, 'wb') as zipFile:
        zipFile.write(r.content)
    return filename


def unzip_files(unzip_target_directory, path_to_zip_file):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(unzip_target_directory)
    return unzip_target_directory


def extract_data_from_excel(unzipped_dir_for_year):
    all_files = [f for f in os.listdir(unzipped_dir_for_year) if os.path.isfile(os.path.join(unzipped_dir_for_year, f))]
    print("all files", all_files)
    all_excel_files = [f for f in all_files if (f.lower().endswith("xls") or f.lower().endswith("xlsx"))]
    print("all_excel_files", all_excel_files)
    files_ends_with_postfix = [f for f in all_excel_files if Path(f).stem.endswith(FILE_POSTFIX)]
    print("file_ends_with_{}".format(FILE_POSTFIX), files_ends_with_postfix)
    if len(files_ends_with_postfix) > 1:
        print("More than one files end with {}".format(FILE_POSTFIX))
        return None
    if len(files_ends_with_postfix) == 0:
        print("No file ends with {}".format(FILE_POSTFIX))
        return None
    # file_ends_with_postfix_path -> Volume/2002/XXXXgcbtb02.xls
    file_ends_with_postfix_path = os.path.join(unzipped_dir_for_year, files_ends_with_postfix[0])
    print("Parsing...", file_ends_with_postfix_path)
    data_frame = pd.read_excel(file_ends_with_postfix_path, sheet_name=0)
    num_rows, num_cols = data_frame.shape
    print("Shape: ", data_frame.shape)
    # find where are the type and variety headers
    # (1, 2), (3, 5)
    type_and_variety_locations = []
    for col in range(num_cols):
        for row in range(num_rows):
            value = str(data_frame.iat[row, col])
            if (TYPE_AND_VARIETY.lower() in value.lower()) or (VARIETY.lower() in value.lower()):
                if abs(row - num_rows) < 3:
                    print("Discard header position less than 3 cells away from bottom edge", (row, col))
                    continue
                type_and_variety_locations.append((row, col))
    # make sure we found something
    if len(type_and_variety_locations) == 0:
        print("Did not find {} location".format(TYPE_AND_VARIETY))
    # find grape by name
    # chardonnay : [
    #    (1, 235.35),
    #    (2, 0),
    #    (3, 0),
    #    (4, 34.5),
    #]
    grape_production_data = defaultdict(list)
    # A in B -> A是不是在B里面
    # for a in B -> 遍历B里面的所有值，遍历时使用的变量名为a
    # 把每个表头都看一遍
    max_col_header = max([header[1] for header in type_and_variety_locations])
    for row_header, col_header in type_and_variety_locations:
        for row_grape in range(row_header, num_rows):
            grape_name = str(data_frame.iat[row_grape, col_header])
            matched_grape_name = None
            for interested_grape_name in INTERESTED_GRAPE_NAMES.keys():
                if interested_grape_name.lower() in grape_name.lower():
                    matched_grape_name = interested_grape_name.lower()
            if matched_grape_name is not None:
                parsed_total_data_for_this_year = False
                for col_production in range(col_header + 1, num_cols):
                    region_id = data_frame.iat[row_header, col_production]
                    discard_value = False
                    try:
                        region_id = int(region_id)
                        if region_id > MAX_REGION_ID:
                            discard_value = True
                    except ValueError:
                        # header is no longer number, so we are no longer in region codes
                        discard_value = True
                    if discard_value:
                        # Only take the state total for this year if we are at max_col_header
                        if max_col_header == col_header and not parsed_total_data_for_this_year:
                            region_id = MAX_REGION_ID
                            parsed_total_data_for_this_year = True
                        else:
                            break
                    region_production_tons = float(str(data_frame.iat[row_grape, col_production]).replace(",", "")
                                                   .replace("--", "0.0"))
                    grape_production_data[matched_grape_name].append((region_id, region_production_tons))
    for key in grape_production_data.keys():
        grape_production_data[key].sort(key=lambda x: x[0])
    return grape_production_data


def select_url_based_on_available_types(types_and_urls):
    """
    Select target URL from (type, url) list
    :param types_and_urls: list of (type, url) tuples
    :return: ERRATA_TYPE URL if Errata type is available, or FINAL_TYPE url, or None if input list is empty
    """
    selected_url = None
    for url_type, url in types_and_urls:
        selected_url = url
        if url_type == ERRATA_TYPE:
            break
    return selected_url


def crawl():
    if len(sys.argv) < 5:
        print(
            "Not enough arguments, needed at least 5, with 'begin_year' in YYYY, and 'end_year' in YYYY, data_root as string, skip download as True|False")
        return 1
    begin_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    data_root = str(sys.argv[3])
    skip_download = str(sys.argv[4]) == "True"
    print("Step 0 Creating data root at ", data_root)
    os.makedirs(data_root, exist_ok=True)
    raw_data_root = os.path.join(data_root, RAW_DATA_DIR)
    os.makedirs(raw_data_root, exist_ok=True)
    print("Step 1 Parsing website data")
    zip_url_dict = get_all_zip_file_paths(USDA_NASS_CA_CRUSH_REPORT_URL)
    for year, types_and_urls in sorted(zip_url_dict.items()):
        print(year, types_and_urls)
    print("Step 2 Downloading ZIP files for selected years")
    # in (year, path) format
    # [(2020, XXX/Volume/2020.zip), (2021, XXX/Volume/2021.zip)]
    zip_file_local_paths = []
    for year in range(begin_year, end_year + 1):
        if year not in zip_url_dict:
            print("{} not in parsed zip url list".format(year))
            return 2
        types_and_urls = zip_url_dict[year]
        selected_url = select_url_based_on_available_types(types_and_urls)
        downloaded_zip_path = download_zip(raw_data_root, year, selected_url, skip_download=skip_download)
        zip_file_local_paths.append((year, downloaded_zip_path))
        print("Downloaded...", downloaded_zip_path)
    print("Step 3 unzipping data")
    unzipped_excel_files = []
    # [(2020, "2020.zip"), (2021, "2021.zip")]
    for year, path_to_zip_file in zip_file_local_paths:
        print("Unzipping...", path_to_zip_file)
        unzipped_dir_for_year = os.path.join(raw_data_root, "{}".format(year))
        unzip_files(unzipped_dir_for_year, path_to_zip_file)
        unzipped_excel_files.append((year, unzipped_dir_for_year))
    print("Step 4 extract data from excels")
    grape_data_by_year = []
    # [(2020, "XXX/Volume/2020"), (2021, "XXXX/Volume/2021")]
    for year, unzipped_dir_for_year in unzipped_excel_files:
        print("Parsing...", unzipped_dir_for_year)
        grape_data_this_year = extract_data_from_excel(unzipped_dir_for_year)
        if grape_data_this_year is None:
            raise ValueError("grape_data_this_year is None")
        grape_data_by_year.append((year, grape_data_this_year))
    csv_data_root = os.path.join(data_root, OUTPUT_DIR)
    os.makedirs(csv_data_root, exist_ok=True)
    print("Step 5 write data to OUTPUT_DIR")
    for year, grape_data_this_year in grape_data_by_year:
        csv_filename = os.path.join(csv_data_root, "{}.csv".format(year))
        print("Writing to {}".format(csv_filename))
        grape_data_this_year_dict = []
        header = [TYPE_AND_VARIETY, WINE_CATEGORY]
        for key, value in grape_data_this_year.items():
            if key not in INTERESTED_GRAPE_NAMES:
                raise ValueError("key {} not found in INTERESTED_GRAPE_NAMES, check your parser step".format(key))
            this_grape_dict = {TYPE_AND_VARIETY: key, WINE_CATEGORY: INTERESTED_GRAPE_NAMES[key]}
            for region_id, production_quantity_tons in value:
                if str(region_id) == str(MAX_REGION_ID):
                    region_id = "California"
                if str(region_id) not in header:
                    header.append(str(region_id))
                this_grape_dict[str(region_id)] = "{:.1f}".format(production_quantity_tons)
            grape_data_this_year_dict.append(this_grape_dict)
        interested_grape_names_lower = [x.lower() for x in INTERESTED_GRAPE_NAMES.keys()]
        grape_data_this_year_dict = sorted(grape_data_this_year_dict, key=lambda x: interested_grape_names_lower.index(x[TYPE_AND_VARIETY]))
        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(grape_data_this_year_dict)
    print("Done")
    return 0

