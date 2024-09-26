# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import pathlib
import shutil
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

USDA_NASS_CA_ACREAGE_REPORT_URL = "https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Acreage/Reports/"

MAX_DISTRICT_ID = 17
TOTAL_DISTRICT_ID = 100
NEW_HEADER = "Type and Variety"
NEW_DISTRICT_REGEX = r"district\s*(?P<district>[0-9]+)"

MID_HEADER = "Empty"
MID_DISTRICT_REGEX = r"dist\s+(?P<district>\d+)\s+\d+\s+[a-z]+"

OLD_HEADER = "VARNAME"
OLD_DISTRICT_REGEX = r"d(?P<district>[0-9]+)(?P<type>[a-z]+)(?P<year>[0-9]+)"

BEARING_INDEX = 0
NON_BEARING_INDEX = 1
TOTAL_INDEX = 2

TYPE_INDEX_DICT = {
    BEARING_INDEX: "bearing",
    NON_BEARING_INDEX: "non_bearing",
    TOTAL_INDEX: "total"
}

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
})
INTERESTED_GRAPE_NAMES = {k.lower(): v.lower() for k, v in INTERESTED_GRAPE_NAMES.items()}


def get_all_zip_file_paths(url_containing_zips):
    # print(zips_page)
    zips_source = requests.get(url_containing_zips).text
    zip_soup = BeautifulSoup(zips_source, "html.parser")
    # in year -> (type, url) format
    zip_url_dict = defaultdict(str)
    current_year = None
    for table_data in zip_soup.select('td'):
        stripped_data = table_data.text.strip()
        # Try to find if this is a year
        year = None
        try:
            year = int(stripped_data[:4])
        except ValueError:
            pass
        if year is not None:
            # Save year for next iteration
            current_year = year
            continue
        # Try to find Excel zip files
        if stripped_data != 'XLS' and stripped_data != 'XLSX':
            # Skip items that is not an Excel link nor year value
            continue
        # Now we have a clickable XLS link
        zip_file_clickable_button = table_data.contents[0]
        zip_relative_url = zip_file_clickable_button['href']
        if current_year is None:
            raise RuntimeError("current_year is None when processing {}".format(table_data))
        year = current_year
        current_year = None
        zip_url = urljoin(url_containing_zips, zip_relative_url)
        print('found_zip_file_path:[{}]year={},url={}'.format(zip_file_clickable_button.text, year, zip_url))
        zip_url_dict[year] = zip_url

    return zip_url_dict


def download_file(target_path, year, url, extension, skip_download=False):
    filename = os.path.join(target_path, "{}.{}".format(year, extension))
    if skip_download:
        return filename
    r = requests.get(url)
    with open(filename, 'wb') as zipFile:
        zipFile.write(r.content)
    return filename


def unzip_files(unzip_target_directory, path_to_zip_file):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(unzip_target_directory)
    return unzip_target_directory


def flatten_sheets_from_excel(source_path, destination_path):
    all_files = [f for f in os.listdir(source_path) if os.path.isfile(os.path.join(source_path, f))]
    all_excel_files = [f for f in all_files if (f.lower().endswith("xls") or f.lower().endswith("xlsx"))]
    for excel_file in all_excel_files:
        extension = pathlib.Path(excel_file).suffix.strip(". ").lower()
        full_excel_path = os.path.join(source_path, excel_file)
        engine = None
        if extension == 'xlsx':
            engine = "openpyxl"
        elif extension == 'xls':
            engine = "xlrd"
        xl = pd.ExcelFile(full_excel_path, engine=engine)
        if len(xl.sheet_names) == 1 or xl.sheet_names[0].strip().lower().replace(" ", "") == "sheet1":
            destination_file = os.path.join(destination_path, excel_file)
            print("Copying file from {} to {}".format(full_excel_path, destination_file))
            shutil.copyfile(full_excel_path, destination_file)
            continue
        for sheet in xl.sheet_names:
            # Force output extension to xlsx to be compatible with latest pandas release
            destination_file = os.path.join(destination_path, "{}.{}".format(sheet, "xlsx"))
            # print("Extracting file from {} to {}".format(full_excel_path, destination_file))
            df = pd.read_excel(xl, sheet_name=sheet)
            df.to_excel(destination_file, index=False)


def extract_data_from_excel(year, flattened_dir_for_year):
    pattern = "gabtb12"
    if year == 1994 or year >= 2022:
        pattern = "gabtb10"
    all_files = [f for f in os.listdir(flattened_dir_for_year) if os.path.isfile(os.path.join(flattened_dir_for_year, f))]
    print("all files", all_files)
    all_excel_files = [f for f in all_files if (f.lower().endswith("xls") or f.lower().endswith("xlsx"))]
    print("all_excel_files", all_excel_files)
    # also exclude temporary files that begins with ~
    files_contains_pattern = sorted([f for f in all_excel_files if (pattern in Path(f).stem.lower()) and not Path(f).stem.startswith("~")])
    print("files_contains_pattern", files_contains_pattern)
    if len(files_contains_pattern) == 0:
        raise RuntimeError("No file contains {} in {}".format(pattern, flattened_dir_for_year))
    grape_bearing_acreage_data = defaultdict(list)
    grape_non_bearing_acreage_data = defaultdict(list)
    grape_total_acreage_data = defaultdict(list)
    grape_acreage_data = [grape_bearing_acreage_data, grape_non_bearing_acreage_data, grape_total_acreage_data]
    for excel_file in files_contains_pattern:
        extension = pathlib.Path(excel_file).suffix.strip(". ").lower()
        engine = None
        if extension == 'xlsx':
            engine = "openpyxl"
        elif extension == 'xls':
            engine = "xlrd"
        full_path = os.path.join(flattened_dir_for_year, excel_file)
        print("Parsing...", full_path)
        data_frame = pd.read_excel(full_path, engine=engine, sheet_name=0, header=None, dtype=str)
        num_rows, num_cols = data_frame.shape
        print("Shape: ", data_frame.shape)
        # find where are the type and variety headers
        header_locations = []
        for col in range(num_cols):
            for row in range(num_rows):
                value = str(data_frame.iat[row, col])
                if (NEW_HEADER.lower() == value.lower().strip()) or (OLD_HEADER.lower() == value.lower().strip()):
                    header_locations.append((value.lower().strip(), row, col))
                else:
                    district_matched = re.match(MID_DISTRICT_REGEX, value.lower())
                    if district_matched is not None and col > 0:
                        previous_cell = str(data_frame.iat[row, col-1]).strip()
                        # print("matched at {},{}, previous cell {}".format(row, col, previous_cell))
                        if previous_cell == "nan" or len(previous_cell) == 0:
                            header_locations.append((MID_HEADER, row, col-1))
        # make sure we found something
        if len(header_locations) == 0:
            raise RuntimeError("Did not find header location in {}".format(full_path))
        # print(header_locations)
        # find grape by name
        for header, row_header, col_header in header_locations:
            matched_grape_name = None
            for row_grape in range(row_header, num_rows):
                grape_name = str(data_frame.iat[row_grape, col_header])
                matched_grape_name_this_header = None
                for interested_grape_name in INTERESTED_GRAPE_NAMES.keys():
                    if interested_grape_name.lower() in grape_name.lower():
                        matched_grape_name_this_header = interested_grape_name.lower()
                if matched_grape_name_this_header is None:
                    continue
                matched_grape_name = matched_grape_name_this_header
                district_id = None
                type_index = 0
                skip = False
                for col_acreage in range(col_header + 1, num_cols):
                    # skip the total column
                    if skip:
                        skip = False
                        continue
                    district_value = str(data_frame.iat[row_header, col_acreage]).strip().lower()
                    # print("district_value is {} at ({}, {})".format(district_value, row_header, col_acreage))
                    district_matched = re.match(NEW_DISTRICT_REGEX, district_value)
                    if district_matched is None:
                        district_matched = re.match(MID_DISTRICT_REGEX, district_value)
                        if district_matched is None:
                            district_matched = re.match(OLD_DISTRICT_REGEX, district_value)
                            if district_matched is None:
                                if "state total".lower() in district_value or district_value.startswith("dst"):
                                    district_id = TOTAL_DISTRICT_ID
                                # if district_matched is None:
                                #     print("Nothing is matched for {} at ({}, {})".format(district_value, row_header, col_acreage))
                    if district_matched is not None:
                        district_id = int(district_matched.group("district"))
                    value = str(data_frame.iat[row_grape, col_acreage]).strip().lower()
                    if district_id is None:
                        if len(value) == 0 or value == "nan":
                            print("No data found in this cell, skipping")
                            continue
                        raise RuntimeError("No district ID when parsing ({},{}) with value {} at {}"
                                           .format(row_grape, col_acreage, value, full_path))
                    if district_id != TOTAL_DISTRICT_ID and district_id > MAX_DISTRICT_ID:
                        raise RuntimeError("Matched district id {} larger than {}".format(district_id, MAX_DISTRICT_ID))
                    # Get rid of non-numeric characters
                    value = re.sub("[^\d\.]", "", value)
                    district_acres = float(value)
                    grape_acreage_data[type_index][matched_grape_name].append((district_id, district_acres))
                    if type_index == TOTAL_INDEX:
                        district_id = None
                        skip = True
                    type_index += 1
                    type_index %= 3
            if matched_grape_name is None:
                raise ValueError("Did not find any interested grape in {}".format(full_path))
    for type_data in grape_acreage_data:
        for key in type_data.keys():
            type_data[key].sort(key=lambda x: x[0])
    # print the database
    # for key, value in grape_production_data.items():
    #     print("======= Production Data for {} =========".format(key))
    #     for region_id, production_quantity_tons in value:
    #         print("Region: {}, Quantity: {:.1f} tons".format(region_id, production_quantity_tons))

    return grape_acreage_data


def extract_needed_data(in_memory_data):
    pass


def attach_data_to_master_sheet(new_data):
    pass


def main():
    if len(sys.argv) < 5:
        print(
            "Not enough arguments, needed at least 2, with 'begin_year' in YYYY, and 'end_year' in YYYY, data_root as string, skip download as True|False")
        return 1
    begin_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    data_root = str(sys.argv[3])
    skip_download = str(sys.argv[4]) == "True"
    print("Step 0 Creating data root at ", data_root)
    os.makedirs(data_root, exist_ok=True)
    crush_data_root = os.path.join(data_root, "AcreageRaw")
    os.makedirs(crush_data_root, exist_ok=True)
    print("Step 1 Parsing website data")
    zip_url_dict = get_all_zip_file_paths(USDA_NASS_CA_ACREAGE_REPORT_URL)
    for year, url in sorted(zip_url_dict.items()):
        print(year, url)
    print("Step 2 Downloading ZIP files for selected years")
    # in (year, path, extension) format
    downloaded_file_local_paths = []
    for year in range(begin_year, end_year + 1):
        if year not in zip_url_dict:
            print("{} not in parsed zip url list".format(year))
            return 2
        selected_url = zip_url_dict[year]
        extension = pathlib.Path(selected_url).suffix.lower().strip(" .")
        # Rename self-extract exe files into zip files
        if extension == "exe":
            extension = "zip"
        downloaded_file_path = download_file(crush_data_root, year, selected_url, extension, skip_download=skip_download)
        downloaded_file_local_paths.append((year, downloaded_file_path, extension))
        print("Downloaded...", downloaded_file_path)
    print("Step 3 unzipping data")
    unzipped_excel_files = []
    for year, path_to_file, extension in downloaded_file_local_paths:
        unzipped_dir_for_year = os.path.join(crush_data_root, "{}".format(year))
        if extension != "zip":
            os.makedirs(unzipped_dir_for_year, exist_ok=True)
            target_file = os.path.join(unzipped_dir_for_year, pathlib.Path(path_to_file).name)
            print("Copying from {} to {}".format(path_to_file, target_file))
            shutil.copyfile(path_to_file, target_file)
            unzipped_excel_files.append((year, unzipped_dir_for_year, extension))
            continue
        print("Unzipping {} to {}".format(path_to_file, unzipped_dir_for_year))
        unzip_files(unzipped_dir_for_year, path_to_file)
        unzipped_excel_files.append((year, unzipped_dir_for_year, extension))
    print("Step 3.5 Flatten Excels")
    flatten_excel_dirs = []
    for year, dir_path, extension in unzipped_excel_files:
        flatten_dir_path = os.path.join(dir_path, "flatten")
        print("flattening excel sheets from {} to {}".format(dir_path, flatten_dir_path))
        os.makedirs(flatten_dir_path, exist_ok=True)
        flatten_sheets_from_excel(dir_path, flatten_dir_path)
        flatten_excel_dirs.append((year, flatten_dir_path))

    print("Step 4 extract data from excels")
    grape_acreage_data_by_year = []
    for year, flattened_excel_dir_for_year in flatten_excel_dirs:
        print("Parsing...", flattened_excel_dir_for_year)
        grape_acreage_data_this_year = extract_data_from_excel(year, flattened_excel_dir_for_year)
        if grape_acreage_data_this_year is None:
            raise ValueError("grape_acreage_data_this_year is None")
        grape_acreage_data_by_year.append((year, grape_acreage_data_this_year))
    csv_data_root = os.path.join(data_root, "Acreage")
    os.makedirs(csv_data_root, exist_ok=True)
    print("Step 5 write data to Organized_Grape_Acreage_Data")
    for year, grape_acreage_data_this_year in grape_acreage_data_by_year:
        for type_index, dir_name in TYPE_INDEX_DICT.items():
            grape_acreage_typed_data_by_year = grape_acreage_data_this_year[type_index]
            csv_data_root_for_type = os.path.join(csv_data_root, dir_name)
            os.makedirs(csv_data_root_for_type, exist_ok=True)
            csv_filename = os.path.join(csv_data_root_for_type, "{}.csv".format(year))
            print("Writing to {}".format(csv_filename))
            grape_acreage_this_year_list = []
            header = [NEW_HEADER, WINE_CATEGORY]
            for grape_name, value in grape_acreage_typed_data_by_year.items():
                this_grape_dict = {}
                this_grape_dict[NEW_HEADER] = grape_name
                if grape_name not in INTERESTED_GRAPE_NAMES:
                    raise ValueError("grape name {} not found in INTERESTED_GRAPE_NAMES, check your parser step"
                                     .format(grape_name))
                this_grape_dict[WINE_CATEGORY] = INTERESTED_GRAPE_NAMES[grape_name]
                for district_id, acres in value:
                    if str(district_id) == str(TOTAL_DISTRICT_ID):
                        district_id = "California"
                    if str(district_id) not in header:
                        header.append(str(district_id))
                    this_grape_dict[str(district_id)] = "{:.1f}".format(acres)
                grape_acreage_this_year_list.append(this_grape_dict)
            interested_grape_names_lower = [x.lower() for x in INTERESTED_GRAPE_NAMES.keys()]
            grape_acreage_this_year_list = sorted(grape_acreage_this_year_list,
                                                    key=lambda x: interested_grape_names_lower.index(x[NEW_HEADER]))
            with open(csv_filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=header)
                writer.writeheader()
                writer.writerows(grape_acreage_this_year_list)
    print("Done")
    return 0


if __name__ == '__main__':
    main()
