# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import crush_data_crawler_lib as crush

crush.RAW_DATA_DIR = "PriceRaw"
crush.OUTPUT_DIR = "Price"
crush.FILE_POSTFIX = "06"

if __name__ == "__main__":
    crush.crawl()
