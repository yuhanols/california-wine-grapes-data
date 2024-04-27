# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import crush_data_crawler_lib as crush

crush.RAW_DATA_DIR = "PurchasedDegreeBrixRaw"
crush.OUTPUT_DIR = "PurchasedDegreeBrix"
crush.FILE_POSTFIX = "05"

if __name__ == "__main__":
    crush.crawl()
