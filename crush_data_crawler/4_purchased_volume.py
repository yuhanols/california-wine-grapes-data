# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import crush_data_crawler_lib as crush

crush.RAW_DATA_DIR = "PurchasedVolumeRaw"
crush.OUTPUT_DIR = "PurchasedVolume"
crush.FILE_POSTFIX = "04"

if __name__ == "__main__":
    crush.crawl()
