# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import crush_data_crawler_lib as crush

crush.RAW_DATA_DIR = "VolumeRaw"
crush.OUTPUT_DIR = "Volume"
crush.FILE_POSTFIX = "02"

if __name__ == "__main__":
    crush.crawl()