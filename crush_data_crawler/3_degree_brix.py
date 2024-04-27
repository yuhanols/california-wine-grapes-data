# Author: Yuhan Wang <onewang@ucdavis.edu>
# Developed in Python 3.9

import crush_data_crawler_lib as crush

crush.RAW_DATA_DIR = "DegreeBrixRaw"
crush.OUTPUT_DIR = "DegreeBrix"
crush.FILE_POSTFIX = "03"

if __name__ == "__main__":
    crush.crawl()
