# Download LODES Files From the Census Website

The Python script `download_lodes_data.py` is designed to download chunks of LODES data files from the [Census Bureau Website](https://lehd.ces.census.gov/data/#lodes). The script was written by Graham MacDonald and Vivian Zheng from the Urban Institute.

The LODES data files are large, so we recommend running the script on a computer with large memory and storage sizes. The variable `n_cores` at line 162 in the script specifies the number of processors that your computer will use while running the script. We have set the default to 1, but feel free to change it based on your computer settings. Technically, the more processors you set, the faster the script will run, but be careful about the memory limit of your computer. 

The script was written in Python 3, and was tested in Python 3.6.5. Please install Python packages that are specified in `requirements.txt` before running the script, and set the current working directory to specify the directory to store LODES files.

A log file will be automatically created in the working directory after running the script, recording the names of the datasets that are downloaded and possible error messages.

If you have any questions, please contact Vivian Zheng (https://twitter.com/vivianzheng0120).
