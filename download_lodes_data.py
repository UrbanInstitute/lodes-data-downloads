###############################################################################
#This script is designed to download chunks of the LODES data from the website.
#Code written by Graham MacDonald and Vivian Zheng from the Urban Institute. 
###############################################################################

#Install packages 
import requests
import gzip
import os
import multiprocessing
from multiprocessing import Pool, freeze_support 
from bs4 import BeautifulSoup
import shutil
import re
import datetime
import sys
import logging

########################################
# Define Functions
########################################

def create_directories():
	"""
	Function:
		Create a directory whose name starts with "LODES_Download_", set the newly created directory as working directory; 
		Create subfolders under the working directory based on the data types specified in "types". 
	Return: 
		Directories are created under current working directory, for storing downloaded CSV files. 
	"""
	#specify the Year of job data, Segment of the workforce, and Job Type 
	work_segs = ["S000","SA01","SA02","SA03","SE01","SE02","SE03","SI01","SI02","SI03"]
	work_types = ["JT00","JT01","JT02","JT03","JT04","JT05"]
	years = [2002, 2017]
	os.mkdir(new_dir_name)
	os.chdir(new_dir_name)
	for p in types:
	    os.mkdir(p)
	    if p == "od":
	    	for typ in work_types:
	    		os.mkdir(f"{p}/{typ}")
	    		for year in range(years[0], years[1] + 1):
	    			os.mkdir(f"{p}/{typ}/{year}")
	    else:
	   		for seg in work_segs:
	   			os.mkdir(f"{p}/{seg}")
	   			for typ in work_types:
	   				os.mkdir(f"{p}/{seg}/{typ}")
	   				for year in range(years[0], years[1] + 1):
	   					os.mkdir(f"{p}/{seg}/{typ}/{year}")


def get_links(vals):
	"""
	Function:
		Get download links for data files of a state"s specific group of LODES data. 
	Args:
		vals: a tuple, where the first item is the state acronym and the second is the group name.
			e.g., ("va", "rac") represents all RAC files for Virginia.
	Return:
		a list, where each item is a file"s download link of a specific state and data group. 
	"""
	form = [("version","LODES7")]
	url_main = "https://lehd.ces.census.gov/php/inc_lodesFiles.php"
	start_url = "https://lehd.ces.census.gov"
	f = dict(form + [("type",vals[1]),("state",vals[0])])
	r = requests.post(url_main, data = f)
	soup = BeautifulSoup(r.text, "lxml")
	a_link = [f"{start_url}{x['href']}" for x in soup.find("div", {"id":"lodes_file_list"}).find_all("a")]
	return a_link


def download_file(url):
    # Modified from http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    """
    Function:
        Download files using the download links from the function get_links, unzip the gzip files to csv files, 
        and remove the gzip files in the folders. 
    Args:
        url: the download links for datasets from function get_links. 
    Return:
        Downloaded CSV files in the local folders. 
    """
    pattern = "20[0-1][0-9]"
    fname = url.split("/")[-1]
    points = fname.split(".")[0].split("_")
    year = re.findall(pattern, fname)[0]
    if len(points) < 5:
        print("The file name in the download link is not correct. Please double check.")
        return None
    else:
        if points[1] == "od": loc = f"{points[1]}/{points[3]}/{year}/"
        else: loc = f"{points[1]}/{points[2]}/{points[3]}/{year}/"
        r = requests.get(url, stream=True)
        f= open(f"{loc}{fname}", "wb")
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk:
                f.write(chunk)
        f.close()
        unzip_file(fname, loc)
    return fname


def unzip_file(fname,loc):
    """
    Function: 
        Convert gzip files to CSV files, and remove the gzip files in the local folders.
    Args:
        fname: the name of the gzip file
        loc: the location of the gzip files 
    Return: 
        The CSV files that are converted from the gzip files 
    """
    inF = gzip.open(f"{loc}{fname}", "rb")
    outF = open(f"{loc}{fname.replace('.gz','')}", "wb")
    outF.write( inF.read() )
    inF.close()
    outF.close()
    os.remove(f"{loc}{fname}")


def process_files():
	"""
	function: 
		Specify the state and data type to donwload, call function get_links to get download links, 
		and call download_file to download and unzip files.
	return: 
		The downloaded and unzipped files are stored in the specified folders. 
	"""
	combos = []
	for i in states:
		for j in types:
			combos.append((i, j))

	for i in combos:
		print(f"\nDownloading {i[1].upper()} data for state {i[0].upper()}")
	#Call get_links function to get download links
	p = multiprocessing.Pool(n_cores)
	results = p.map(get_links, combos)
	flat_results = [x for y in results for x in y]
	
	#Call download_file function to download files 
	p = multiprocessing.Pool(n_cores)
	downloaded_files_all = p.map(download_file, flat_results)
	return downloaded_files_all



########################################
# Run Functions 
########################################

if __name__ == "__main__":
	#prevent Windows system to freeze when running multiprocessing
	freeze_support()
	now = datetime.datetime.now()
	current_time = now.strftime("%Y-%m-%d-%H-%M-%S")
	logging.basicConfig(level=logging.INFO, format="%(message)s")  #log file configuration
	logger = logging.getLogger()
	logger.addHandler(logging.FileHandler(f"log_{current_time}.log", "a"))
	print = logger.info
	n_cores = 1  # Set the number of processors to use the run the script; set default as 1
	types = ["od","rac","wac"]   #read in LODES data categories 
	states = ["al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc",   	#read instate acronyms
	           "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", 
	           "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", 
	           "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", 
	           "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", 
	           "vt", "va", "wa", "wv", "wi", "wy", "pr"]
	new_dir_name = "LODES_Download_" + current_time    #create a new directory to store downloaded files 
	try:
		create_directories()
		all_downloaded_files = process_files()
		print(f"\n\nThe downloaded CSV files are now available in the directory {new_dir_name}.")
	except Exception as e:
		logging.fatal(e, exc_info=True)  # log exception info at FATAL log level