"""
João Loss - joao.loss@edu.ufes.br

This script scrapes UOL news text from links collected by uol_links_extraction.py. The links are stored in the specified
year folder within the UOL_LINKS_PATH directory. Results are saved in the OUTPUT_FOLDER_PATH, and logs are stored in LOG_PATH.

Note: to avoid long processing times, only one folder is processed per execution.
Note: multithreading is used to improve performance.
"""

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import logging
import logging.handlers
import requests
from requests.exceptions import ReadTimeout, ConnectionError, RequestException
import os
import argparse
from argparse import ArgumentError
from queue import Queue # handles locking internally for multithreading tasks
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time
import re

UOL_LINKS_PATH = os.path.join("out", "uol_links")

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress console output."
    )

    def check_year(year:str):
        if year not in os.listdir(UOL_LINKS_PATH): # Check if the year is a folder inside UOL_LINKS_PATH
            raise ArgumentError(message=f"{year} is not a folder in {UOL_LINKS_PATH}.")
        path = os.path.join(UOL_LINKS_PATH, year)
        if len(os.listdir(path)) == 0: # Check if the year folder is empty
            raise ArgumentError(message=f"{path} is empty.")
        return year
    
    parser.add_argument(
        "--year-folder",
        help=f"Year from which news will be scraped. It should be the name of the corresponding folder in {UOL_LINKS_PATH}.",
        type=check_year,
        required=True
    )

    return parser.parse_args()

args = parse_args()

os.makedirs("logs", exist_ok=True)
LOG_PATH = os.path.join("logs", f"{os.path.basename(__file__).split(".")[0]}_{args.year_folder}.log")

OUTPUT_FOLDER_PATH = os.path.join("out", "uol_news", args.year_folder)
os.makedirs(OUTPUT_FOLDER_PATH, exist_ok=True)

REQUEST_TIMEOUT = 15
RETRY_TIME = 2
MAX_WORKERS = 5
GLOBAL_LOCK = Lock()
ERROR_COUNT = 0

def logs_listener_config(quiet_mode:bool, queue:Queue) -> logging.handlers.QueueListener:
    file_handler = logging.FileHandler(filename=LOG_PATH, mode="w")
    file_handler.setLevel(logging.INFO)
    
    if quiet_mode:
        return logging.handlers.QueueListener(queue, file_handler, respect_handler_level=True)
    
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)

    return logging.handlers.QueueListener(queue, file_handler, stdout_handler, respect_handler_level=True)

def root_logger_config(queue:Queue) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # don't filter here — the listener is responsible for deciding which logs to output

    formatter = logging.Formatter("[%(levelname)s - %(asctime)s - %(threadName)s] %(message)s")
    
    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.DEBUG) # don't filter here — the listener is responsible for deciding which logs to output
    queue_handler.setFormatter(formatter)

    root_logger.addHandler(queue_handler)

def get_response(link:str) -> requests.Response | None:
    """
    Return the response or None in failure.
    """
    for n_try in range(3):
        try:
            response = requests.get(link, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                logging.error(f"Status code == {response.status_code} for {link}, skipping...")
                response = None
            break
        except ConnectionError:
            logging.debug(f"ConnectionError for {link} (attempt {n_try+1})")
            time.sleep(RETRY_TIME)
        except ReadTimeout:
            logging.debug(f"ReadTimeout for {link} (attempt {n_try+1})")
            time.sleep(RETRY_TIME)
        except RequestException as e:
            logging.debug(f"RequestException for {link} (attempt {n_try+1}): {e}\n")
            time.sleep(RETRY_TIME)
    else:
        logging.error(f"Failed to connect with {link} after {n_try+1} attempts, skipping...")
        response = None

    time.sleep(2)
    return response

def worker_selenium(link:str) -> str:
    """
    Built as a fallback for the requests + beautifulsoup workflow. 
    For more details, see the comment in the worker() function.
    """
    
    options = Options() 
    options.add_argument("--no-sandbox") # turn off security mode to avoid some issues
    options.add_argument("--log-level=3") # set to log only error messages
    options.add_argument("--start-maximized")
    # options.add_argument("--headless=new")
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(link)
    text = None
    try:
        element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#texto, .text"))) # id="texto" or class="text"
        text = clean_text(element.text)
    except TimeoutException:
        logging.error(f"Timeout by selenium for {link}")
    driver.quit()
    return text

def clean_text(text:str) -> str:
    """
    Implement simple text cleaning.
    """
    return re.sub(r'\s+', ' ', text).strip().lower()

def worker(link:str, output_file_path:str):
    """
    Scrape news text from 'link' and append to 'output_file_path'. Returns True on success, False on failure.
    """
    
    # Technical note: global variables can be READ without 'global' keyword, but MODIFICATION requires 'global' declaration,
    # when Python sees an assignment (=) to a variable inside a function, it automatically treats that variable as local.
    global ERROR_COUNT

    cleaned_text = None
    response = get_response(link)
    if response:
        soup = BeautifulSoup(response.text, 'html.parser')
        divs = soup.find_all(name="div", class_="text")
        if len(divs) == 0:
            divs = soup.find_all(name="div", id="texto")
        
        cleaned_text = clean_text(divs[0].text)
    else:
        # After several tests, I noticed that running Selenium without the --headless option improved the success rate:
        # some links that failed to load with requests + beautifulsoup were successfully accessed via Selenium (non-headless mode).
        # However, running Selenium in headless mode was quite inconsistent and didn’t provide significant improvements.
        # If having hundreds of Chrome windows open is not an issue, uncomment the lines below to use Selenium as a fallback
        # when requests + beautifulsoup fail.
        # -----
        # logging.info("Trying selenium...")
        # cleaned_text = worker_selenium(link)
        pass

    if cleaned_text:
        with GLOBAL_LOCK:
            with open(file=output_file_path, mode="a") as f:
                f.write(cleaned_text + "\n")
    else:
        with GLOBAL_LOCK:
            ERROR_COUNT += 1
    
    time.sleep(2)

def main():
    logs_queue = Queue()
    logs_listener = logs_listener_config(quiet_mode=args.quiet, queue=logs_queue)
    logs_listener.start()

    root_logger_config(logs_queue)

    year_folder_path = os.path.join(UOL_LINKS_PATH, args.year_folder)
    files = os.listdir(year_folder_path)

    logging.info(f"{len(files)} file(s) to process.")
    total_links = 0
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="Worker") as executor:
        for file in files:
            file_path = os.path.join(year_folder_path, file)
            with open(file=file_path, mode="r") as f:
                links = [line.strip() for line in f.readlines()]
        
            num_links = len(links)
            total_links += num_links
            logging.info(f"{num_links} links from '{file_path}'.")

            for link in links:
                executor.submit(worker, link, os.path.join(OUTPUT_FOLDER_PATH, file))

    if total_links > 0:
        success_rate = (total_links - ERROR_COUNT) / total_links * 100
    else:
        success_rate = 0.0

    logging.info(f"Processed {total_links} links - {total_links - ERROR_COUNT}/{total_links} succeeded ({success_rate:.1f}%).")
    end_time = time.time()
    logging.info(f"Total time taken to complete: {int((end_time - start_time)/60)}min")
    logs_listener.stop()
    
if __name__ == "__main__":
    main()