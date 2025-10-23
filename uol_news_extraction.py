"""
João Loss - joao.loss@edu.ufes.br

This script scrapes UOL news text from links collected by uol_links_extraction.py. The links are stored in the specified
year folder within the UOL_LINKS_PATH directory. Results are saved in the OUTPUT_FOLDER_PATH, and logs are stored in LOG_PATH.

Note: to avoid extremely long processing times, only one folder or file is processed per execution.
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
from pathlib import Path
import sys
import argparse
from queue import Queue # handles locking internally for multithreading tasks
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time
import re

# --- Global definitions

UOL_LINKS_PATH = os.path.join("out", "uol_links")

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress console output."
    )

    def check_path(path: str) -> tuple[str, str]:
        """
        Validate that the argument is either a folder (year) or a file inside UOL_LINKS_PATH.
        Returns a tuple: (type: "folder" or "file", full_path)
        """
        full_path_folder = os.path.join(UOL_LINKS_PATH, path)
        if os.path.isdir(full_path_folder):
            if len(os.listdir(full_path_folder)) == 0:
                raise argparse.ArgumentTypeError(f"The folder '{full_path_folder}' is empty.")
            return ("folder", full_path_folder)
        
        # Check if it's a file inside any year folder
        for year in os.listdir(UOL_LINKS_PATH):
            year_path = os.path.join(UOL_LINKS_PATH, year)
            file_path = os.path.join(year_path, path)
            if os.path.isfile(file_path):
                return ("file", file_path)

        raise argparse.ArgumentTypeError(f"'{path}' is neither a valid year folder nor a file inside a year folder in {UOL_LINKS_PATH}.")

    parser.add_argument(
        "--path",
        required=True,
        type=check_path,
        help="Either the year folder to process all files, or the name of a specific file inside a year folder."
    )

    return parser.parse_args()

args = parse_args()
input_type, input_path = args.path
quiet_mode = args.quiet

target = Path(input_path).name # take the last part of input_path (file name or folder name)

# Create log file path
log_sufix = target
if input_type == "file":
    log_sufix = target.split(".")[0] # remove .txt from file name
os.makedirs("logs", exist_ok=True)
LOG_PATH = os.path.join("logs", f"{os.path.basename(__file__).split(".")[0]}_{log_sufix}.log")

# Create output folder path
ouput_folder_name = target
if input_type == "file":
    ouput_folder_name = Path(input_path).parent.name # take the parent folder of the file
OUTPUT_FOLDER_PATH = os.path.join("out", "uol_news", ouput_folder_name)
os.makedirs(OUTPUT_FOLDER_PATH, exist_ok=True)

REQUEST_TIMEOUT = 15
RETRY_TIME = 2
MAX_WORKERS = 5
GLOBAL_LOCK = Lock()
error_count = 0

# ---

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
    Implement simple text cleaning and remove some extremely common sequences that was observed after some scrapes.
    """
    return re.sub(r'\s+', ' ', text).strip().lower().replace("flavio ricco patrícia abravanel ainda não tem previsão para gravações no sbt", "").replace("favorite! agora você pode escolher seus blogs favoritos. clique na estrela e depois arraste para ordenar.", "")

def worker(link:str, output_file_path:str):
    """
    Scrape news text from 'link' and append to 'output_file_path'. Returns True on success, False on failure.
    """
    
    # Technical note: global variables can be READ without 'global' keyword, but MODIFICATION requires 'global' declaration,
    # when Python sees an assignment (=) to a variable inside a function, it automatically treats that variable as local.
    global error_count

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

    if cleaned_text: # could collect and is not empty
        with GLOBAL_LOCK:
            with open(file=output_file_path, mode="a") as f:
                f.write(cleaned_text + "\n")
    else:
        with GLOBAL_LOCK:
            error_count += 1
    
    time.sleep(2)

def main():
    logs_queue = Queue()
    logs_listener = logs_listener_config(quiet_mode=quiet_mode, queue=logs_queue)
    logs_listener.start()

    root_logger_config(logs_queue)

    total_links = 0
    start_time = time.time()

    if input_type == "file":
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="Worker") as executor:
            with open(file=input_path, mode="r") as f:
                links = [line.strip() for line in f.readlines()]
        
            num_links = len(links)
            total_links += num_links
            logging.info(f"{num_links} links from '{input_path}'.")

            for link in links:
                executor.submit(worker, link, os.path.join(OUTPUT_FOLDER_PATH, target))
    
    elif input_type == "folder":
        files = os.listdir(input_path)
        already_processed = os.listdir(OUTPUT_FOLDER_PATH)
        files = [f for f in files if f not in already_processed]
        logging.info(f"{len(files)} file(s) to process.")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="Worker") as executor:
            for file in files:
                file_path = os.path.join(input_path, file)
                with open(file=file_path, mode="r") as f:
                    links = [line.strip() for line in f.readlines()]
            
                num_links = len(links)
                total_links += num_links
                logging.info(f"{num_links} links from '{file_path}'.")

                for link in links:
                    executor.submit(worker, link, os.path.join(OUTPUT_FOLDER_PATH, file))

    if total_links > 0:
        success_rate = (total_links - error_count) / total_links * 100
    else:
        success_rate = 0.0

    logging.info(f"Processed {total_links} links - {total_links - error_count}/{total_links} succeeded ({success_rate:.1f}%).")
    end_time = time.time()
    logging.info(f"Total time taken to complete: {int((end_time - start_time)/60)}min")
    logs_listener.stop()
    
if __name__ == "__main__":
    main()