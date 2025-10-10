from multiprocessing import Process, Queue
import pandas as pd
import json
import time
import os
import sys
import logging
import logging.handlers
from bs4 import BeautifulSoup
import requests
from requests.exceptions import ConnectionError, ReadTimeout, RequestException

ARCHIVE_CSV_PATH = os.path.join("out", "archive_links.csv")

os.makedirs("logs", exist_ok=True)
LOG_PATH = os.path.join("logs", "uol_links_extraction.log")

OUTPUT_FILES_PATH = os.path.join("out", "uol_links")
os.makedirs(OUTPUT_FILES_PATH, exist_ok=True)

MAX_NUM_WORKERS = 5
REQUEST_TIMEOUT = 25
RETRY_TIME = 5

LOG_PATTERN = "[%(levelname)s - %(name)s] %(message)s"

def queue_listener_handlers_config() -> tuple[logging.Handler, logging.Handler]:
    file_handler = logging.FileHandler(filename=LOG_PATH, mode="w")
    stdout_handler = logging.StreamHandler(sys.stdout)

    return file_handler, stdout_handler

def worker_logger_config(queue:Queue, year:int, month:int) -> logging.Logger:
    worker_logger = logging.getLogger(f"Worker_{os.getpid()}_{month}_{year}")
    worker_logger.setLevel(logging.INFO)

    formatter = logging.Formatter(LOG_PATTERN)

    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)
    queue_handler.setFormatter(formatter)

    worker_logger.addHandler(queue_handler)

    return worker_logger

def worker(archive_links:list[str], year:int, month:int, queue:Queue):
    worker_logger = worker_logger_config(queue, year, month)

    def get_real_url_date(url):
        """
        Return the actual year and month from the URL, as it may differ from those in the original archive URL.
        """
        date = url.split("/web/")[1][:8]
        y = int(date[:4])
        m = int(date[4:6])
        return y, m
    
    href_filter = lambda u: "http" + u.split("http")[-1] # clarification comment at the end
    
    # Create the corresponding folder
    year_folder_path = os.path.join(OUTPUT_FILES_PATH, str(year))
    os.makedirs(year_folder_path, exist_ok=True)

    loss_count = 0
    for i, link in enumerate(archive_links):
        for n_try in range(3):
            try:
                response = requests.get(link, timeout=REQUEST_TIMEOUT)
                break
            except ConnectionError:
                worker_logger.warning(f"ConnectionError for {link} (attempt {n_try+1})")
                time.sleep(RETRY_TIME)
            except ReadTimeout:
                worker_logger.warning(f"ReadTimeout for {link} (attempt {n_try+1})")
                time.sleep(RETRY_TIME)
            except RequestException as e:
                worker_logger.warning(f"RequestException for {link} (attempt {n_try+1}): {e}\n")
                time.sleep(RETRY_TIME)
        else:
            worker_logger.error(f"Failed to connect with {link} after {n_try+1} attempts, skipping...")
            loss_count += 1
            continue

        if response.status_code != 200:
            worker_logger.error(f"Response status code != 200 for {link} (got {response.status_code}), skipping...")
            loss_count += 1
            continue

        html = response.text
        actual_year, actual_month = get_real_url_date(response.url)
        soup = BeautifulSoup(html, 'html.parser')
        anchor_elements = soup.find_all("a")

        uol_links = set()
        for e in anchor_elements:
            href = e.get("href")
            if href is not None:
                href = str(href)

                # Verify potencial URL news pattern
                if f"/{actual_year}/" in href:
                    uol_links.add(href_filter(href))
        
        logging.debug(f"{len(uol_links)} links found in ({i+1}) {link}")

        file_path = os.path.join(year_folder_path, f"{actual_month}-{actual_year}.txt")
        if year != actual_year:
            adjusted_year_folder_path = os.path.join(OUTPUT_FILES_PATH, str(actual_year))
            os.makedirs(adjusted_year_folder_path, exist_ok=True)
            file_path = os.path.join(adjusted_year_folder_path, f"{actual_month}-{actual_year}.txt")
            
        with open(file_path, 'a') as f:
            f.write("\n".join(uol_links) + "\n")
    
    worker_logger.info(f"Loss rate: {loss_count}/{len(archive_links)}")

def parent_logger_config(queue:Queue) -> logging.Logger:
    logger = logging.getLogger(f"Parent_{os.getpid()}")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(LOG_PATTERN)

    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)
    queue_handler.setFormatter(formatter)

    logger.addHandler(queue_handler)

    return logger

def main():
    archive_df = pd.read_csv(ARCHIVE_CSV_PATH)
    archive_df["links"] = [json.loads(s) for s in archive_df["links"]]
    grouped_archive_df = archive_df.groupby("year")

    log_queue = Queue(-1)

    file_handler, stdout_handler = queue_listener_handlers_config()
    queue_listener = logging.handlers.QueueListener(log_queue,
                                                    file_handler, stdout_handler,
                                                    respect_handler_level=False) # Always pass each log message to each handler
    queue_listener.start()
    
    logger = parent_logger_config(log_queue)
    
    start_time = time.time()
    running_workers = list()

    try:
        for _, year_df in grouped_archive_df:
            for row in year_df.itertuples(index=False, name="row"):
                while len(running_workers) >= MAX_NUM_WORKERS:
                    # Wait until any child finishes
                    for rw in list(running_workers):
                        rw.join(timeout=.5)
                        if not rw.is_alive():
                            running_workers.remove(rw)
                            break
                    
                    # All child processes still running
                    else:
                        time.sleep(0.5)
                        continue
                    break
                
                logger.info(f"Starting {row.month}/{row.year}")

                # Start the new process
                new_worker = Process(target=worker, args=(row.links, row.year, row.month, log_queue))
                new_worker.start()
                running_workers.append(new_worker)

        # Wait for all child processes remaining
        for p in running_workers:
            p.join()
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt detected. Terminating all workers...")
        for p in running_workers:
            if p.is_alive():
                logger.warning(f"Terminating process {p.pid}...")
                p.terminate()
        for p in running_workers:
            p.join()
        logger.info("All workers terminated safely.")
    finally:
        end_time = time.time()
        logger.info(f"Total time taken to complete: {(end_time - start_time)/60:.02f}min")
        queue_listener.stop()

if __name__ == "__main__":
    main()