"""
João Loss - joao.loss@edu.ufes.br

This file contains a script that traverses the Wayback Machine archive page (https://help.archive.org/help/using-the-wayback-machine/)
to scrape links from UOL within a specified date range. The results are stored in OUTPUT_CSV_PATH. The logs are saved in LOG_PATH.

Note: Selenium was used instead of BeautifulSoup because some essential elements for scraping are loaded via JS after the initial received page.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException
from argparse import ArgumentParser, ArgumentTypeError
from datetime import datetime
import time
import pandas as pd
import os
from json import dumps
import logging
import sys

os.makedirs(name="out", exist_ok=True)
OUTPUT_CSV_PATH = os.path.join("out", "archive_links.csv")

os.makedirs("logs", exist_ok=True)
LOG_PATH = os.path.join("logs", "archive_links_extraction.log")

SITE = "www.uol.com.br"
WEB_ARCHIVE_LINK = "https://web.archive.org/web/{year}0101*/" + SITE
STR_TO_INT = {"JAN": 1, "FEB": 2, "MAR": 3,
              "APR": 4, "MAY": 5, "JUN": 6, 
              "JUL": 7, "AUG": 8, "SEP": 9, 
              "OCT": 10, "NOV": 11, "DEC": 12}

def get_args() -> ArgumentParser:
    """
    Returns a command-line argument parser.
    """

    parser = ArgumentParser()
    
    parser.add_argument(
        "--headless",
        help="If enabled, the program will run without a graphical user interface (headless mode).",
        action="store_true",
        default=False
    )

    def parse_month_year(date_str: str) -> datetime:
        """
        Parses a date string in 'mm/yyyy' format into a datetime object.
        """
        try:
            return datetime.strptime(date_str, "%m/%Y")
        except ValueError:
            raise ArgumentTypeError(f"Invalid date format: '{date_str}'. Expected format is 'mm/yyyy'.")

    parser.add_argument(
        "--start-date",
        help="Start date in 'mm/yyyy' format.",
        type=parse_month_year,
        required=True
    )

    parser.add_argument(
        "--end-date",
        help="End date in 'mm/yyyy' format.",
        type=parse_month_year,
        required=True
    )

    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress console output."
    )

    return parser.parse_args()

def get_archive_links(start_date: datetime, end_date: datetime, options: Options) -> list[dict]:
    """
    Fetch archive page links for each month between start_date and end_date (inclusive).

    Returns a list of dicts, each containing:
        - "year": int
        - "month": int
        - "links": list of URLs (strings) for that month's days
    """

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    current_year = start_date.year
    links = list()

    date_checker = lambda d: (d >= start_date) and (d <= end_date)
    url_checker = lambda u: ("https://" in u) and ("www.uol.com.br" in u)

    while current_year <= end_date.year:
        logging.info(f"==> Curr. year: {current_year}")

        url = WEB_ARCHIVE_LINK.format(year=current_year)
        driver.get(url)

        all_months = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "month")))

        for i, month in enumerate(all_months):
            
            try:
                # Scroll so this month element becomes visible
                x = month.location["x"]
                y = month.location["y"]
                driver.execute_script(f"window.scrollTo({x}, {y});")
                time.sleep(1)
            except StaleElementReferenceException:
                logging.error(f"Failed while processing month {i+1}.")
                continue

            # Attempt retrieving links for that month
            for _ in range(5):
                try:
                    # Get the month number (from its title text)
                    month_num = STR_TO_INT[month.find_element(By.CLASS_NAME, "month-title").text]
                    date = datetime(year=current_year, month=month_num, day=1)

                    # Only proceed if this month is within the desired date range
                    if date_checker(date):
                        calendar_day_list = month.find_elements(By.CLASS_NAME, "calendar-day")
                        href_list = list()
                        for calendar_day in calendar_day_list:
                            # Extract href attribute from each day link
                            href = calendar_day.find_element(By.TAG_NAME, "a").get_attribute("href").strip()
                            if url_checker(href):
                                href_list.append(href)
                        
                        links.append({
                            "year": current_year,
                            "month": month_num,
                            "links": dumps(href_list) # from list to str
                        })
                        logging.info(f"{len(href_list)} links found for {month_num}/{current_year}.")

                    break  # break out of retry loop if success
                except Exception as e:
                    logging.error(f"Failed to get links for month element {month}.")
                    time.sleep(1)

        current_year += 1  # move to next year
        logging.info("")

    driver.quit()
    return links

def config_root_logger(quite_mode:bool):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # let handlers check log levels

    pattern = "[%(levelname)s - %(asctime)s] %(message)s"
    formatter = logging.Formatter(pattern)

    file_handler = logging.FileHandler(filename=LOG_PATH, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    
    if not quite_mode:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(formatter)
        root.addHandler(stdout_handler)

def main():
    args = get_args()
    config_root_logger(args.quiet)

    options = Options()
    options.add_argument("--no-sandbox") # turn off security mode to avoid some issues
    options.add_argument("--log-level=3") # set to log only error messages
    options.add_argument("--start-maximized")
    if args.headless:
        options.add_argument("--headless") # no GUI
    
    options.page_load_strategy = "eager" # wait until the initial HTML document is loaded and parsed

    start_time = time.time()
    links = get_archive_links(start_date=args.start_date,
                              end_date=args.end_date,
                              options=options)
    end_time = time.time()
    logging.info(f"Total time taken to complete: {(end_time - start_time)/60:.02f}min")  
    
    df = pd.DataFrame(data=links)
    df.to_csv(path_or_buf=OUTPUT_CSV_PATH, index=False)

if __name__ == "__main__":
    main()