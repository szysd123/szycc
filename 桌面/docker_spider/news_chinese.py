import random
import time
import pymysql
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
from selenium.webdriver.chrome.options import Options
from datetime import datetime

import schedule

# Import configuration file
from config import DB_CONFIG, CHROME_OPTIONS, MAX_PAGES


class SinaFinanceScraper:
    def __init__(self, db_config=DB_CONFIG, driver_options=None):
        # MySQL connection configuration
        self.db_config = db_config
        self.conn = pymysql.connect(**self.db_config)
        self.cursor = self.conn.cursor()

        # Browser settings
        chrome_options = Options()
        for option in CHROME_OPTIONS:  # Use the configuration imported from config
            chrome_options.add_argument(option)
        if driver_options:
            chrome_options.add_argument(driver_options)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)  # Set implicit wait time

        # Track the times already scraped to avoid duplication
        self.seen_times = set()

    def scrape_data(self, url, type):
        # Record start time
        start_time = datetime.now()
        scraped_count = 0  # Count the number of items scraped in this run
        stop_scraping = False  # Add a flag to stop scraping

        self.driver.get(url)
        current_page = 0  # Page counter for the current scrape
        max_pages = MAX_PAGES  # Get the maximum page count from the config

        while not stop_scraping and current_page < max_pages:  # Exit when stop_scraping is True or max pages is reached
            # Simulate scrolling
            self.scroll_page()

            # Add explicit wait to ensure the page loads completely
            time.sleep(2)

            # Get the elements for content and time
            try:
                elems = self.driver.find_elements(By.XPATH, '//p[@class="bd_i_txt_c"]')
                elems1 = self.driver.find_elements(By.XPATH, '//p[@class="bd_i_time_c"]')
            except Exception as e:
                print(f"Error scraping data: {e}")
                break

            # If no more data, stop scraping
            if not elems or not elems1:
                print("No more data, stopping scrape...")
                break

            # Ensure the number of content and time elements match to avoid index errors
            for elem, elem1 in zip(elems, elems1):
                content = elem.text
                time_text = elem1.text

                # Check if the current time and data already exist in the database, if yes, stop scraping
                if self.is_data_exists(time_text, content):
                    print("Latest content already fetched")
                    stop_scraping = True  # Set stop_scraping to True to exit the outer loop
                    break  # Exit the current for loop

                # Skip current iteration if time already exists (to avoid duplicate scraping)
                if time_text in self.seen_times:
                    continue

                # Add the time to the seen_times set
                self.seen_times.add(time_text)

                # Check if the content contains 【】, if not, set title to None
                titles = re.findall(r'【(.*?)】', content)  # Match content inside 【】
                if not titles:
                    title = None
                    non_titles = content  # If no 【】, the non-title part is the entire content
                else:
                    title = titles[0]  # Get the first content inside 【】
                    non_titles = re.sub(r'【.*?】', '', content)  # Remove content inside 【】, the rest is the body

                # Insert data into MySQL database, adding the type field
                self.insert_data(time_text, title, non_titles, type)
                scraped_count += 1  # Increment the count of scraped items

            current_page += 1  # Increment the page counter

        # Record the end time and calculate the duration
        end_time = datetime.now()
        duration = end_time - start_time

        # Insert logs into the database
        self.insert_log(start_time, end_time, duration, url, scraped_count)

    def scroll_page(self):
        """Simulate scrolling to ensure more content loads"""
        for _ in range(33):  # Control the number of scrolls to avoid scrolling too much at once
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2, 4))  # Wait for the page to load

    def is_data_exists(self, time_text, content):
        """Check if time, title, and content already exist in the database"""
        # Build the query
        query = "SELECT COUNT(*) FROM finance_data WHERE time = %s AND content = %s"
        try:
            self.cursor.execute(query, (time_text, content))
            result = self.cursor.fetchone()
            if result[0] > 0:
                return True  # Data exists
            else:
                return False  # Data doesn't exist
        except Exception as e:
            print(f"Error checking data existence: {e}")
            return False  # If an error occurs, assume the data doesn't exist

    def insert_data(self, time_text, title, non_titles, type):
        """Insert data into the database, adding the type field"""
        insert_query = "INSERT INTO finance_data (time, title, content, type) VALUES (%s, %s, %s, %s)"
        try:
            self.cursor.execute(insert_query, (time_text, title, non_titles, type))
            self.conn.commit()  # Commit the transaction
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()  # Roll back the transaction in case of an error

    def insert_log(self, start_time, end_time, duration, url, scraped_count):
        """Record scraping logs"""
        log_query = """
        INSERT INTO scrape_log (start_time, end_time, duration, url, scraped_count)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            self.cursor.execute(log_query, (start_time, end_time, duration, url, scraped_count))
            self.conn.commit()  # Commit the log transaction
        except Exception as e:
            print(f"Error inserting log: {e}")
            self.conn.rollback()  # Roll back the transaction in case of an error

    def close(self):
        # Close the browser
        self.driver.quit()

        # Close the database connection
        self.cursor.close()
        self.conn.close()


def job():
    # Start the scraper
    scraper = SinaFinanceScraper()
    try:
        scraper.scrape_data("https://finance.sina.com.cn/7x24/?tag=102", type="International")
    finally:
        scraper.close()


if __name__ == "__main__":
    # Run every 10 minutes
    schedule.every(10).minutes.do(job)

    while True:
        # Wait until the next scheduled job
        schedule.run_pending()
        time.sleep(1)
