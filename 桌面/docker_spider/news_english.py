import random
import time
import pymysql
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
from datetime import datetime
import schedule


class SinaFinanceScraper:
    def __init__(self, config):
        """
        Initialize the scraper class, including database connection and browser configuration.
        """
        self.db_config = config['db_config']
        self.conn = pymysql.connect(**self.db_config)
        self.cursor = self.conn.cursor()

        # Configure Chrome driver
        chrome_options = webdriver.ChromeOptions()
        for option in config['chrome_options']:
            chrome_options.add_argument(option)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)  # Set implicit wait time

        self.seen_times = set()  # Used to keep track of times already scraped, to avoid duplicates

    def scrape_data(self, url, type_):
        """
        Main scraping logic.
        :param url: The target URL to scrape.
        :param type_: The type of data being scraped.
        """
        start_time = datetime.now()
        scraped_count = 0  # Count the number of items scraped in this session
        stop_scraping = False

        try:
            self.driver.get(url)
            time.sleep(2)  # Wait for the page to load initially

            # Close any popups
            self.close_popup()

        except Exception as e:
            print(f"Page load failed: {e}")
            return

        current_page = 0
        max_pages = config['scraper']['max_pages']  # Maximum number of pages to scrape

        while not stop_scraping and current_page < max_pages:
            self.scroll_page()
            time.sleep(2)  # Wait for the content to load after scrolling

            try:
                # Get the time and content elements on the page
                time_elements = self.driver.find_elements(By.XPATH, '//div[@class="date"]')
                content_elements = self.driver.find_elements(By.XPATH, '//div[@class="flex_right"]')

                if not time_elements or not content_elements:
                    print("No more data on the page, stopping scrape...")
                    break

                for time_elem, content_elem in zip(time_elements, content_elements):
                    time_text = time_elem.text
                    content = content_elem.text

                    # Check if the data has already been scraped
                    if self.is_data_exists(time_text, content):
                        print("Scraped data already exists, stopping scrape...")
                        stop_scraping = True
                        break

                    # Add time to the seen set to avoid duplicate scraping
                    if time_text in self.seen_times:
                        continue

                    self.seen_times.add(time_text)

                    # Extract the title and content
                    titles = re.findall(r'【(.*?)】', content)
                    title = titles[0] if titles else None
                    non_titles = re.sub(r'【.*?】', '', content)

                    # Store the data in the database
                    self.insert_data(time_text, title, non_titles, type_)
                    scraped_count += 1

                current_page += 1

            except Exception as e:
                print(f"Page parsing failed: {e}")
                break

        # End scraping, record the log
        end_time = datetime.now()
        duration = end_time - start_time
        self.insert_log(start_time, end_time, duration, url, scraped_count)

    def scroll_page(self):
        """
        Simulate scrolling on the page to load more content.
        """
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        for _ in range(33):
            try:
                self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                time.sleep(random.uniform(1, 3))

                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:  # Reached the bottom of the page
                    print("Reached the bottom of the page")
                    break
                last_height = new_height
            except Exception as e:
                print(f"Page scroll failed: {e}")
                break

    def close_popup(self):
        """
        Close any popups, assuming the popup button can be found by its xpath.
        """
        try:
            close_button = self.driver.find_element(By.XPATH, '/html/body/div[9]/div[2]/div[1]/i')
            close_button.click()  # Click the close button
            print("Popup closed")
        except Exception as e:
            print(f"Popup close failed: {e}")

    def is_data_exists(self, time_text, content):
        """
        Check if the data already exists in the database.
        """
        try:
            time_text = datetime.strptime(time_text, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass  # If the time format does not match, skip this entry

        content = re.sub(r'\s+', ' ', content).strip()

        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        query = "SELECT COUNT(*) FROM news_data WHERE time = %s AND content = %s"
        try:
            self.cursor.execute(query, (time_text, content))
            result = self.cursor.fetchone()
            return result[0] > 0
        except Exception as e:
            print(f"Error checking if data exists: {e}")
            return False

    def insert_data(self, time_text, title, content, type_):
        """
        Insert the scraped data into the database.
        """
        # noinspection SqlNoDataSourceInspection
        query = "INSERT INTO news_data (time, title, content, type) VALUES (%s, %s, %s, %s)"
        try:
            self.cursor.execute(query, (time_text, title, content, type_))
            self.conn.commit()
        except Exception as e:
            print(f"Data insertion failed: {e}")
            self.conn.rollback()

    def insert_log(self, start_time, end_time, duration, url, scraped_count):
        """
        Record the scraping log in the database.
        """
        # noinspection SqlNoDataSourceInspection
        query = """
        INSERT INTO scrape_log (start_time, end_time, duration, url, scraped_count)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            self.cursor.execute(query, (start_time, end_time, duration, url, scraped_count))
            self.conn.commit()
        except Exception as e:
            print(f"Log recording failed: {e}")
            self.conn.rollback()

    def close(self):
        """
        Release resources: close the browser and database connection.
        """
        try:
            if self.driver:
                self.driver.quit()
            if self.conn:
                self.cursor.close()
                self.conn.close()
        except Exception as e:
            print(f"Error releasing resources: {e}")


def job(config):
    # Start the scraper
    scraper = SinaFinanceScraper(config)
    try:
        scraper.scrape_data(config['scraper']['url'], type_=config['scraper']['type'])
    finally:
        scraper.close()


if __name__ == "__main__":
    # Load the configuration file
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Set up the scheduled task
    schedule.every(config['schedule']['interval_minutes']).minutes.do(job, config=config)

    while True:
        # Wait for the next task after each run
        schedule.run_pending()
        time.sleep(1)
