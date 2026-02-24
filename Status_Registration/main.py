from scraper import scrape_registration
import os
from dotenv import load_dotenv
load_dotenv()
username = os.getenv("CUFE_USERNAME")
password = os.getenv("CUFE_PASSWORD")

scrape_registration(username, password)
