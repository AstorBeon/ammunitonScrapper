import requests
from bs4 import BeautifulSoup

from Models.Offer import Offer


class Scrapper():
    AVAILABLE_AMMO_SIZES = ["7,65",".223Rem",".223","308 Win","9mm", "9x19", "308", ".22LR"]

    def __init__(self):
        self.mission="I'm a happy scrapper, doing god's work and helping find cheap ammo"
        self.offers=[]






    def scrap_top_gun(self) -> [Offer]:
        base_url = 'https://sklep.top-gun.pl/5-amunicja'

        # Headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        }

        global AVAILABLE_AMMO_SIZES
        def extract_data_from_title(title):
            size = "?"

            #get size
            for av_size in AVAILABLE_AMMO_SIZES:
                if av_size in title:
                    size = av_size
                    title = title.replace(av_size, "")
            return title, size
        # Function to get total number of pages
        def get_total_pages():
            url = f'{base_url}#/cena-0-7'
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to load first page: {response.status_code}")
                return 1

            soup = BeautifulSoup(response.text, 'html.parser')
            pagination = soup.find('ul', class_='pagination')
            if not pagination:
                return 1

            page_links = pagination.find_all('a')
            page_numbers = [int(link.get_text()) for link in page_links if link.get_text().isdigit()]
            return max(page_numbers) if page_numbers else 1

        # Function to scrape product data
        def scrape_all_products():
            products_data = []
            total_pages = get_total_pages()
            print(f"Total pages found: {total_pages}")

            for page in range(1, total_pages + 1):
                url = f'{base_url}?p={page}#/cena-0-7'
                print(f'\nScraping page {page}: {url}')
                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                product_containers = soup.find_all('div', class_='product-container')

                for product in product_containers:
                    title_tag = product.find('a', class_='product-name')
                    price_tag = product.find('span', class_='price')
                    link = product.find('a')['href']

                    title = title_tag.get_text(strip=True) if title_tag else "No title"
                    price = price_tag.get_text(strip=True) if price_tag else "No price"
                    title,size = extract_data_from_title(title)

                    products_data.append({
                        'store':"Top gun",
                        'title': title,
                        'link': link,
                        'size': size,
                        'price': price
                    })

            return products_data

        # Run the scraper
        self.offers.extend(scrape_all_products())




s = Scrapper()
s.scrap_top_gun()