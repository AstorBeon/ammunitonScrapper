import os
import re

import requests
from bs4 import BeautifulSoup

from Models.Offer import Offer



AVAILABLE_AMMO_SIZES = ["7,65",".223Rem",".223","308 Win","9mm", "9x19", "308", ".22LR"]
AVAILABLE_DYNAMIC_AMMO_SIZES = ["\d{1,2},\d{1,2}x\d{1,2}"] #todo add more
AVILABLE_AMMO_SIZE_MAPPINGS = {}

def extract_data_from_title(title):
    size = "?"
    global AVAILABLE_AMMO_SIZES,AVAILABLE_DYNAMIC_AMMO_SIZES
    # get size
    is_found=False
    for av_size in AVAILABLE_AMMO_SIZES:
        if av_size in title:
            size = av_size
            title = title.replace(av_size, "")
            is_found = True
            break
    if not is_found:
        for reg_size in AVAILABLE_DYNAMIC_AMMO_SIZES:
            res = re.findall(reg_size,title)
            if res:
                size = res[0]
                title = title.replace(res[0],"")
                break
    return title, size



def scrap_top_gun() -> [Offer]:
    base_url = 'https://sklep.top-gun.pl/5-amunicja'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }


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
    return scrape_all_products()
    # Run the scraper

def scrap_strefa_celu() -> [Offer]:

    # Base URL for the ammunition section
    base_url = 'https://strefacelu.pl/category/bron-palna-i-amunicja-amunicja-sportowa'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    # Function to get total number of pages
    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
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
            url = f'{base_url}?p={page}'
            print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='product')

            for product in product_containers:
                title_tag = product.find('a', class_='product_name').get_text()
                price_tag = product.find('div', class_='main_price').get_text(strip=True)
                link_tag = f"https://strefacelu.pl{product.find('a', class_='product_name')['href']}"

                title = title_tag if title_tag else "No title"
                price = price_tag if price_tag else "No price"
                link = link_tag if link_tag else "No link"

                title,size = extract_data_from_title(title)

                products_data.append({
                    'title': title,
                    "size":size,
                    'store':"Strefa Celu",
                    'price': price,
                    'link': link
                })

        return products_data

    # Run the scraper
    product_list = scrape_all_products()
    return product_list

def scrap_garand() -> [Offer]:
    from urllib.parse import urljoin
    base_url = 'https://garand.com.pl/category/amunicja'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    # Function to get total number of pages
    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
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
            url = f'{base_url}?p={page}'
            print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', {"data-equalizer-watch":"thumb"})

            for product in product_containers:
                #sub = product.find("div", )
                #print(product)
                #os._exit(0)
                title_tag = product.find('p', class_='name')

                price_tag = product.find('div', class_='box-price')
                link_tag = product.find('a', class_='product_name')

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"

                title,size = extract_data_from_title(title)

                products_data.append({
                    'title': title,
                    'size':size,
                    'store':"Garand",
                    'price': price,
                    'link': link
                })

        return products_data

    # Run the scraper
    product_list = scrape_all_products()
    return product_list




STORES_SCRAPPERS = {
    "Garand":scrap_garand,
    "Top gun":scrap_top_gun,
    "Strefa celu":scrap_strefa_celu
}


# s = Scrapper()
# #s.scrap_top_gun()
# #s.scrap_strefa_celu()
# s.scrap_garand()