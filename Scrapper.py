import os
import re
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from narwhals import DataFrame

from Models.Offer import Offer



AVAILABLE_AMMO_SIZES = ["762x25","243Win","30-30 WIN",".222 REM","223 REM",".338","kal. 38Spec","38Spec",".38 Special",".357 Magnum",".357","kal. 45ACP","45ACP","7,65","7,62",".223Rem",".223","308 Win","9mm", "9x19", "308", ".22LR","22LR", "22 LR",".44 Rem.",".44", "9 PARA","357"]
AVAILABLE_DYNAMIC_AMMO_SIZES = [r"(\d{1,2}(,|\.)\d{1,2}x\d{1,2})",r"(\d{1,3}x\d{2})", r"(kal\. [\\/a-zA-Z0-9]+)"] #todo add more
AVAILABLE_AMMO_SIZE_MAPPINGS = {r"(9mm|9MM|9 mm|9 MM|9x19|9 PARA)":"9mm",
                                r"(\.22LR|22LR|22 LR|\.22 LR|kal. 22LR,|kal.22LR|kal. 22lr)":".22LR",
                                r"(308|308Win|308 Win)":".308 Win",}

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
                print(f"Found for regex: {reg_size}")
                print(f"Matched with: {title}")
                print(f"Result: {res}")
                if type(res[0]) in (list,tuple):
                    size = res[0][0]
                else:
                    size = res[0]

                print(f"Size: {size}")
                title = title.replace(size,"")
                break
    return title, size

def map_single_size(size:str):
    for key,val in AVAILABLE_AMMO_SIZE_MAPPINGS.items():
        if re.findall(key,size):
            return val
    return size

def trim_price(price_text:str):
    return re.sub(r"[^0-9,\.]","",price_text)

def get_all_existing_sizes(df:DataFrame):
    if df.empty:
        return []
    options = list(set(df["size"].to_list()))

    return options

def map_sizes(data:pd.DataFrame):

    data['size'] = data["size"].apply(lambda x:map_single_size(str(x)) )

    return data

def map_prices(data:pd.DataFrame):
    data["price"] = data["price"].apply(trim_price)




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
        #print([link.get_text().strip() for link in page_links])
        page_numbers = [int(link.get_text().strip()) for link in page_links if link.get_text().strip().isdigit()]
        return max(page_numbers) if page_numbers else 1

    # Function to scrape product data
    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        # print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            url = f'{base_url}?p={page}#/cena-0-7'
            #print(f'\nScraping page {page}: {url}')
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
                available = product.find('span',class_="img-sticker position-2") is None

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                title,size = extract_data_from_title(title)

                products_data.append({
                    'store':"Top gun",
                    'title': title,
                    'link': link,
                    'size': size,
                    'price': price,
                    "available":available
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
                available = product.find("div",{"data-equalizer-watch":"product-availability"}).get_text(strip=True)=="Dostępny"
                #print(available)
                title = title_tag if title_tag else "No title"
                price = price_tag if price_tag else "No price"
                link = link_tag if link_tag else "No link"

                title,size = extract_data_from_title(title)

                products_data.append({
                    'title': title,
                    "size":size,
                    'store':"Strefa Celu",
                    'price': price,
                    'available':available,
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
            url = f'{base_url}/{page}?p={page}'
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
                available = product.find('span',class_="product-availability-label").get_text() != "Brak"
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"

                title,size = extract_data_from_title(title)

                products_data.append({
                    'title': title,
                    'size':size,
                    'store':"Garand",
                    'price': price,
                    "available": available,
                    'link': link
                })
                #print(products_data[-1])

        return products_data

    # Run the scraper
    product_list = scrape_all_products()
    return product_list

def scrap_jmbron() -> [Offer]:
    # Base URL for the ammunition section
    base_url = 'https://jmbron.pl/kategoria-produktu/amunicja/'

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
        pagination = soup.find('ul', class_='page-numbers')
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
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}page/{page}/'
            print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('li', class_='product')

            for product in product_containers:
                title_tag = product.find('h2', class_='woocommerce-loop-product__title')
                price_tag = product.find('span', class_='woocommerce-Price-amount')
                link_tag = product.find('a', href=True)
                availability_tag = product.find('p', class_='in-stock')

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                link = urljoin(base_url, link_tag['href']) if link_tag else "No link"
                availability = availability_tag.get_text(strip=True)=="Na stanie" if availability_tag else False
                title,size = extract_data_from_title(title)
                products_data.append({
                    'title': title,
                    'price': price,
                    'link': link,
                    'size':size,
                    'availability': availability
                })
        print(products_data)
        return products_data

    product_list = scrape_all_products()
    return product_list

def scrap_magazynuzbrojenia() -> [Offer]:
    base_url = 'https://sklep.magazynuzbrojenia.pl/pl/c/Amunicja/1'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    # Function to get total number of pages
    def get_total_pages():
        response = requests.get(base_url, headers=headers,verify=False)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('ul', class_='paginator')
        if not pagination:
            return 1

        page_links = pagination.find_all('li')
        page_numbers = [int(link.get_text()) for link in page_links if link.get_text().isdigit()]
        return max(page_numbers) if page_numbers else 1

    # Function to scrape product data
    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'https://sklep.magazynuzbrojenia.pl/pl/c/Amunicja/{page}'
            print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers,verify=False)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='f-row description')

            for product in product_containers:
                title_tag = product.find('a', class_='prodname')
                price_tag = product.find('div', class_='price')
                link = f"https://sklep.magazynuzbrojenia.pl/{title_tag['href']}"
                availability_tag = product.find('p', class_='avail')
                #todo pin
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                #link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                availability = "brak towaru" not in availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title,size = extract_data_from_title(title)
                products_data.append({
                    'title': title,
                    'price': price,
                    'size':size,
                    'link': link,
                    'availability': availability
                })

        return products_data

    return scrape_all_products()

def scrap_kaliber() -> [Offer]:
    base_url = 'https://kaliber.pl/184-amunicja'

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

        pagination = soup.find("div",id="pagination_bottom").find_all("li")

        if not pagination:
            return 1

        #print([link.get_text().replace("\n","") for link in pagination ])
        page_numbers = [int(link.get_text().replace("\n","")) for link in pagination if link.get_text().replace("\n","").isdigit()]
        return max(page_numbers) if page_numbers else 1

    # Function to scrape product data
    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'https://kaliber.pl/184-amunicja#/page-{page}'
            print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('li', class_='productsSection-products-one')

            for product in product_containers:
                title_tag = product.find('h2', class_='product-name')
                price_tag = product.find('span', class_='price product-price')
                link_tag = product.find('a', class_='product-name')
                availability = bool(link_tag)

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                #availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title,size = extract_data_from_title(title)
                products_data.append({
                    'title': title,
                    'price': price,
                    'link': link,
                    'size':size,
                    'availability': availability
                })

        return products_data

    # Run the scraper
    return scrape_all_products()

STORES_SCRAPPERS = {
    "Garand":scrap_garand,
    "Top gun":scrap_top_gun,
    "Strefa celu":scrap_strefa_celu,
    "JM Bron":scrap_jmbron,
    "Magazyn uzbrojenia":scrap_magazynuzbrojenia,
    "Kaliber":scrap_kaliber,
}

#Jm bron
#Magazyn uzbrojenia
#Strefa celu
#Kaliber
#Gun center
#Salon broni
#Bazooka
# s = Scrapper()
#scrap_top_gun()
#scrap_strefa_celu()
#scrap_garand()
#s = scrap_jmbron()
#print(scrap_kaliber())
#print(s)
