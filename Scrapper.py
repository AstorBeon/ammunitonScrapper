import datetime
import math
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import cloudscraper
import pandas as pd
import requests
from bs4 import BeautifulSoup
from narwhals import DataFrame

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}


AVAILABLE_AMMO_SIZES = ["762x25","243Win","7×64","30-30 WIN",".222 REM","223 REM","223REM","223Rem","223rem","338 Win.",".338","kal. 38Spec","38Spec",".38 Special",".357 Magnum",".357","kal. 45ACP","45ACP","7,65","7,62×39","7,62",".223Rem",".223REM",".223","308 Win","9mm", "9x19","9×19","9×17","9 mm","9X19MM", "308", ".22LR","22LR", "22 LR",".22","22WMR",".44 Rem.",".44", "9 PARA","357","12/70",".45 AUTO",".45 ACP",".45", "38 Super Auto",".40",  "10mm auto","10mm Auto","10mm","9 SHORT",".300 BLK",".300",
                        "kal.380Auto","kal.50AE", ".30","kal:44","45","kal:38","0.38",".38","12/60","12/76","16/70","20/70","12/89","38SPL","22lr","300 AAC","9x19MM","9 LUGER",".25","6.5","12/67","12/76","7,63","12/65",
                        "kal.32",".17","30-06","5,6MM","6,5","7 x 65","270Win.","223 Rem","44 Mag","Śrutowa","śrutowa"]
AVAILABLE_DYNAMIC_AMMO_SIZES = [r"(\d{1,2}(,|\.)\d{1,2}x\d{1,2})",r"(\d{1,3}x\d{2})", r"(kal\. [\\/a-zA-Z0-9]+)"] #todo add more
AVAILABLE_AMMO_SIZE_MAPPINGS = {r"(9|9mm|9MM|9 mm|9 MM|9x19|9 PARA|9 SHORT|9×19|9x19MM|9X19MM|9 LUGER)":"9mm",
                                r"(\.22LR|22LR|22 LR|\.22 LR|kal. 22LR,|kal.22LR|kal. 22lr|22lr|22)":".22LR",
                                r"(308|308Win|308 Win)":".308 Win",
                                r"(38|0.38|kal. 38Spec|38Spec|38SPL|.38|kal:38)":"38 Special",
                                r"223 ?(REM|Rem|rem)":".223 Rem",
                                r"\.?338 ?(Win.)?":"338 Win",
                                r"45":".45"}

requests.packages.urllib3.disable_warnings()

def extract_data_from_title(title:str) -> (str,str):
    size = "?"
    global AVAILABLE_AMMO_SIZES,AVAILABLE_DYNAMIC_AMMO_SIZES
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

                if type(res[0]) in (list,tuple):
                    size = res[0][0]
                else:
                    size = res[0]

                title = title.replace(size,"")
                break
    return title, size

def map_single_size(size:str):
    size = size.replace("kal. ","")
    for key,val in AVAILABLE_AMMO_SIZE_MAPPINGS.items():
        if re.findall(key,size):
            return val
    return size

def trim_price(price_text:str) -> str:
    if type(price_text) != str:
        price_text = str(price_text)
    return re.sub(r"[^0-9,\\.]","",price_text).replace(",",".")


def clean_other_than_nums(text):
    return re.sub(r"[^0-9]", "", text)


def get_all_existing_sizes(df:DataFrame)->[str]:
    if df.empty:
        return []

    options = list(set(df["Kaliber"].to_list()))

    return options

def map_sizes(data:pd.DataFrame) -> pd.DataFrame:
    data['Kaliber'] = data["Kaliber"].apply(lambda x:map_single_size(str(x)) )

    return data

def map_prices(data:pd.DataFrame) -> pd.DataFrame:
    """
    Method for modifying prices to proper numeric format
    :param data: dataframe to be mapped
    :return:  updated dataframe
    """
    data["Cena"] = data["Cena"].apply(trim_price)
    return data

def _single_title_price_map(row):
    per_box = re.search(r"((opak\. ?\d+szt\.)|(op\. ?\d+szt\.\(zł/op\.\)))",row["Tytuł"])

    if per_box is not None:
        match_string = per_box.group(0)
        amount = re.sub(r"[^0-9]","",match_string)
        row["Tytuł"] = row["Tytuł"].replace(match_string,"")
        row["Cena"] = row["Cena"]/int(amount)

    return row


def map_prices_by_box_size(data:pd.DataFrame) -> pd.DataFrame:
    """
    Method for mapping existing ammo prices by box prices
    :param data: complete df to be mapped
    :return: df with updated values
    """
    data= data.apply(lambda x: _single_title_price_map(x),axis=1 )
    return data

def scrap_top_gun() -> [dict]:
    base_url = 'https://sklep.top-gun.pl/5-amunicja'

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
        page_numbers = [int(link.get_text().strip()) for link in page_links if link.get_text().strip().isdigit()]
        return max(page_numbers) if page_numbers else 1

    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            url = f'{base_url}?p={page}#/cena-0-7'
            #
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
                    "Miasto":"Warszawa",
                    "Sklep":"Top gun",
                    "Tytuł": title,
                    'Link': link,
                    "Kaliber": size,
                    "Cena": price,
                    "Dostępny":available
                })

        return products_data
    return scrape_all_products()


def scrap_strefa_celu() -> [dict]:

    # Base URL for the ammunition section
    base_url = 'https://strefacelu.pl/category/bron-palna-i-amunicja-amunicja-sportowa'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }


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


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()


        for page in range(1, total_pages + 1):
            url = f'{base_url}?p={page}'

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='product')

            for product in product_containers:
                title_tag = product.find('a', class_='product_name')
                price_tag = product.find('div', class_='main_price')
                link_tag = f"https://strefacelu.pl{product.find('a', class_='product_name')['href']}"
                available = product.find("div",{"data-equalizer-watch":"product-availability"})
                #print(available)
                title = title_tag.get_text() if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                link = link_tag if link_tag else "No link"

                title,size = extract_data_from_title(title)

                products_data.append({
                    "Miasto": "Warszawa",
                    "Tytuł": title,
                    "Kaliber":size,
                    "Sklep":"Strefa Celu",
                    "Cena": price if "LR" not in title else str(float(price)/50),
                    "Dostępny":available.get_text(strip=True)=="Dostępny" if available else False,
                    'Link': link
                })

        return products_data


    product_list = scrape_all_products()
    return product_list

def scrap_garand() -> [dict]:
    from urllib.parse import urljoin
    base_url = 'https://garand.com.pl/category/amunicja'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }


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


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()


        for page in range(1, total_pages + 1):
            url = f'{base_url}/{page}?p={page}'

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
                    "Miasto": "Warszawa",
                    "Tytuł": title,
                    "Kaliber":size,
                    "Sklep":"Garand",
                    "Cena": price,
                    "Dostępny": available,
                    'Link': link
                })
                #print(products_data[-1])

        return products_data


    product_list = scrape_all_products()
    return product_list

def scrap_jmbron() -> [dict]:
    # Base URL for the ammunition section
    base_url = 'https://jmbron.pl/kategoria-produktu/amunicja/'

    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }


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


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}page/{page}/'

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
                    "Miasto": "Warszawa",
                    "Tytuł": title,
                    "Cena": price,
                    'Link': link,
                    "Kaliber":size,
                    "Dostępny": availability,
                    "Sklep": "JM Bron"
                })

        return products_data

    product_list = scrape_all_products()
    return product_list

def scrap_magazynuzbrojenia() -> [dict]:
    base_url = 'https://sklep.magazynuzbrojenia.pl/pl/c/Amunicja/1'


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


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'https://sklep.magazynuzbrojenia.pl/pl/c/Amunicja/{page}'

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
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else "No price"
                availability = "brak towaru" not in availability_tag.get_text(strip=True)\
                    if availability_tag else "Availability unknown"
                title,size = extract_data_from_title(title)
                excluded_words = ["Pistolet","Karabin","Karabian","Rifle","Shotgun"]
                price_limit = 200
                if any([x in title for x in excluded_words]) or float(re.sub(".00","",re.sub(",",".",re.sub("[^0-9,]","",price)))) > price_limit:
                    continue
                products_data.append({
                    "Miasto": "Warszawa",
                    "Tytuł": title,
                    "Cena": price,
                    "Kaliber":size,
                    'Link': link,
                    "Dostępny": availability,
                    "Sklep":"Magazyn uzbrojenia"
                })

        return products_data

    return scrape_all_products()

def scrap_kaliber() -> [dict]:
    base_url = 'https://kaliber.pl/185-amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        pagination = soup.find_all("nav")[-1].find_all("a")

        if not pagination:
            return 1


        page_numbers = [re.sub("[^0-9]]","",l.get_text().strip()) for l in pagination]
        page_numbers = [int(p) for p in page_numbers if p.isdigit()]

        return max(page_numbers) if page_numbers else 1


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'https://kaliber.pl/185-amunicja#/page-{page}'
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('article')

            for product in product_containers:
                title_tag = product.find('p', class_='product-miniature__title')
                price_tag = product.find('span', class_='price product-price')

                link_tag = title_tag.find('a')

                availability = "?" #bool(link_tag)

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                link = link_tag['href'] if link_tag and link_tag.has_attr('href') else "No link"
                #availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title,size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Warszawa",
                    "Tytuł": title,
                    "Cena": "",
                    'Link': link,
                    "Kaliber":size,
                    "Dostępny": availability,
                    "Sklep":'Kaliber'
                })

        return products_data


    return scrape_all_products()

def scrap_salonbroni() -> [dict]:
    base_url = 'https://www.salonbroni.pl/amunicja'

    url = f'https://www.salonbroni.pl/amunicja'
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    pages = soup.find("ul",class_="paginator")
    pages = max([int(x.get_text()) for x in pages.find_all("li") if x.get_text().isdigit()])



    def scrape_products(page=1):
        products_data = []
        url = f'https://www.salonbroni.pl/amunicja/{page}'
        response = requests.get(url, headers=headers, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        product_containers = soup.find_all('div', class_='product_view-extended')

        for product in product_containers:
            title_tag = product.find('a', class_='prodname')
            price_tag = product.find('div', class_='price')
            availability = not product.find("button",class_="availability-notifier-btn")
            title = title_tag.get_text(strip=True) if title_tag else "No title"
            price = price_tag.get_text(strip=True) if price_tag else "No price"
            link = urljoin(base_url, title_tag['href']) if title_tag and title_tag.has_attr('href') else "No link"
            #availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
            title,size = extract_data_from_title(title)
            products_data.append({
                "Miasto": "Warszawa",
                "Tytuł": title,
                "Cena": price,
                "Kaliber":size,
                'Link': link,
                "Dostępny": availability,
                "Sklep":"Salon broni"
            })


        return products_data

    products = []

    for i in range(1, pages+1):
        products.extend(scrape_products(i))

    return products

def scrap_bestgun() -> [dict]:
    base_url = 'https://www.bestgun.pl/amunicja-c-260.html'

    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            pagination = int([x for x in soup.find("div", class_="IndexStron").find_all("a") if x.get_text().isdigit()][-1].get_text())
        except Exception as e:

            return 1

        return pagination

    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find('div', class_='ListingWierszeKontener').find_all("div",class_="LiniaDolna")

            for product in product_containers:
                content = product.find("div",class_="ProdCena")

                title_tag = content.find('h3')
                price_tag = content.find('span', class_='Cena')

                link_tag = product.find('a', class_='Zoom')
                availability = content.find_all("li")[-1].get_text() == "Dostępność:  Dostępny"

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True).replace(" zł/ szt.","") if price_tag else ""

                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Ciechanów",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability,
                    "Sklep": 'Best gun'
                })

        return products_data
    return scrape_all_products()

def scrap_mex_armory() -> [dict]:
    base_url = 'https://mexarmory.pl/product-category/amunicja'

    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            pagination = max([int(x.get_text()) for x in soup.find("nav", class_="woocommerce-pagination").find_all("a") if x.get_text().isdigit()])
        except Exception as e:
            print(e)
            return 1

        return pagination

    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}/page/{page}'
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find('div', class_='shop-container').find_all("div",class_="product-small")

            for product in product_containers:
                title_tag = product.find('div', class_="title-wrapper")
                price_tag = product.find('div', class_='price-wrapper')
                link_tag = product.find('a')
                availability = False if product.find("div",class_="out-of-stock-label") else True
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = re.sub("[^0-9,]","",price_tag.get_text(strip=True)) if price_tag else ""
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Warszawa",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability,
                    "Sklep": 'Mex armory'
                })

        return products_data


    return scrape_all_products()

def scrap_gun_eagle_rusznikarnia() -> [dict]:
    base_url = 'https://www.gun-eagle.pl/amunicja-c-7.html'

    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            pagination = int([x for x in soup.find("div", class_="IndexStron").find_all("a") if x.get_text().isdigit()][-1].get_text())
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'

            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='ElementListingRamka')

            for product in product_containers:
                content = product.find("div",class_="ProdCena")

                title_tag = content.find('h3')
                price_tag = content.find('span', class_='Cena')

                link_tag = product.find('a', class_='Zoom')
                availability = content.find_all("li")[-1].get_text() == "Dostępność:  Dostępny"

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                price = price.replace(" zł","")
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Ostrołęka",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability,
                    "Sklep": 'Gun eagle rusznikarnia'
                })

        return products_data


    return scrape_all_products()

def scrap_top_shot() -> [dict]:
    base_url = 'https://sklep.top-shot.pl/138-amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        def clean_other_than_nums(text):
            return re.sub(r"[^0-9]","",text)

        try:

            pagination = max([int(clean_other_than_nums(x.get_text(strip=True))) for x in soup.find("ul", class_="page-list").find_all("li") if clean_other_than_nums(x.get_text(strip=True)).isdigit()])
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        #print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='thumbnail-container')

            for product in product_containers:

                title_tag = product.find('h3')
                price_tag = product.find('div', class_='product-price-and-shipping')

                link_tag = product.find('a', class_='product-thumbnail')
                availability = product.find("li", class_="out_of_stock")
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                link = link_tag['href'] if link_tag and link_tag.has_attr('href') else "No link"
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Łódź",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability is None,
                    "Sklep": 'Top shot'
                })

        return products_data


    return scrape_all_products()

def scrap_kwatermistrz() -> [dict]:
    base_url = 'https://www.kwatermistrz.com.pl/amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:

            pagination = max([int(clean_other_than_nums(x.get_text(strip=True))) for x in soup.find("ul", class_="paginator").find_all("li") if clean_other_than_nums(x.get_text(strip=True)).isdigit()])
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()


        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='product-inner-wrap')

            for product in product_containers:

                title_tag = product.find('a',class_="prodname")
                price_tag = product.find('div', class_='product__basket')


                availability = product.find("form", class_="availability-notifier")
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""

                price = re.findall(r"Cena:[0-9,]+",price)[0].replace("Cena:","")
                link = title_tag['href'] if title_tag and title_tag.has_attr('href') else "No link"
                link = base_url + link
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Łódź",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability is None,
                    "Sklep": 'Kwatermistrz'
                })

        return products_data


    return scrape_all_products()

def scrap_c4guns() -> [dict]:
    base_url = 'https://c4guns.sklep.pl/16-amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:

            #pagination = max([int(clean_other_than_nums(x.get_text(strip=True))) for x in soup.find("ul", class_="paginator").find_all("li") if clean_other_than_nums(x.get_text(strip=True)).isdigit()])
            page_desc = soup.find("div",{"id":"js-product-list-top"}).find("span",class_="hidden-sm-down").get_text()
            #current = re.findall(r"\d+-\d+",page_desc)[0]
            total = re.findall(r"z \d+",page_desc)[0]
            pagination = math.ceil(int(total[2:])/24)
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('article', class_='product-miniature-default')

            for product in product_containers:
                container = product.find('div', class_='product-description')
                title_tag = container.find('h2',class_="product-title")
                link_tag = product.find('div',class_="thumbnail-container").find("a")
                price_tag = container.find('div', class_='product-price-and-shipping')


                #availability = product.find("form", class_="availability-notifier")
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                #print(price)
                #print(price)
                price = price.replace(" zł:","")
                link = link_tag['href'] if link_tag and link_tag.has_attr('href') else "No link"

                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Łódź",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": "?",
                    "Sklep": 'C4guns'
                })

        return products_data


    return scrape_all_products()

def scrap_puchacz() -> [dict]:
    base_url = 'https://www.puchacz.net/amunicja-c-6.html?'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:

            pagination = max([int(clean_other_than_nums(x.get_text(strip=True))) for x in soup.find("div", class_="IndexStron").find_all("a") if clean_other_than_nums(x.get_text(strip=True)).isdigit()])
            #page_desc = soup.find("div",{"id":"js-product-list-top"}).find("span",class_="hidden-sm-down").get_text()
            #current = re.findall(r"\d+-\d+",page_desc)[0]
            #total = re.findall(r"z \d+",page_desc)[0]
            #pagination = math.ceil(int(total[2:])/24)
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='OknoRwd')

            for product in product_containers:

                container = product.find('div', class_='ProdCena')
                if container is None:
                    #Skipping last
                    continue
                title_tag = container.find('h3')
                link_tag = product.find('a',class_="Zoom")
                price_tag = container.find('div', class_='ProduktCena')


                #print(product.find("ul", class_="ListaOpisowa").find("img")["alt"])
                availability = product.find("ul", class_="ListaOpisowa").find("img")["alt"]=="Dostępny"
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                #print(price)
                #print(price)
                price = price.replace(" zł:","")
                link = link_tag['href'] if link_tag and link_tag.has_attr('href') else "No link"
                link = base_url + link
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Piotrków Trybunalski",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability,
                    "Sklep": 'Puchacz'
                })

        return products_data


    return scrape_all_products()

def scrap_rparms() -> [dict]:
    base_url = 'https://rparms.pl/4-amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:
            #print(soup.find("div",class_="text-pagination").get_text(strip=True))
            pagination =int(soup.find("div",class_="text-pagination").get_text(strip=True)[20:-10])


            #page_desc = soup.find("div",{"id":"js-product-list-top"}).find("span",class_="hidden-sm-down").get_text()
            #current = re.findall(r"\d+-\d+",page_desc)[0]
            #total = re.findall(r"z \d+",page_desc)[0]
            #pagination = math.ceil(int(total[2:])/24)
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return math.ceil(pagination/50)


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)
            #print(f"Page {page} of {total_pages}")
            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('article', class_='js-product-miniature')

            for product in product_containers:

                container = product.find('div', class_='product_desc')
                if container is None:
                    #Skipping last
                    continue
                title_tag = container.find('h3')
                link_tag = product.find('a')
                price_tag = product.find('span', class_='price')


                #print(product.find("ul", class_="ListaOpisowa").find("img")["alt"])
                try:
                    availability_poznan,availability_aleks = (
                        [x.get_text(strip=True) for x in container.find("div", class_="availability_on_listing").find_all("span")])
                except Exception as e:
                    availability_poznan=False
                    availability_aleks=False

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                #print(price)
                #print(price)
                price = re.sub(r"( |[^0-9])zł","",price)
                #print(price)
                link = link_tag['href'] if link_tag and link_tag.has_attr('href') else "No link"
                link = base_url + link
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)

                products_data.append({
                    "Miasto": "Poznań",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability_poznan=="Dostępny",
                    "Sklep": 'RParms'
                })

                products_data.append({
                    "Miasto": "Aleksandrów Łódzki",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability_aleks=="Dostępny",
                    "Sklep": 'RParms'
                })


        return products_data


    return scrape_all_products()

def scrap_astorclassic() -> [dict]:
    base_url = 'https://astroclassic.pl/kat/amunicja/'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:
            #print(soup.find("div",class_="text-pagination").get_text(strip=True))
            pagination =max(
                [int(x.get_text()) for x in soup.find("nav",class_="woocommerce-pagination").
                            find_all("li") if x.get_text().isdigit()]
            )


            #page_desc = soup.find("div",{"id":"js-product-list-top"}).find("span",class_="hidden-sm-down").get_text()
            #current = re.findall(r"\d+-\d+",page_desc)[0]
            #total = re.findall(r"z \d+",page_desc)[0]
            #pagination = math.ceil(int(total[2:])/24)
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()


        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)
            #print(f"Page {page} of {total_pages}")
            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='astra-shop-summary-wrap')

            for product in product_containers:

                title_tag = product.find('a')

                price_tag = product.find('span', class_='ammo_price')


                #print(product.find("ul", class_="ListaOpisowa").find("img")["alt"])
                try:
                    availability_tarnobrzeg,availability_poznan = (
                        [x["id"]=="yes" for x in product.find("span",id=True)])
                except Exception as e:

                    availability_tarnobrzeg=False
                    availability_poznan=False

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                #print(price)
                #print(price)
                price = re.sub(r"( |[^0-9]+)zł","",price).replace("/szt.","")
                #print(price)
                link = title_tag['href'] if title_tag and title_tag.has_attr('href') else "No link"

                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)

                products_data.append({
                    "Miasto": "Tarnobrzeg",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability_tarnobrzeg,
                    "Sklep": 'Astroclassic'
                })

                products_data.append({
                    "Miasto": "Poznań",
                    "Tytuł": title,
                    "Cena": price,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availability_poznan,
                    "Sklep": 'Astroclassic'
                })


        return products_data



    return scrape_all_products()

#https://gunsmasters.pl/produkty/amunicja,2,55
def scrap_gunsmasters() -> [dict]:
    base_url = 'https://gunsmasters.pl/produkty/dostępność=dostępny-/amunicja,2,55'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:

            pagination = max([int(clean_other_than_nums(x.get_text(strip=True))) for x in soup.find("ul", class_="pagination").find_all("li") if clean_other_than_nums(x.get_text(strip=True)).isdigit()])
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='product-item')

            for product in product_containers:

                title_tag = product.find('h2')
                prices_tag = product.find_all('span', class_='price')
                #print([x.get_text(strip=True) for x in prices_tag])

                #availability = product.find("form", class_="availability-notifier")
                title = title_tag.get_text(strip=True) if title_tag else "No title"
                try:
                    price = product.find("span", class_="red-price").get_text(strip=True)

                except Exception as e:
                    price = prices_tag[2].get_text(strip=True)

                availibility = len(prices_tag)==3

                price = price.replace("Cena:","").replace(" zł","")
                link = product.find("a",class_="product-url")['href']
                link = f"https://gunsmasters.pl/{link}"
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Wrocław",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility != "Powiadom o dostępności",
                    "Sklep": 'Gunmasters'
                })

        return products_data


    return scrape_all_products()

def scrap_knieja() -> [dict]:
    base_url = 'https://www.knieja.com.pl/30-amunicja-i-elaboracja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        try:

            pagination = max([int(clean_other_than_nums(x.get_text(strip=True))) for x in soup.find("ul", class_="pagination").find_all("li") if clean_other_than_nums(x.get_text(strip=True)).isdigit()])
        except Exception as e:
            print(e)
            return 1

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination


    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}?page={page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div', class_='card-product')

            for product in product_containers:

                title_tag = product.find('h2')
                try:
                    price = product.find('span', class_='price').get_text(strip=True)
                    availibility=True
                except:
                    price=''
                    availibility = False
                #print([x.get_text(strip=True) for x in prices_tag])

                #availability = product.find("form", class_="availability-notifier")
                title = title_tag.get_text(strip=True) if title_tag else "No title"

                #availibility = "zł" in price

                price = price.replace("Cena:","").replace(" zł","").replace("\xa0zł","")

                link = product.find("a")['href']

                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Kraków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Knieja'
                })

        return products_data


    return scrape_all_products()

def scrap_atenagun() -> [dict]:
    base_url = 'https://www.atenagun.pl/kategoria/amunicja'

    def scrape_all_products():
        products_data = []
        page=1
        while True:

            url = f'{base_url}/page/{page}/'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)
            page+=1
            if response.status_code != 200:

                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('article')

            for product in product_containers:

                title_tag = product.find('h2')
                try:
                    price = product.find('span', class_='woocommerce-Price-amount').get_text(strip=True)

                except:
                    price=''

                #print([x.get_text(strip=True) for x in prices_tag])
                availibility = product.find("div",class_="stock").get_text(strip=True) == "w magazynie"
                #availability = product.find("form", class_="availability-notifier")
                title = title_tag.get_text(strip=True) if title_tag else "No title"

                #availibility = "zł" in price

                price = price.replace("Cena:","").replace("zł","").replace("\xa0zł","")

                link = product.find("a")['href']

                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                if price=="0" or "PROMOCJA" in title:
                    continue

                #Price adjustment for packs
                if float(price.replace(",","."))>20:
                    amount_in_title = re.search(r"[0-9]\.szt",title)
                    if amount_in_title:
                        div = int(amount_in_title.group(0)[:-4])
                        price = str(round(float(price)/div,2))
                    else:
                        #Additional request
                        subresponse = requests.get(link, headers=headers)

                        subsoup = BeautifulSoup(subresponse.text, 'html.parser')
                        maindiv = subsoup.find("div",class_="woocommerce-product-attributes-item--attribute_pa_opakowanie")
                        #print(maindiv.get_text(strip=True)) #todo continue here
                        try:
                            amount = int(re.sub(r"[^0-9]","",maindiv.get_text(strip=True)))
                            price = float(price.replace(",", "."))/amount
                        except Exception as e:
                            price="?"

                products_data.append({
                    "Miasto": "Kraków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Atena Gun'
                })

        return products_data


    return scrape_all_products()

def scrap_snajper() -> [dict]:
    base_url = 'https://sklepsnajper.pl/kategoria/amunicja/'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="page-numbers").find_all("li") if x.get_text(strip=True).isdigit()])
        except:
            return 1




    def scrape_all_products():
        products_data = []
        page=1
        while True:

            url = f'{base_url}/page/{page}/'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)
            page+=1
            if response.status_code != 200:

                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('li', class_="product-type-simple")

            for product in product_containers:

                title_tag = product.find('h2')
                try:
                    price = product.find('span', class_='price').get_text(strip=True)

                except:
                    price=''

                #print([x.get_text(strip=True) for x in prices_tag])
                try:
                    availibility = product.find("span",class_="ast-shop-product-out-of-stock").get_text(strip=True)
                    availibility = False
                except:
                    availibility = True
                #availability = product.find("form", class_="availability-notifier")
                title = title_tag.get_text(strip=True) if title_tag else "No title"

                #availibility = "zł" in price

                price = price.replace("Cena:","").replace("zł","").replace("\xa0zł","")

                link = product.find("a")['href']

                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Kraków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Snajper'
                })

        return products_data

    return scrape_all_products()

def scrap_coltwroclaw() -> [dict]:
    base_url = 'https://coltwroclaw.pl/18-amunicja'

    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="page-list").find_all("li") if x.get_text(strip=True).isdigit()])
        except:
            return 1




    def scrape_all_products():
        products_data = []
        pages = get_total_pages()

        for page in range(pages):

            url = f'{base_url}?page={page}/'

            response = requests.get(url, headers=headers)
            page+=1
            if response.status_code != 200:

                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('article')

            for product in product_containers:

                title_tag = product.find('h2')
                try:
                    price = product.find('div', class_='product-price-and-shipping').get_text(strip=True)
                except:
                    price=''
                try:
                    availibility = product.find("div",class_="product-availability").get_text(strip=True)=="Dostępny"
                except:
                    availibility = False

                title = title_tag.get_text(strip=True) if title_tag else "No title"

                price = re.sub(r"[^0-9,\.]","",price)

                link = product.find("a")['href']
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Kraków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Colt Wroclaw'
                })

        return products_data


    return scrape_all_products()

def scrap_vismag() -> [dict]:
    base_url = 'https://bron-sklep.pl/11-amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="pagination").find_all("li") if x.get_text(strip=True).isdigit()])
        except:
            return 1




    def scrape_all_products():
        products_data = []

        for page in range(get_total_pages()):
            url = f'{base_url}?p={page}/'

            response = requests.get(url, headers=headers)
            page+=1
            if response.status_code != 200:

                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all("li",class_="ajax_block_product")

            for product in product_containers:

                title_tag = product.find('div',class_="product_name")
                try:
                    price = product.find('p', class_='price_container').get_text(strip=True)
                except:
                    price=''

                #availibility = "?"

                title = title_tag.get_text(strip=True) if title_tag else "No title"

                price = re.sub(r"[^0-9,\.]","",price)

                link = product.find("a")['href']

                #Additional check for availibility
                resp = requests.get(link, headers=headers)
                soup = BeautifulSoup(resp.text, 'html.parser')
                availibility = soup.find("span",{"id":"availability_value"}).get_text(strip=True) == "Ten produkt nie występuje już w magazynie"

                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Lublin",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Vismag'
                })


        return products_data


    return scrape_all_products()

def scrap_bazooka() -> [dict]:
    base_url = "https://bazooka.waw.pl/?amunicja"

    def scrape_all_products():
        products_data = []


        response = requests.get(base_url, headers=headers)

        if response.status_code != 200:
            print(response.status_code)
            return

        soup = BeautifulSoup(response.text, 'html.parser')

        #h2 rozmiar
        #UL - oferty
            #LI - poszczególne produkty

        sizes = soup.find_all("h2")[1:]
        uls = soup.find_all("ul")
        for size,ul in zip(sizes,uls):
            size = size.get_text(strip=True) if "Pozostałe" not in size.get_text(strip=True) else None

            for li in ul.find_all("li"):

                line = li.get_text(strip=True) #strong 0 cena
                price = re.sub(r"([^0-9,\\.])","",li.find("strong").get_text(strip=True))[:-1]
                title = line.replace(price,"")
                if size is None:
                    title, size = extract_data_from_title(title)

                if "(brak)" in title:
                    title = title.replace("(brak)","")
                    price = None


                price = re.findall(r"[0-9,]+zł",title)[0][:-2]


                products_data.append({
                    "Miasto": "Pruszków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": f"https://www.bazooka.sklep.pl/search?q={title.replace(' ','+').replace('\\xa','')}",
                    "Kaliber": size,
                    "Dostępny": True,
                    "Sklep": 'Bazooka'
                })


        return products_data


    return scrape_all_products()

def scrap_bazooka_updated() -> [dict]:
    base_url = "https://bazooka.waw.pl/?amunicja"

    def scrape_all_products():
        products_data = []


        response = requests.get(base_url, headers=headers)

        if response.status_code != 200:
            print(response.status_code)
            return

        soup = BeautifulSoup(response.text, 'html.parser').find(id="page")

        #h2 rozmiar
        #UL - oferty
            #LI - poszczególne produkty

        sizes = soup.find_all("strong")[4:]
        uls = soup.find_all("ul")


        for size,ul in zip(sizes,uls):
            size = size.get_text(strip=True) if "Pozostałe" not in size.get_text(strip=True) else None

            for li in ul.find_all("li"):

                line = li.get_text(strip=True) #strong 0 cena

                try:
                    price = re.search(r"\([0-9,]+",line).group(0)#.strip().replace("/szt","")
                    price = re.sub(r"([^0-9,\\.])","",price)
                except Exception:
                    continue
                title = line.replace(price,"")
                if size is None:
                    title, size = extract_data_from_title(title)

                if "(brak)" in title:
                    title = title.replace("(brak)","")
                    price = None

                #Special handling for price per pack
                if "zł/op" in title and price is not None and "/szt" not in title:

                    per_pack = int(re.sub(r"[^0-9]","",re.findall(r"op\. ?\d+",title)[0]))
                    price = float(price)/per_pack
                    price = str(price).replace(".",",")

                title = title.replace("/szt","").replace("(zł.)","").replace("zł. ","")
                products_data.append({
                    "Miasto": "Pruszków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": f"https://www.bazooka.sklep.pl/search?q={title.replace(' ','+').replace('\\xa','')}",
                    "Kaliber": size,
                    "Dostępny": True,
                    "Sklep": 'Bazooka'
                })


        return products_data


    return scrape_all_products()


def scrap_cyngiel() -> [dict]:
    base_url = 'https://cyngiel.com.pl/sklep-z-bronia/amunicja-do-broni-palnej/?products-per-page=all'



    def scrape_all_products():
        products_data = []


        response = requests.get(base_url, headers=headers)

        if response.status_code != 200:
            print("Failed to pull for cyngiel")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        product_containers = soup.find_all('div',class_="product-inner")

        for product in product_containers:

            title_tag = product.find('h2')
            link = title_tag.find('a')['href']
            try:
                price = product.find('bdi').get_text(strip=True)
            except:
                price=''

            availibility = product.find("div",class_="outofstock-badge") is None



            title = title_tag.get_text(strip=True) if title_tag else "No title"

            price = re.sub(r"[^0-9,\.]","",price)

            title, size = extract_data_from_title(title)
            template = {
                "Miasto": "Warszawa",
                "Tytuł": title,
                "Cena": price ,
                "Link": link,
                "Kaliber": size,
                "Dostępny": availibility,
                "Sklep": 'Cyngiel'
            }

            products_data.append(template)
            template_siedlce = template.copy()
            template_siedlce["Miasto"]="Siedlce"
            products_data.append(template_siedlce)



        return products_data


    return scrape_all_products()

def scrap_emilitaria() -> [dict]:
    base_url = 'https://e-militaria.pl/amunicja-946'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("nav",class_="pagination").find_all("li") if x.get_text(strip=True).isdigit()])
        except:
            return 1




    def scrape_all_products():
        products_data = []
        pages = get_total_pages()
        for page in range(pages):

            if page != 0:
                url = f'{base_url}?page={page+1}'
            else:
                url = base_url

            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div',class_="product-description")

            for product in product_containers:

                title = product.find("div",class_="col")
                price = product.find("span",class_="product-price").get_text(strip=True)
                title = title.find("h4").get_text(strip=True)



                #print([x.get_text(strip=True) for x in prices_tag])
                try:
                    availibility =product.find("li",class_="out_of_stock").get_text(strip=True) is not None
                except:
                    availibility = True

                #availability = product.find("form", class_="availability-notifier")
                #title = title_tag.get_text(strip=True) if title_tag else "No title"

                #availibility = "zł" in price

                price = price.replace("Cena:","").replace("zł","").replace("\xa0","")

                link = product.find("a")['href']

                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Mirków",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'E-militaria'
                })

        return products_data

    return scrape_all_products()

def scrap_edex() -> [dict]:
    base_url = 'https://edexbron.pl/kategoria/amunicja-1'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return int(soup.find("div",class_="pagination__page-selector").find("span",class_="pagination__page-selector-text").get_text(strip=True).split(" ")[1])

        except:
            return 1




    def scrape_all_products():
        products_data = []

        for page in range(get_total_pages()):

            url = f'{base_url}/{page}'
            response = requests.get(url, headers=headers)
            page+=1
            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('product-tile')
            for product in product_containers:

                title_tag = product.find('h3')
                try:
                    price = product.find('div', class_='product-tile__price').get_text(strip=True)

                except:
                    price=''
                try:
                    availibility = "Brak" in product.find("strong",class_="product-tile__availability-value").get_text(strip=True)
                except:
                    #On error - it's a ad on the side of the page
                    continue

                title = title_tag.get_text(strip=True) if title_tag else "No title"


                price = price.replace("Cena:","").replace("zł","").replace("\xa0zł","")
                link = f"{base_url}{product.find('a')['href']}"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Jasło",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Edex'
                })

        return products_data

    return scrape_all_products()

def scrap_goldguns() -> [dict]:
    base_url = 'https://goldguns.pl/pl/c/AMUNICJA/288'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="paginator").find_all("li") if x.get_text(strip=True).isdigit()])

        except:
            return 1




    def scrape_all_products():
        products_data = []

        for page in range(get_total_pages()):

            url = f'{base_url}/{page}'
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div',class_="modProdBoxContainer")
            for product in product_containers:

                title_tag = product.find('span',class_="productname")
                try:
                    price = product.find('div', class_='price').get_text(strip=True)

                except:
                    price=''

                availibility =  product.find("button",class_="availability-notifier-btn") is None


                title = title_tag.get_text(strip=True) if title_tag else "No title"


                price = re.search(r"\d+,\d+",price).group(0)
                link = f"{base_url}{product.find('a')['href']}"
                title, size = extract_data_from_title(title)

                products_data.append({
                    "Miasto": "Poznań",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'GoldGuns'
                })

        return products_data

    return scrape_all_products()

def scrap_gunmonkey() -> [dict]:
    base_url = 'https://gunmonkey.pl/amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="paginator").find_all("li") if x.get_text(strip=True).isdigit()])

        except:
            return 1




    def scrape_all_products():
        products_data = []

        for page in range(get_total_pages()):

            url = f'{base_url}/{page}'
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('div',class_="product-inner-wrap")
            for product in product_containers:

                title_tag = product.find('a',class_="prodname")
                try:
                    price = product.find('div', class_='price').get_text(strip=True)

                except:
                    price=''

                availibility =  product.find("button",class_="addtobasket") is not None


                title = title_tag.get_text(strip=True) if title_tag else "No title"


                price = re.search(r"\d+,\d+",price).group(0)
                link = f"https://gunmonkey.pl{title_tag['href']}"
                title, size = extract_data_from_title(title)

                if "(1op" in title:

                    amount = re.search(r"\(1op(ak)?.?=? ?\d+ ?s?zt\.?\)",title).group(0)[5:]
                    amount = int(re.sub("[^0-9]","",amount))
                    price = float(price.replace(",","."))/amount



                products_data.append({
                    "Miasto": "Jaworzno",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Gun Monkey'
                })

        return products_data

    return scrape_all_products()

def scrap_proce_i_pestki() -> [dict]:
    base_url = 'https://proceipestki.pl/kategoria-produktu/amunicja/'
    pnp_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://proceipestki.pl/",
    }

    def get_total_pages():
        session = requests.Session()
        session.headers.update(pnp_headers)
        response = session.get(base_url)

        scraper = cloudscraper.create_scraper()
        url = "https://proceipestki.pl/kategoria-produktu/amunicja/page/2/"
        response = scraper.get(url).text

        # if response.status_code != 200:
        #     print(f"Failed to load the page: {response.status_code}")
        #     print(response.text)
        #     return 1

        soup = BeautifulSoup(response, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="page-numbers").find_all("li") if x.get_text(strip=True).isdigit()])

        except:
            return 1




    def scrape_all_products():
        products_data = []

        scraper = cloudscraper.create_scraper()

        for page in range(get_total_pages()):

            url = f'{base_url}/page/{page}'

            response = scraper.get(url).text
            soup = BeautifulSoup(response, 'html.parser')
            # if response.status_code != 200:
            #     break


            product_containers = soup.find_all('li',class_="product")
            for product in product_containers:

                title_tag = product.find('h2')
                try:
                    price = product.find('span', class_='price').get_text(strip=True)

                except:
                    price=''



                availibility = product.find("span",class_="out-of-stock-sticker") is not None


                title = title_tag.get_text(strip=True) if title_tag else "No title"


                price = price.replace("zł","")
                link = product.find('a')['href']
                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Łódź",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Proce i Pestki'
                })

        return products_data
    res = scrape_all_products()

    return res


def scrap_siwiaszczyk() -> [dict]:
    base_url = 'https://siwiaszczyk.pl/amunicja-do-broni'


    # def get_total_pages():
    #     response = requests.get(base_url, headers=headers)
    #     if response.status_code != 200:
    #         print(f"Failed to load the page: {response.status_code}")
    #         return 1
    #
    #     soup = BeautifulSoup(response.text, 'html.parser')
    #
    #     try:
    #         return max([int(x.get_text(strip=True)) for x in soup.find("div",class_="ep-pagination").find_all("li") if x.get_text(strip=True).isdigit()])
    #
    #     except:
    #         return 1




    def scrape_all_products():
        products_data = []
        page=1 #Aż do 41!!!
        while True:

            url = f'{base_url}/{page}'
            page+=1
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Fail code: {response.status_code}")
                break




            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('product-tile')

            for product in product_containers:

                title = product["name"]
                try:
                    price = str(product["price"])

                except:
                    price=''
                link = f"{base_url}{product.find('a')['href']}"
                try:
                    subresponse = requests.get(link)
                    subsoup =BeautifulSoup(subresponse.text, 'html.parser')
                    availibility = "Dostępny" in subsoup.find("strong",class_="product-availability__description_unavailable").get_text(strip=True)
                except:
                    availibility=False





                title, size = extract_data_from_title(title)
                products_data.append({
                    "Miasto": "Łódź",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Siwiaszczyk'
                })

            if len(product_containers)<10:

                break


        return products_data

    return scrape_all_products()


def scrap_trop() -> [dict]:
    base_url = 'https://sklep-mysliwski.com/5-amunicja'


    def get_total_pages():
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to load the page: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            return max([int(x.get_text(strip=True)) for x in soup.find("ul",class_="pagination").find_all("li") if x.get_text(strip=True).isdigit()])

        except:
            return 1




    def scrape_all_products():
        products_data = []

        for page in range(get_total_pages()):

            url = f'{base_url}/{page}'
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find_all('article',class_="product-miniature")
            for product in product_containers:

                title_tag = product.find('h3')
                try:
                    price = product.find_all('span', class_='price')[-1].get_text(strip=True)

                except:
                    price=''

                availibility =  "niedostępny" not in product.find("p").get_text(strip=True)


                title = title_tag.get_text(strip=True) if title_tag else "No title"


                price = re.search(r"\d+,\d+",price).group(0)
                link = f"{product.find_all('a')[-1]['href']}"
                title, size = extract_data_from_title(title)

                products_data.append({
                    "Miasto": "Poznań",
                    "Tytuł": title,
                    "Cena": price ,
                    "Link": link,
                    "Kaliber": size,
                    "Dostępny": availibility,
                    "Sklep": 'Trop'
                })

        return products_data

    return scrape_all_products()



STORES_SCRAPPERS = {
    "Garand":scrap_garand, #Warszawa
    "Top gun":scrap_top_gun, #Warszawa
    "Strefa Celu":scrap_strefa_celu, #Warszawa
    "JM Bron":scrap_jmbron, #Warszawa
    "Magazyn uzbrojenia":scrap_magazynuzbrojenia, #Warszawa
    "Kaliber":scrap_kaliber, #Warszawa
    "Salon broni":scrap_salonbroni, #Warszawa
    "Best gun":scrap_bestgun, #Ciechanów
    "Mex armory": scrap_mex_armory, #Warszawa
    "Bazooka" : scrap_bazooka_updated,
    "Gun eagle rusznikarnia": scrap_gun_eagle_rusznikarnia, #Ostrołęka
    "Top shot": scrap_top_shot, #Łódź
    "Kwatermistrz":scrap_kwatermistrz, #Łódź
    "C4guns":scrap_c4guns, #Piotrków Trybunalski
    "RParms":scrap_rparms, #Aleksandrów Łódzki
    "Astroclassic":scrap_astorclassic, #Poznań
    "Gunmasters":scrap_gunsmasters, #Wrocław,
    "Colt Wroclaw":scrap_coltwroclaw, #Wrocław
    "Knieja":scrap_knieja, #Kraków,
    "Atena Gun":scrap_atenagun, #Kraków
    "Snajper":scrap_snajper, #Kraków
    "Vismag":scrap_vismag, #Lublin
    "Cyngiel":scrap_cyngiel, #Warszawa/Siedlce/Kobyłka,
    "E-militaria":scrap_emilitaria, #Mirków
    "Edex":scrap_edex, #Jasło
    "GoldGuns":scrap_goldguns, #Poznań
    "Gun Monkey":scrap_gunmonkey, #Jaworzno
    "Proce i Pestki":scrap_proce_i_pestki, #Łódź
    #"Siwiaszczyk": scrap_siwiaszczyk, #Łódź
    "Trop":scrap_trop #Wrocław
}

def normalize_data(df:list):
    total_df = pd.DataFrame(df)
    total_df = total_df[["Miasto", "Tytuł", "Cena", "Link", "Kaliber", "Dostępny", "Sklep"]]
    total_df = map_sizes(total_df)

    total_df = map_prices(total_df)

    exclude_regex = r"([Pp]ude[lł]ko)|(SZKOLENIE)|(SPŁONKI|Spłonki|spłonki)"

    total_df = total_df[~total_df['Tytuł'].str.contains(exclude_regex,regex=True)]



    total_df.columns = ["Miasto","Tytuł","Cena","Link","Kaliber","Dostępny","Sklep"]

    def drop_all_odd(value):
        subval = re.subn(r"[^0-9,\\.]", "", value, count=1)[0].replace(".00", "")
        subval = re.sub("\\.{2}[0-9]+$", "", subval)
        if subval.count(".") == 2:
            subval = re.sub(r"\.[0-9]{2}\.?$", "", subval)

        return subval

    # total_df.to_excel("tmp.xlsx", index=False)

    total_df["Cena"] = pd.to_numeric(total_df["Cena"].fillna('').apply(lambda x: drop_all_odd(x)),
                                      errors='coerce')  # .fillna('-1')

    total_df['Dostępny'] = total_df['Dostępny'].apply(lambda x: "T" if x  else ("N" if not x else "?"))

    #adjusting prices for box size (if available)
    total_df = map_prices_by_box_size(total_df)

    total_df.drop_duplicates(inplace=True)


    #re.sub(r"\.", "", "aa.bb.c")
    return total_df


def refurbished_scrap_all(multithread=True):
    start = time.perf_counter()
    complete_data = []
    stores_completed = {x:"?" for x in STORES_SCRAPPERS.keys()}



    def pull_single_store(store_name_arg,additional_success_info=""):

        try:
            if not multithread:
                print(f"Pulling {store_name_arg}..." , end="\r")
            res = STORES_SCRAPPERS[store_name_arg]()
            try:
                complete_data.extend(res)
            except Exception as e:
                print(e)
                stores_completed[store_name_arg] = False
                print(f"Failure for: {store_name_arg}")
            #st.session_state["pulled_data"][store_name_arg] = res
            if not res:
                stores_completed[store_name_arg] = False
                print(f"ERROR - Failed to scrap {store_name_arg}")
            else:
                stores_completed[store_name_arg] = True
                print(f"OK - Successfully scrapped {store_name_arg} -> {len(res)} items - {additional_success_info}")



        except Exception as e:

            print(traceback.print_exc())

            stores_completed[store_name_arg] = False
            print(f"ERROR - Failed to scrap {store_name_arg}")


    #Multi thread
    if multithread:
        with ThreadPoolExecutor(max_workers=5) as executor:

            futures = [executor.submit(pull_single_store,store) for store,count in zip(STORES_SCRAPPERS.keys(),range(len(STORES_SCRAPPERS)))]

            for f in futures:
                _ = f.result()

                pass
    else:
        # Single thread
        for store,count in zip(STORES_SCRAPPERS.keys(),range(len(STORES_SCRAPPERS))):
            pull_single_store(store,f"{count+1}/{len(STORES_SCRAPPERS)}")


    complete_df = normalize_data(complete_data)

    end = time.perf_counter()
    #complete_df.to_excel("Complete data.xlsx",index=False)
    filename = f"data/my_silly_database_{datetime.datetime.now().strftime('%d_%m_%Y')}.xlsx"
    complete_df.to_excel(filename, index=False)
    totaltime=(end - start)/60
    print(f"Elapsed: {totaltime:.2f} minutes")
    return filename


#refurbished_scrap_all(True)


#refurbished_scrap_all()
# files = os.listdir("data")
# print(files)
# files.sort(key=lambda x: datetime.datetime.strptime(x.replace(".xlsx","").replace("my_silly_database_",""), "%d_%m_%Y"),reverse=True)
#
# print(files)
#
#
