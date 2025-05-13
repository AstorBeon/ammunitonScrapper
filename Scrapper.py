import re
from urllib.parse import urljoin
import pandas as pd
import requests
from bs4 import BeautifulSoup
from narwhals import DataFrame

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}


AVAILABLE_AMMO_SIZES = ["762x25","243Win","30-30 WIN",".222 REM","223 REM",".338","kal. 38Spec","38Spec",".38 Special",".357 Magnum",".357","kal. 45ACP","45ACP","7,65","7,62",".223Rem",".223REM",".223","308 Win","9mm", "9x19","9 mm", "308", ".22LR","22LR", "22 LR",".44 Rem.",".44", "9 PARA","357","12/70",".45 AUTO",".45 ACP",".45", "38 Super Auto",".40",  "10mm auto","10mm Auto","10mm","9 SHORT"]
AVAILABLE_DYNAMIC_AMMO_SIZES = [r"(\d{1,2}(,|\.)\d{1,2}x\d{1,2})",r"(\d{1,3}x\d{2})", r"(kal\. [\\/a-zA-Z0-9]+)"] #todo add more
AVAILABLE_AMMO_SIZE_MAPPINGS = {r"(9mm|9MM|9 mm|9 MM|9x19|9 PARA|9 SHORT)":"9mm",
                                r"(\.22LR|22LR|22 LR|\.22 LR|kal. 22LR,|kal.22LR|kal. 22lr)":".22LR",
                                r"(308|308Win|308 Win)":".308 Win",}

def extract_data_from_title(title:str) -> (str,str):
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

                if type(res[0]) in (list,tuple):
                    size = res[0][0]
                else:
                    size = res[0]

                #print(f"Size: {size}")
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
    return re.sub(r"[^0-9,\.]","",price_text).replace(",",".")

def get_all_existing_sizes(df:DataFrame)->[str]:
    if df.empty:
        return []
    options = list(set(df["size"].to_list()))

    return options

def map_sizes(data:pd.DataFrame) -> pd.DataFrame:

    data['size'] = data["size"].apply(lambda x:map_single_size(str(x)) )

    return data

def map_prices(data:pd.DataFrame) -> pd.DataFrame:
    data["price"] = data["price"].apply(trim_price)
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
            ##print(f'\nScraping page {page}: {url}')
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
                    "city":"Warsaw",
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

def scrap_strefa_celu() -> [dict]:

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
        #print(f"Total pages found: {total_pages}")
        
        for page in range(1, total_pages + 1):
            url = f'{base_url}?p={page}'
            #print(f'\nScraping page {page}: {url}')
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
                    "city": "Warsaw",
                    'title': title,
                    "size":size,
                    'store':"Strefa Celu",
                    'price': price,
                    'available':available.get_text(strip=True)=="Dostępny" if available else False,
                    'link': link
                })

        return products_data

    # Run the scraper
    product_list = scrape_all_products()
    return product_list

def scrap_garand() -> [dict]:
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
        #print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            url = f'{base_url}/{page}?p={page}'
            #print(f'\nScraping page {page}: {url}')
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
                    "city": "Warsaw",
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

def scrap_jmbron() -> [dict]:
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
        #print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}page/{page}/'
            #print(f'\nScraping page {page}: {url}')
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
                    "city": "Warsaw",
                    'title': title,
                    'price': price,
                    'link': link,
                    'size':size,
                    'available': availability,
                    'store': "JM Bron"
                })

        return products_data

    product_list = scrape_all_products()
    return product_list

def scrap_magazynuzbrojenia() -> [dict]:
    base_url = 'https://sklep.magazynuzbrojenia.pl/pl/c/Amunicja/1'

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
        #print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'https://sklep.magazynuzbrojenia.pl/pl/c/Amunicja/{page}'
            #print(f'\nScraping page {page}: {url}')
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
                availability = "brak towaru" not in availability_tag.get_text(strip=True)\
                    if availability_tag else "Availability unknown"
                title,size = extract_data_from_title(title)
                products_data.append({
                    "city": "Warsaw",
                    'title': title,
                    'price': price,
                    'size':size,
                    'link': link,
                    'available': availability,
                    'store':"Magazyn uzbrojenia"
                })

        return products_data

    return scrape_all_products()

def scrap_kaliber() -> [dict]:
    base_url = 'https://kaliber.pl/184-amunicja'

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
        page_numbers = [int(link.get_text().replace("\n",""))
                        for link in pagination if link.get_text().replace("\n","").isdigit()]
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
            #print(f'\nScraping page {page}: {url}')
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
                    "city": "Warsaw",
                    'title': title,
                    'price': price,
                    'link': link,
                    'size':size,
                    'available': availability,
                    'store':'Kaliber'
                })

        return products_data

    # Run the scraper
    return scrape_all_products()

def scrap_salonbroni() -> [dict]:
    base_url = 'https://www.salonbroni.pl/amunicja'

    url = f'https://www.salonbroni.pl/amunicja'
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    pages = soup.find("ul",class_="paginator")
    pages = max([int(x.get_text()) for x in pages.find_all("li") if x.get_text().isdigit()])


    # Function to scrape product data
    def scrape_products(page=1):
        products_data = []
        #todo update
        url = f'https://www.salonbroni.pl/amunicja/{page}'
        #print(f'\nScraping page {page}: {url}')
        response = requests.get(url, headers=headers, verify=False)

        soup = BeautifulSoup(response.text, 'html.parser')

        # Adjust the selectors based on the actual HTML structure
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
                "city": "Warsaw",
                'title': title,
                'price': price,
                'size':size,
                'link': link,
                'available': availability,
                'store':"Salon broni"
            })


        return products_data

    products = []

    for i in range(1, pages):
        products.extend(scrape_products(i))
    # Run the scraper
    return products

def scrap_bestgun() -> [dict]:
    base_url = 'https://www.bestgun.pl/amunicja-c-260.html'

    # Function to get total number of pages
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

    # Function to scrape product data
    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        print(f"Total pages found: {total_pages}")

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
                price = price_tag.get_text(strip=True) if price_tag else ""
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "city": "Ciechanów",
                    'title': title,
                    'price': price,
                    'link': link,
                    'size': size,
                    'available': availability,
                    'store': 'Best gun'
                })

        return products_data

    # Run the scraper
    return scrape_all_products()

def scrap_mex_armory() -> [dict]:
    base_url = 'https://mexarmory.pl/product-category/amunicja'

    # Function to get total number of pages
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

        # print([link.get_text().replace("\n","") for link in pagination ])

        return pagination

    # Function to scrape product data
    def scrape_all_products():
        products_data = []
        total_pages = get_total_pages()
        print(f"Total pages found: {total_pages}")

        for page in range(1, total_pages + 1):
            if page == 1:
                url = base_url
            else:
                url = f'{base_url}/page/{page}'
            # print(f'\nScraping page {page}: {url}')
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            product_containers = soup.find('div', class_='shop-container').find_all("div",class_="product-small")

            for product in product_containers:
                #content = product.find("div",class_="ProdCena")

                title_tag = product.find('div', class_="title-wrapper")
                price_tag = product.find('div', class_='price-wrapper')

                link_tag = product.find('a')

                availability = product.find("div",class_="out-of-stock-label")

                title = title_tag.get_text(strip=True) if title_tag else "No title"
                price = price_tag.get_text(strip=True) if price_tag else ""
                link = urljoin(base_url, link_tag['href']) if link_tag and link_tag.has_attr('href') else "No link"
                # availability = availability_tag.get_text(strip=True) if availability_tag else "Availability unknown"
                title, size = extract_data_from_title(title)
                products_data.append({
                    "city": "Warszawa",
                    'title': title,
                    'price': price,
                    'link': link,
                    'size': size,
                    'available': availability,
                    'store': 'Mex armory'
                })

        return products_data

    # Run the scraper
    return scrape_all_products()


STORES_SCRAPPERS = {
    "Garand":scrap_garand,
    "Top gun":scrap_top_gun,
    "Strefa Celu":scrap_strefa_celu,
    "JM Bron":scrap_jmbron,
    "Magazyn uzbrojenia":scrap_magazynuzbrojenia,
    "Kaliber":scrap_kaliber,
    "Salon broni":scrap_salonbroni,
    "Best gun":scrap_bestgun,
    "Mex armory": scrap_mex_armory
}


