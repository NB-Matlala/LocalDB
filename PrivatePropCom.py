####### out & in comm extract ############
import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import gzip
import math
from datetime import datetime
from azure.storage.blob import BlobClient
import os
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options

# chrome_options = Options()
# chrome_options.add_argument("--headless")
# driver = webdriver.Chrome(options=chrome_options)


base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

######################################Functions##########################################################

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0

def commercial_extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = "Commercial"

    try:
        list_price = soup.find('div',class_='listing-result__price txt-heading-2')
        list_priceV = list_price.text.strip()
        list_priceV = list_priceV.replace('\xa0', ' ')
    except KeyError:
        list_priceV = None
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    try:
        size = None
        features = soup.find_all('span', class_='listing-result__feature')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#erf-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
            elif '#property-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
    except KeyError:
        size = None

    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)

    try:
        street_address = json_data['address']['streetAddress']
    except KeyError:
        street_address = None
    try:
        address_locality = json_data['address']['addressLocality']
    except KeyError:
        address_locality = None
    try:
        address_region = json_data['address']['addressRegion']
    except KeyError:
        address_region = None

    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        url = None
        prop_ID = None

    bedroom = None
    bathroom = None
    garages = None

    for prop in json_data.get('additionalProperty', []):
        if prop['name'] == 'Bedrooms':
            bedroom = prop['value']
        elif prop['name'] == 'Bathrooms':
            bathroom = prop['value']
        elif prop['name'] == 'Garages':
            garages = prop['value']
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
        "Bedrooms": bedroom, "Bathrooms": bathroom,
        "Floor Size": size, "Garages": garages,
        "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

def indust_extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = "Industrial"

    try:
        list_price = soup.find('div',class_='listing-result__price txt-heading-2')
        list_priceV = list_price.text.strip()
        list_priceV = list_priceV.replace('\xa0', ' ')
    except KeyError:
        list_priceV = None
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    try:
        size = None
        features = soup.find_all('span', class_='listing-result__feature')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#erf-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
            elif '#property-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
    except KeyError:
        size = None

    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)

    try:
        street_address = json_data['address']['streetAddress']
    except KeyError:
        street_address = None
    try:
        address_locality = json_data['address']['addressLocality']
    except KeyError:
        address_locality = None
    try:
        address_region = json_data['address']['addressRegion']
    except KeyError:
        address_region = None

    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        url = None
        prop_ID = None

    bedroom = None
    bathroom = None
    garages = None

    for prop in json_data.get('additionalProperty', []):
        if prop['name'] == 'Bedrooms':
            bedroom = prop['value']
        elif prop['name'] == 'Bathrooms':
            bathroom = prop['value']
        elif prop['name'] == 'Garages':
            garages = prop['value']
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
        "Bedrooms": bedroom, "Bathrooms": bathroom,
        "Floor Size": size, "Garages": garages,
        "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

def retail_extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = "Retail"

    try:
        list_price = soup.find('div',class_='listing-result__price txt-heading-2')
        list_priceV = list_price.text.strip()
        list_priceV = list_priceV.replace('\xa0', ' ')
    except KeyError:
        list_priceV = None
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    try:
        size = None
        features = soup.find_all('span', class_='listing-result__feature')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#erf-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
            elif '#property-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
    except KeyError:
        size = None

    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)

    try:
        street_address = json_data['address']['streetAddress']
    except KeyError:
        street_address = None
    try:
        address_locality = json_data['address']['addressLocality']
    except KeyError:
        address_locality = None
    try:
        address_region = json_data['address']['addressRegion']
    except KeyError:
        address_region = None

    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        url = None
        prop_ID = None

    bedroom = None
    bathroom = None
    garages = None

    for prop in json_data.get('additionalProperty', []):
        if prop['name'] == 'Bedrooms':
            bedroom = prop['value']
        elif prop['name'] == 'Bathrooms':
            bathroom = prop['value']
        elif prop['name'] == 'Garages':
            garages = prop['value']
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
        "Bedrooms": bedroom, "Bathrooms": bathroom,
        "Floor Size": size, "Garages": garages,
        "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

def office_extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = "Office"

    try:
        list_price = soup.find('div',class_='listing-result__price txt-heading-2')
        list_priceV = list_price.text.strip()
        list_priceV = list_priceV.replace('\xa0', ' ')
    except KeyError:
        list_priceV = None
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    try:
        size = None
        features = soup.find_all('span', class_='listing-result__feature')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#erf-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
            elif '#property-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
    except KeyError:
        size = None

    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)

    try:
        street_address = json_data['address']['streetAddress']
    except KeyError:
        street_address = None
    try:
        address_locality = json_data['address']['addressLocality']
    except KeyError:
        address_locality = None
    try:
        address_region = json_data['address']['addressRegion']
    except KeyError:
        address_region = None

    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        url = None
        prop_ID = None

    bedroom = None
    bathroom = None
    garages = None

    for prop in json_data.get('additionalProperty', []):
        if prop['name'] == 'Bedrooms':
            bedroom = prop['value']
        elif prop['name'] == 'Bathrooms':
            bathroom = prop['value']
        elif prop['name'] == 'Garages':
            garages = prop['value']
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
        "Bedrooms": bedroom, "Bathrooms": bathroom,
        "Floor Size": size, "Garages": garages,
        "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

def hospit_extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = "Hospitality"

    try:
        list_price = soup.find('div',class_='listing-result__price txt-heading-2')
        list_priceV = list_price.text.strip()
        list_priceV = list_priceV.replace('\xa0', ' ')
    except KeyError:
        list_priceV = None
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    try:
        size = None
        features = soup.find_all('span', class_='listing-result__feature')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#erf-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
            elif '#property-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
    except KeyError:
        size = None

    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)

    try:
        street_address = json_data['address']['streetAddress']
    except KeyError:
        street_address = None
    try:
        address_locality = json_data['address']['addressLocality']
    except KeyError:
        address_locality = None
    try:
        address_region = json_data['address']['addressRegion']
    except KeyError:
        address_region = None

    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        url = None
        prop_ID = None

    bedroom = None
    bathroom = None
    garages = None

    for prop in json_data.get('additionalProperty', []):
        if prop['name'] == 'Bedrooms':
            bedroom = prop['value']
        elif prop['name'] == 'Bathrooms':
            bathroom = prop['value']
        elif prop['name'] == 'Garages':
            garages = prop['value']
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
        "Bedrooms": bedroom, "Bathrooms": bathroom,
        "Floor Size": size, "Garages": garages,
        "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

def plot_extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = "Vacant Land / Plot"

    try:
        list_price = soup.find('div',class_='listing-result__price txt-heading-2')
        list_priceV = list_price.text.strip()
        list_priceV = list_priceV.replace('\xa0', ' ')
    except KeyError:
        list_priceV = None
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    try:
        size = None
        features = soup.find_all('span', class_='listing-result__feature')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#erf-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
            elif '#property-size' in icon:
                size = feature.text.strip()
                size = size.replace('\xa0', ' ')
    except KeyError:
        size = None

    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)

    try:
        street_address = json_data['address']['streetAddress']
    except KeyError:
        street_address = None
    try:
        address_locality = json_data['address']['addressLocality']
    except KeyError:
        address_locality = None
    try:
        address_region = json_data['address']['addressRegion']
    except KeyError:
        address_region = None

    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        url = None
        prop_ID = None

    bedroom = None
    bathroom = None
    garages = None

    for prop in json_data.get('additionalProperty', []):
        if prop['name'] == 'Bedrooms':
            bedroom = prop['value']
        elif prop['name'] == 'Bathrooms':
            bathroom = prop['value']
        elif prop['name'] == 'Garages':
            garages = prop['value']
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
        "Bedrooms": bedroom, "Bathrooms": bathroom,
        "Floor Size": size, "Garages": garages,
        "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}


######################################Functions##########################################################

async def main():
    fieldnames = ['Listing ID', 'Title', 'Property Type', 'Price', 'Street', 'Region', 'Locality','Bedrooms', 'Bathrooms', 'Floor Size', 'Garages', 'URL',
                  'Agent Name', 'Agent Url', 'Time_stamp']
    # filename = "PrivatePropCom.csv"
    gz_filename = "PrivatePropCom.csv.gz"

    async with aiohttp.ClientSession() as session:
        # with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
        #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #     writer.writeheader()
        #     start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with gzip.open(gz_filename, "wt", newline="", encoding="utf-8") as gzfile:
            writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            async def process_province(prov,p_num):
                new_links = []
                link = f"{base_url}/commercial-sales/{prov}/{p_num}"
                new_links.append(link)

                # response_text = await fetch(session, f"{base_url}/commercial-sales/gauteng/{prov}")
                # home_page = BeautifulSoup(response_text, 'html.parser')

                # links = []
                # ul = home_page.find('ul', class_='region-content-holder__unordered-list')
                # li_items = ul.find_all('li')
                # for area in li_items:
                #     link = area.find('a')
                #     link = f"{base_url}{link.get('href')}"
                #     links.append(link)

                # new_links = []
                # for l in links:
                #     try:
                #         res_in_text = await fetch(session, f"{l}")
                #         inner = BeautifulSoup(res_in_text, 'html.parser')
                #         ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
                #         if ul2:
                #             li_items2 = ul2.find_all('li', class_='region-content-holder__list')
                #             for area2 in li_items2:
                #                 link2 = area2.find('a')
                #                 link2 = f"{base_url}{link2.get('href')}"
                #                 new_links.append(link2)
                #         else:
                #             new_links.append(l)
                #     except aiohttp.ClientError as e:
                #         print(f"Request failed for {l}: {e}")

                async def process_link0(x):
                    try:
                        x = f"{x}?pt=0"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}&page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = commercial_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                async def process_link6(x):
                    try:
                        x = f"{x}?pt=6"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}&page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = indust_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                async def process_link9(x):
                    try:
                        x = f"{x}?pt=9"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}&page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = retail_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                async def process_link8(x):
                    try:
                        x = f"{x}?pt=8"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}&page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = office_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                async def process_link4(x):
                    try:
                        x = f"{x}?pt=4"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}&page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = hospit_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                async def process_link7(x):
                    try:
                        x = f"{x}?pt=7"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}&page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = plot_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                tasks = []
                for x in new_links:
                    tasks.extend([process_link0(x), process_link6(x), process_link9(x), process_link8(x), process_link4(x), process_link7(x)])

                await asyncio.gather(*tasks)
            provinces = {
                    'kwazulu-natal': '2',
                    'gauteng': '3',
                    'western-cape': '4',
                    'northern-cape': '5',
                    'free-state': '6',
                    'eastern-cape': '7',
                    'Limpopo': '8',
                    'north-west': '9',
                    'mpumalanga': '10'
                }
            await asyncio.gather(*(process_province(prov,p_num) for prov,p_num in provinces.items()))
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")


        # Upload the CSV file to Azure Blob Storage
        connection_string = f"{con_str}"
        container_name = "privateprop"
        blob_name = "PrivatePropCom.csv.gz"

        blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)

        with open(gz_filename, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"Outside File uploaded to Azure Blob Storage: {blob_name}")





# Running the main
asyncio.run(main())


##############################################################################################################################

async def fetch2(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return await response.text()

######################################Functions##########################################################
def getPages2(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0

def getIds(soup):
    try:
        # script_data = soup.find('script', type='application/ld+json').string
        # json_data = json.loads(script_data)
        # url = json_data['url']
        url = soup['href']

        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            return prop_ID_match.group(1)
    except Exception as e:
        print(f"Error extracting ID from {soup}: {e}")
    return None

def extractor(soup, url): # extracts from created urls    prop_ID = erfSize = floor_size = rates = levy = None
    beds = baths = lounge = dining = garage = parking = storeys = None
    agent_name = agent_url = price = street = locality = province = None

    try:
        prop_div = soup.find('div', class_='property-details')

        price = soup.find('div', class_='listing-price-display__price').text.strip()
        price = price.replace('\xa0', ' ')

        street = soup.find('div', class_ ='listing-details__address').text.strip()

        lists = prop_div.find('ul', class_='property-details__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            value = feature.find('span', class_='property-details__value').text.strip()
            if '#listing-alt' in icon:
                prop_ID = value
            elif '#property-type' in icon:
                prop_type = value
            elif '#erf-size' in icon:
                erfSize = value.replace('\xa0', ' ')
            elif '#property-size' in icon:
                floor_size = value.replace('\xa0', ' ')
            elif '#rates' in icon:
                rates = value.replace('\xa0', ' ')
            elif '#levies' in icon:
                levy = value.replace('\xa0', ' ')
    except Exception as e:
        print(f"Error extracting property details for {url}: {e}")

    try:
        prop_feat_div = soup.find('div', id='property-features-list')
        lists_feat = prop_feat_div.find('ul', class_='property-features__list')
        feats = lists_feat.find_all('li')
        for feat in feats:
            feat_icon = feat.find('svg').find('use').get('xlink:href')
            value = feat.find('span', class_='property-features__value').text.strip()
            if '#bedrooms' in feat_icon:
                beds = value
            elif '#bathroom' in feat_icon:
                baths = value
            elif '#lounges' in feat_icon:
                lounge = value
            elif '#dining' in feat_icon:
                dining = value
            elif '#garages' in feat_icon:
                garage = value
            elif '#covered-parkiung' in feat_icon:
                parking = value
            elif '#storeys' in feat_icon:
                storeys = value
    except Exception as e:
        print(f"Error extracting property features list for {url}: {e}")


    try:
        details_div = soup.find('div', class_='listing-details')
        script_data = details_div.find('script', type='application/ld+json').string
        json_data = json.loads(script_data)
        locality = json_data['address']['addressLocality']
        province = json_data['address']['addressRegion']
    except Exception as e:
        print(f"Error extracting property json regions {url}: {e}")
    current_datetime = datetime.now().strftime('%Y-%m-%d')
    
    return {
        "Listing ID": prop_ID, "Price": price, "Street": street, "Locality": locality, "Province": province,
        "Erf Size": erfSize, "Property Type": prop_type, "Floor Size": floor_size,
        "Rates and taxes": rates, "Levies": levy, "Bedrooms": beds, "Bathrooms": baths, "Lounges": lounge,
        "Dining": dining, "Garages": garage, "Covered Parking": parking, "Storeys": storeys, "Agent Name": agent_name,
        "Agent Url": agent_url, "Time_stamp":current_datetime
    }

######################################Functions##########################################################
async def main2():
    fieldnames2 = ['Listing ID', 'Price', 'Street', 'Locality', 'Province',
                   'Erf Size', 'Property Type', 'Floor Size', 'Rates and taxes', 'Levies',
                  'Bedrooms', 'Bathrooms', 'Lounges', 'Dining', 'Garages', 'Covered Parking', 'Storeys',
                  'Agent Name', 'Agent Url', 'Time_stamp']
    # filename2 = "PrivatePropRes(Inside)5.csv"
    gz_filename2 = "PrivatePropRes(Inside)5.csv.gz"
    ids = []
    semaphore2 = asyncio.Semaphore(500)

    async with aiohttp.ClientSession() as session2:
        with gzip.open(gz_filename2, 'a', newline='', encoding='utf-8-sig') as gzfile:
            writer = csv.DictWriter(gzfile, fieldnames=fieldnames2)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def process_province2(prov,p_num):
                # response_text = await fetch2(session2, f"{base_url}/commercial-sales/gauteng/{prov}", semaphore2)
                # home_page = BeautifulSoup(response_text, 'html.parser')

                # links = []
                # ul = home_page.find('ul', class_='region-content-holder__unordered-list')
                # li_items = ul.find_all('li')
                # for area in li_items:
                #     link = area.find('a')
                #     link = f"{base_url}{link.get('href')}"
                #     links.append(link)

                new_links = []
                new_links.append(f"{base_url}/commercial-sales/{prov}/{p_num}")
                # for l in links:
                # try:
                #     res_in_text = await fetch2(session2, f"{base_url}/commercial-sales/gauteng/{prov}", semaphore2)
                #     inner = BeautifulSoup(res_in_text, 'html.parser')
                #     ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
                #     if ul2:
                #         li_items2 = ul2.find_all('li', class_='region-content-holder__list')
                #         for area2 in li_items2:
                #             link2 = area2.find('a')
                #             link2 = f"{base_url}{link2.get('href')}"
                #             new_links.append(link2)
                #     else:
                #         new_links.append(f"{base_url}/commercial-sales/gauteng/{prov}")
                # except aiohttp.ClientError as e:
                #     print(f"Request failed for {base_url}/commercial-sales/gauteng/{prov}: {e}")

                async def process_link(x):
                    try:
                        x_response_text = await fetch2(session2, x, semaphore2)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages2(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch2(session2, f"{x}?page={s}", semaphore2)
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='featured-listing')
                            prop_contain.extend(x_prop.find_all('a', class_='listing-result'))

                            for prop in prop_contain:
                                data = getIds(prop)
                                ids.append(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                tasks = [process_link(x) for x in new_links]
                await asyncio.gather(*tasks)

            async def process_ids():
                count = 0

                async def process_id(list_id):
                    nonlocal count
                    count += 1
                    if count % 1000 == 0:
                        print(f"Processed {count} IDs, sleeping for 20 seconds...")
                        await asyncio.sleep(55)
                    list_url = f"{base_url}/for-sale/something/something/something/{list_id}"
                    try:
                        listing = await fetch2(session2, list_url, semaphore2)
                        list_page = BeautifulSoup(listing, 'html.parser')
                        data = extractor(list_page, list_url)
                        writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing ID {list_id}: {e}")

                tasks = [process_id(list_id) for list_id in ids]
                await asyncio.gather(*tasks)
            provinces = {
                    'kwazulu-natal': '2',
                    'gauteng': '3',
                    'western-cape': '4',
                    'northern-cape': '5',
                    'free-state': '6',
                    'eastern-cape': '7',
                    'Limpopo': '8',
                    'north-west': '9',
                    'mpumalanga': '10'
                }
            await asyncio.gather(*(process_province2(prov,p_num) for prov,p_num in provinces.items()))
            await process_ids()
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")

    connection_string = f"{con_str}"
    container_name = "privateprop"
    blob_name = "PrivatePropRes(Inside)5.csv.gz"

    blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)

    with open(gz_filename2, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"Inside File uploaded to Azure Blob Storage: {blob_name}")

# Running the main coroutine
asyncio.run(main2())
