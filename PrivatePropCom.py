import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import math
from datetime import datetime
from azure.storage.blob import BlobClient

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

######################################Functions##########################################################

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.strip()
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
                    agent_url = f"https://www.privateproperty.co.za/estate-agency/estate-agent/{agent_id}"
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
                    agent_url = f"https://www.privateproperty.co.za/estate-agency/estate-agent/{agent_id}"
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
                    agent_url = f"https://www.privateproperty.co.za/estate-agency/estate-agent/{agent_id}"
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
                    agent_url = f"https://www.privateproperty.co.za/estate-agency/estate-agent/{agent_id}"
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
                    agent_url = f"https://www.privateproperty.co.za/estate-agency/estate-agent/{agent_id}"
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
                    agent_url = f"https://www.privateproperty.co.za/estate-agency/estate-agent/{agent_id}"
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
    filename = "PrivatePropCom.csv"

    async with aiohttp.ClientSession() as session:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def process_province(prov):
                response_text = await fetch(session, f"https://www.privateproperty.co.za/commercial-sales/gauteng/{prov}")
                home_page = BeautifulSoup(response_text, 'html.parser')

                links = []
                ul = home_page.find('ul', class_='region-content-holder__unordered-list')
                li_items = ul.find_all('li')
                for area in li_items:
                    link = area.find('a')
                    link = f"https://www.privateproperty.co.za{link.get('href')}"
                    links.append(link)

                new_links = []
                for l in links:
                    try:
                        res_in_text = await fetch(session, f"{l}")
                        inner = BeautifulSoup(res_in_text, 'html.parser')
                        ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
                        if ul2:
                            li_items2 = ul2.find_all('li', class_='region-content-holder__list')
                            for area2 in li_items2:
                                link2 = area2.find('a')
                                link2 = f"https://www.privateproperty.co.za{link2.get('href')}"
                                new_links.append(link2)
                        else:
                            new_links.append(l)
                    except aiohttp.ClientError as e:
                        print(f"Request failed for {l}: {e}")

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

            await asyncio.gather(*(process_province(prov) for prov in range(2, 11)))
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")


        # Upload the CSV file to Azure Blob Storage
        connection_string = "SharedAccessSignature=sv=2021-10-04&ss=btqf&srt=sco&st=2023-10-17T07%3A39%3A17Z&se=2030-10-18T07%3A39%3A00Z&sp=rwdxftlacup&sig=%2BTFZttmuMZLkl%2Bq%2Bf2t%2FPNBSJkWUzw52PPp1sL9X8Wk%3D;BlobEndpoint=https://stautotrader.blob.core.windows.net/;FileEndpoint=https://stautotrader.file.core.windows.net/;QueueEndpoint=https://stautotrader.queue.core.windows.net/;TableEndpoint=https://stautotrader.table.core.windows.net/;"
        container_name = "privateprop"
        blob_name = "PrivatePropCom.csv"

        blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)
        
        with open(filename, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"File uploaded to Azure Blob Storage: {blob_name}")

            



# Running the main coroutine
asyncio.run(main())
