import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import math
from datetime import datetime

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

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

def extractor(soup):
    try:
        title = soup.get('title')
    except KeyError:
        title = None

    property_type = None
    if 'House' in title:
        property_type = 'House'
    elif 'Townhouse' in title:
        property_type = 'Townhouse / Cluster'
    elif 'Cluster' in title:
        property_type = 'Townhouse / Cluster'
    elif 'Flat' in title:
        property_type = 'Apartment / Flat'
    elif 'Apartment' in title:
        property_type = 'Apartment / Flat'
    elif 'Plot' in title:
        property_type = 'Vacant Land / Plot'
    elif 'Land' in title:
        property_type = 'Vacant Land / Plot'
    elif 'Smallholding' in title:
        property_type = 'Farm / Smallholding'
    elif 'Farm' in title:
        property_type = 'Farm / Smallholding'

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

async def main():
    fieldnames = ['Listing ID', 'Title', 'Property Type', 'Price', 'Street', 'Region', 'Locality','Bedrooms', 'Bathrooms', 'Floor Size', 'Garages', 'URL',
                  'Agent Name', 'Agent Url', 'Time_stamp']
    filename = "CarsExtract2.csv"

    async with aiohttp.ClientSession() as session:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"start Time: {start_time}")

            async def process_province(prov):
                response_text = await fetch(session, f"https://www.privateproperty.co.za/for-sale/mpumalanga/{prov}")
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

                async def process_link(x):
                    try:
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}?page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                await asyncio.gather(*(process_link(x) for x in new_links))

            await asyncio.gather(*(process_province(prov) for prov in range(2, 11)))

            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n\nend Time: {end_time}")

# Running the main coroutine
asyncio.run(main())
