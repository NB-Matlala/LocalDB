import requests
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import math
import threading
from threading import Lock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobClient

def fetch(url, session, semaphore):
    with semaphore:
        response = session.get(url)
        return response.text

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

def getIds(soup):
    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)
    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        prop_ID = None

    return prop_ID

def extractor(soup, url): # extracts from created urls
    try:
        prop_ID = None
        erfSize = None
        floor_size = None
        rates = None
        levy = None

        prop_div = soup.find('div', class_='property-features')
        lists = prop_div.find('ul', class_='property-features__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span',class_='property-features__value').text.strip()

            elif '#property-type' in icon:
                prop_type = feature.find('span',class_='property-features__value').text.strip()

            elif '#erf-size' in icon:
                erfSize = feature.find('span',class_='property-features__value').text.strip()
                erfSize = erfSize.replace('\xa0', ' ')

            elif '#property-size' in icon:
                floor_size = feature.find('span',class_='property-features__value').text.strip()
                floor_size = floor_size.replace('\xa0', ' ')

            elif '#rates' in icon:
                rates = feature.find('span',class_='property-features__value').text.strip()
                rates = rates.replace('\xa0', ' ')

            elif '#levies' in icon:
                levy = feature.find('span',class_='property-features__value').text.strip()
                levy = levy.replace('\xa0', ' ')

    except KeyError:
        prop_ID = None
        erfSize = None
        prop_type = None
        floor_size = None
        rates = None
        levy = None

    beds = None
    baths = None
    lounge = None
    dining = None
    garage = None
    parking = None
    storeys = None

    try:
        prop_feat_div = soup.find('div', id='property-features-list')
        lists_feat = prop_feat_div.find('ul', class_='property-features__list')
        feats = lists_feat.find_all('li')
        for feat in feats:
            feat_icon = feat.find('svg').find('use').get('xlink:href')
            if '#bedrooms' in feat_icon:
                beds = feat.find('span',class_='property-features__value').text.strip()
            elif '#bathroom' in feat_icon:
                baths = feat.find('span',class_='property-features__value').text.strip()
            elif '#lounges' in feat_icon:
                lounge = feat.find('span',class_='property-features__value').text.strip()
            elif '#dining' in feat_icon:
                dining = feat.find('span',class_='property-features__value').text.strip()
            elif '#garages' in feat_icon:
                garage = feat.find('span',class_='property-features__value').text.strip()
            elif '#covered-parkiung' in feat_icon:
                parking = feat.find('span',class_='property-features__value').text.strip()
            elif '#storeys' in feat_icon:
                storeys = feat.find('span',class_='property-features__value').text.strip()

    except (AttributeError, KeyError) as f:
        print(f"Property Features Not Found: for {url}")
        beds = None
        baths = None
        lounge = None
        dining = None
        garage = None
        parking = None
        storeys = None

    agent_name = None
    agent_url = None

    try:
        script_tag = soup.find('script', string=re.compile(r'const serverVariables'))
        if script_tag:
            script_content = script_tag.string
            script_data2 = re.search(r'const serverVariables\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
            json_data = json.loads(script_data2)
            try:
                agent_name = json_data['bundleParams']['agencyInfo']['agencyName']
                agent_url = json_data['bundleParams']['agencyInfo']['agencyPageUrl']
                agent_url = f"https://www.privateproperty.co.za{agent_url}"
            except :
                agent_name = "Private Seller"
                agent_url = None
    except (AttributeError, KeyError) as e:
        agent_name = None
        agent_url = None

    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Erf Size": erfSize, "Property Type": prop_type, "Floor Size": floor_size,
        "Rates and taxes": rates, "Levies": levy, "Bedrooms": beds, "Bathrooms": baths, "Lounges": lounge,
        "Dining": dining, "Garages": garage, "Covered Parking": parking, "Storeys": storeys, "Agent Name": agent_name,
        "Agent Url": agent_url, "Time_stamp": current_datetime}

######################################Functions##########################################################
def main():
    fieldnames = ['Listing ID', 'Erf Size', 'Property Type', 'Floor Size', 'Rates and taxes', 'Levies',
                  'Bedrooms', 'Bathrooms', 'Lounges', 'Dining', 'Garages', 'Covered Parking', 'Storeys',
                  'Agent Name', 'Agent Url', 'Time_stamp']
    filename = "PrivatePropRes(Inside)2.csv"
    ids = []
    ids_lock = Lock()  # Protect shared resource
    semaphore = threading.Semaphore(700)
    
    with requests.Session() as session:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            def process_province(prov):
                response_text = fetch(f"https://www.privateproperty.co.za/for-sale/mpumalanga/{prov}", session, semaphore)
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
                        res_in_text = fetch(f"{l}", session, semaphore)
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
                    except requests.RequestException as e:
                        print(f"Request failed for {l}: {e}")

                def process_link(x):
                    try:
                        x_response_text = fetch(x, session, semaphore)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                time.sleep(sleep_duration)

                            prop_page_text = fetch(f"{x}?page={s}", session, semaphore)
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('div', class_='listingResult')
                            for prop in prop_contain:
                                prop_link = prop.find('a', class_='listingResult').get('href')
                                link = f"https://www.privateproperty.co.za{prop_link}"
                                with ids_lock:
                                    ids.append(link)
                    except requests.RequestException as e:
                        print(f"Request failed for {x}: {e}")

                with ThreadPoolExecutor(max_workers=15) as executor:
                    executor.map(process_link, new_links)

            process_province('2')

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for url in ids:
                    futures.append(executor.submit(fetch, url, session, semaphore))

                for future in as_completed(futures):
                    try:
                        response_text = future.result()
                        soup = BeautifulSoup(response_text, 'html.parser')
                        json_data = extractor(soup, url)  # Explicitly pass the URL
                        writer.writerow(json_data)
                    except Exception as e:
                        print(f"Request failed for {url}: {e}")

            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")

            # Upload to Azure Blob Storage
            blob = BlobClient.from_connection_string(
                conn_str="DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;",
                container_name="privateprop",
                blob_name=filename)

            with open(filename, "rb") as data:
                blob.upload_blob(data, overwrite=True)

if __name__ == "__main__":
    main()
