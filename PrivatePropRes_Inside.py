######################################################################################################################################################
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import math
import json
import time
import threading
from queue import Queue
from datetime import datetime
import csv
import gzip
from azure.storage.blob import BlobClient
import os

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")

session = HTMLSession()

# Thread worker function
def worker(queue, results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        extract_function = item.get("extract_function")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extract_function(soup, url)
            if result:
                results.append(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

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

def extractor(soup, url):
    # Initialize variables with default values
    prop_ID = erfSize = floor_size = rates = levy = None
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

# Initialize thread queue and results list
queue = Queue()
results = []
provinces = {
    'northern-cape': '5',
    'free-state': '6',
    'eastern-cape': '7',
    'Limpopo': '8',
    'north-west': '9',
    'mpumalanga': '10'
}

for prov,p_num in provinces.items():  
    # response_text = session.get(f"{base_url}/for-sale/mpumalanga/{prov}")
    # home_page = BeautifulSoup(response_text.content, 'html.parser')
    # links = []
    # ul = home_page.find('ul', class_='region-content-holder__unordered-list')
    # li_items = ul.find_all('li')
    # for area in li_items:
    #     link = area.find('a')
    #     link = f"{base_url}{link.get('href')}"
    #     links.append(link)  
    # new_links = []
    # for l in links:

    x = f"{base_url}/for-sale/{prov}/{p_num}"
    try:
        land = session.get(x)
        land_html = BeautifulSoup(land.content, 'html.parser')
        pgs = getPages(land_html, x)

        for p in range(1, pgs + 1):
            home_page = session.get(f"{x}?page={p}")
            # home_page = session.get(f"{x}?page={p}")
            soup = BeautifulSoup(home_page.content, 'html.parser')
            prop_contain = soup.find_all('a', class_='featured-listing')
            prop_contain.extend(soup.find_all('a', class_='listing-result'))
            for x_page in prop_contain:
                prop_id = getIds(x_page)
                if prop_id:
                    list_url = f"{base_url}/for-sale/something/something/something/{prop_id}" 
                    queue.put({"url": list_url, "extract_function": extractor})

    except Exception as e:
        print(f"Failed to process URL {x}: {e}")

# Start threads
num_threads = 10  # Adjust the number of threads based on your system's capabilities
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# # Write results to CSV
# csv_filename = 'PrivatePropRes(Inside).csv'
# with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
#     fieldnames = results[0].keys() if results else []
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#     writer.writeheader()
#     for result in results:
#         writer.writerow(result)
        
# Write results to gzip
gz_filename = 'PrivatePropRes(Inside).csv.gz'
with gzip.open(csv_filename, 'w', newline='', encoding='utf-8') as gzfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

# Upload to Azure Blob Storage
blob_connection_string = f"{con_str}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privateprop",
    blob_name=gz_filename
)
with open(gz_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")
