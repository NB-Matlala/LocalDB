
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
        pt = item.get("prop_type")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extract_function(soup, url, pt)
            if result:
                results.extend(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='sort-and-listing-count').text
        num_pgV = int(re.sub(r'\D', '', num_pg.split('of ')[-1]))
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0

def extractor(soup, url, pt):
    prop_contain = soup.find_all('a', class_='listing-result')
    prop_contain.extend(soup.find_all('a', class_='featured-listing'))
    property_type = None
    
    if pt == '10':
        property_type = 'Townhouse / Cluster'
    elif pt == '5':
        property_type = 'House'
    elif pt == '2':
        property_type = 'Apartment / Flat'
    elif pt == '7':
        property_type = 'Vacant Land / Plot'
    elif pt == '1':
        property_type = 'Farm / Smallholding'

    data = []   
    for x_page in prop_contain:

        try:
            link = x_page['href']

            prop_ID_match = re.search(r'/([^/]+)$', link)
            if prop_ID_match:
                prop_id = prop_ID_match.group(1)

                # url = f"{url}{prop_id}"
        except Exception as e:
            print(f"Error extracting ID from {e}")    
        
        
        try:
            agent_name = None
            agent_url = None
            agent_div = x_page.find('div', class_='listing-result__advertiser txt-small-regular')
            title = x_page.get('title')

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
        except Exception as e:
            agent_name = None
            agent_url = None
            print(f"Something went wrong with getting details: {e}")
        current_datetime = datetime.now().strftime('%Y-%m-%d')
        
        
        data.append({
            "Listing ID": prop_id,
            "Title": title,
            "Property Type": property_type,
            "URL": f"{base_url}{link}",
            "Agent Name": agent_name,
            "Agent Url": agent_url,
            "Time_stamp": current_datetime
        })

    return data


# Initialize thread queue and results list
queue = Queue()
results = []


prop_types = ['10','5','2','7','1']
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


for prov,p_num in provinces.items():  #range(2, 11)
    x_prov = f"{base_url}/for-sale/{prov}/{p_num}"
    
    
    for pt in prop_types:
        x = f"{x_prov}?pt={pt}"
        land = session.get(x)
        land_html = BeautifulSoup(land.content, 'html.parser')
        pgs = getPages(land_html, x)

        for p in range(1, pgs + 1):
            url = f"{x}&page={p}"
            queue.put({"url": url, "extract_function": extractor, "prop_type": pt})

# Start threads
num_threads = 10  
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
# csv_filename = 'PrivatePropRes.csv'
# with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
#     fieldnames = results[0].keys() if results else []
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#     writer.writeheader()
#     for result in results:
#         writer.writerow(result)

# Write to gzip format
gz_filename = "PrivatePropRes.csv.gz"
with gzip.open(gz_filename, "wt", newline="", encoding="utf-8") as gzfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

###
### Upload to Azure Blob Storage
blob_connection_string = f"{con_str}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privateprop",
    blob_name = gz_filename     #csv_filename
)
with open(gz_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")

