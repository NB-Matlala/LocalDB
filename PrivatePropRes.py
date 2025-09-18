# import os
# import re
# import csv
# import math
# import json
# import random
# import threading
# from queue import Queue
# from datetime import datetime
# from bs4 import BeautifulSoup
# from requests_html import HTMLSession
# from azure.storage.blob import BlobClient

# base_url = os.getenv("BASE_URL")
# con_str = os.getenv("CON_STR")
# session = HTMLSession()

# queue_lock = threading.Lock()
# write_lock = threading.Lock()

# filename = "PrivatePropRes.csv"
# fieldnames = ['Listing ID', 'Title', 'Property Type', 'Price', 'Street', 'Region', 'Locality','Bedrooms', 'Bathrooms', 'Floor Size', 'Garages', 'URL', 'Agent Name', 'Agent Url', 'Time_stamp']

# q = Queue()

# # Load and reuse your extractors here: cluster_extractor(), house_extractor(), etc.
# def cluster_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Townhouse / Cluster"

#     try:
#         list_price = soup.find('div',class_='listing-result__price txt-heading-2')
#         list_priceV = list_price.text.strip()
#         list_priceV = list_priceV.replace('\xa0', ' ')
#     except KeyError:
#         list_priceV = None
#     try:
#         agent_name = None
#         agent_url = None
#         agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

#         if agent_div:
#             try:
#                 agent_detail = agent_div.find('img', class_='listing-result__logo')
#                 agent_name = agent_detail.get('alt')
#                 agent_url = agent_detail.get('src')
#                 agent_id_match = re.search(r'offices/(\d+)', agent_url)
#                 if agent_id_match:
#                     agent_id = agent_id_match.group(1)
#                     agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
#             except:
#                 agent_name = "Private Seller"
#                 agent_url = None
#     except KeyError:
#         agent_name = None
#         agent_url = None

#     try:
#         size = None
#         features = soup.find_all('span', class_='listing-result__feature')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#erf-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#             elif '#property-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#     except KeyError:
#         size = None

#     script_data = soup.find('script', type='application/ld+json').string
#     json_data = json.loads(script_data)

#     try:
#         street_address = json_data['address']['streetAddress']
#     except KeyError:
#         street_address = None
#     try:
#         address_locality = json_data['address']['addressLocality']
#     except KeyError:
#         address_locality = None
#     try:
#         address_region = json_data['address']['addressRegion']
#     except KeyError:
#         address_region = None

#     try:
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             prop_ID = prop_ID_match.group(1)
#         else:
#             prop_ID = None
#     except KeyError:
#         url = None
#         prop_ID = None

#     bedroom = None
#     bathroom = None
#     garages = None

#     for prop in json_data.get('additionalProperty', []):
#         if prop['name'] == 'Bedrooms':
#             bedroom = prop['value']
#         elif prop['name'] == 'Bathrooms':
#             bathroom = prop['value']
#         elif prop['name'] == 'Garages':
#             garages = prop['value']
#     current_datetime = datetime.now().strftime('%Y-%m-%d')

#     return {
#         "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
#         "Bedrooms": bedroom, "Bathrooms": bathroom,
#         "Floor Size": size, "Garages": garages,
#         "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

# def house_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "House"

#     try:
#         list_price = soup.find('div',class_='listing-result__price txt-heading-2')
#         list_priceV = list_price.text.strip()
#         list_priceV = list_priceV.replace('\xa0', ' ')
#     except KeyError:
#         list_priceV = None
#     try:
#         agent_name = None
#         agent_url = None
#         agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

#         if agent_div:
#             try:
#                 agent_detail = agent_div.find('img', class_='listing-result__logo')
#                 agent_name = agent_detail.get('alt')
#                 agent_url = agent_detail.get('src')
#                 agent_id_match = re.search(r'offices/(\d+)', agent_url)
#                 if agent_id_match:
#                     agent_id = agent_id_match.group(1)
#                     agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
#             except:
#                 agent_name = "Private Seller"
#                 agent_url = None
#     except KeyError:
#         agent_name = None
#         agent_url = None

#     try:
#         size = None
#         features = soup.find_all('span', class_='listing-result__feature')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#erf-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#             elif '#property-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#     except KeyError:
#         size = None

#     script_data = soup.find('script', type='application/ld+json').string
#     json_data = json.loads(script_data)

#     try:
#         street_address = json_data['address']['streetAddress']
#     except KeyError:
#         street_address = None
#     try:
#         address_locality = json_data['address']['addressLocality']
#     except KeyError:
#         address_locality = None
#     try:
#         address_region = json_data['address']['addressRegion']
#     except KeyError:
#         address_region = None

#     try:
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             prop_ID = prop_ID_match.group(1)
#         else:
#             prop_ID = None
#     except KeyError:
#         url = None
#         prop_ID = None

#     bedroom = None
#     bathroom = None
#     garages = None

#     for prop in json_data.get('additionalProperty', []):
#         if prop['name'] == 'Bedrooms':
#             bedroom = prop['value']
#         elif prop['name'] == 'Bathrooms':
#             bathroom = prop['value']
#         elif prop['name'] == 'Garages':
#             garages = prop['value']
#     current_datetime = datetime.now().strftime('%Y-%m-%d')

#     return {
#         "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
#         "Bedrooms": bedroom, "Bathrooms": bathroom,
#         "Floor Size": size, "Garages": garages,
#         "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

# def apartment_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Apartment / Flat"

#     try:
#         list_price = soup.find('div',class_='listing-result__price txt-heading-2')
#         list_priceV = list_price.text.strip()
#         list_priceV = list_priceV.replace('\xa0', ' ')
#     except KeyError:
#         list_priceV = None
#     try:
#         agent_name = None
#         agent_url = None
#         agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

#         if agent_div:
#             try:
#                 agent_detail = agent_div.find('img', class_='listing-result__logo')
#                 agent_name = agent_detail.get('alt')
#                 agent_url = agent_detail.get('src')
#                 agent_id_match = re.search(r'offices/(\d+)', agent_url)
#                 if agent_id_match:
#                     agent_id = agent_id_match.group(1)
#                     agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
#             except:
#                 agent_name = "Private Seller"
#                 agent_url = None
#     except KeyError:
#         agent_name = None
#         agent_url = None

#     try:
#         size = None
#         features = soup.find_all('span', class_='listing-result__feature')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#erf-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#             elif '#property-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#     except KeyError:
#         size = None

#     script_data = soup.find('script', type='application/ld+json').string
#     json_data = json.loads(script_data)

#     try:
#         street_address = json_data['address']['streetAddress']
#     except KeyError:
#         street_address = None
#     try:
#         address_locality = json_data['address']['addressLocality']
#     except KeyError:
#         address_locality = None
#     try:
#         address_region = json_data['address']['addressRegion']
#     except KeyError:
#         address_region = None

#     try:
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             prop_ID = prop_ID_match.group(1)
#         else:
#             prop_ID = None
#     except KeyError:
#         url = None
#         prop_ID = None

#     bedroom = None
#     bathroom = None
#     garages = None

#     for prop in json_data.get('additionalProperty', []):
#         if prop['name'] == 'Bedrooms':
#             bedroom = prop['value']
#         elif prop['name'] == 'Bathrooms':
#             bathroom = prop['value']
#         elif prop['name'] == 'Garages':
#             garages = prop['value']
#     current_datetime = datetime.now().strftime('%Y-%m-%d')

#     return {
#         "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
#         "Bedrooms": bedroom, "Bathrooms": bathroom,
#         "Floor Size": size, "Garages": garages,
#         "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

# def land_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Vacant Land / Plot"

#     try:
#         list_price = soup.find('div',class_='listing-result__price txt-heading-2')
#         list_priceV = list_price.text.strip()
#         list_priceV = list_priceV.replace('\xa0', ' ')
#     except KeyError:
#         list_priceV = None
#     try:
#         agent_name = None
#         agent_url = None
#         agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

#         if agent_div:
#             try:
#                 agent_detail = agent_div.find('img', class_='listing-result__logo')
#                 agent_name = agent_detail.get('alt')
#                 agent_url = agent_detail.get('src')
#                 agent_id_match = re.search(r'offices/(\d+)', agent_url)
#                 if agent_id_match:
#                     agent_id = agent_id_match.group(1)
#                     agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
#             except:
#                 agent_name = "Private Seller"
#                 agent_url = None
#     except KeyError:
#         agent_name = None
#         agent_url = None

#     try:
#         size = None
#         features = soup.find_all('span', class_='listing-result__feature')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#erf-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#             elif '#property-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#     except KeyError:
#         size = None

#     script_data = soup.find('script', type='application/ld+json').string
#     json_data = json.loads(script_data)

#     try:
#         street_address = json_data['address']['streetAddress']
#     except KeyError:
#         street_address = None
#     try:
#         address_locality = json_data['address']['addressLocality']
#     except KeyError:
#         address_locality = None
#     try:
#         address_region = json_data['address']['addressRegion']
#     except KeyError:
#         address_region = None

#     try:
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             prop_ID = prop_ID_match.group(1)
#         else:
#             prop_ID = None
#     except KeyError:
#         url = None
#         prop_ID = None

#     bedroom = None
#     bathroom = None
#     garages = None

#     for prop in json_data.get('additionalProperty', []):
#         if prop['name'] == 'Bedrooms':
#             bedroom = prop['value']
#         elif prop['name'] == 'Bathrooms':
#             bathroom = prop['value']
#         elif prop['name'] == 'Garages':
#             garages = prop['value']
#     current_datetime = datetime.now().strftime('%Y-%m-%d')

#     return {
#         "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
#         "Bedrooms": bedroom, "Bathrooms": bathroom,
#         "Floor Size": size, "Garages": garages,
#         "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}

# def farm_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Farm / Smallholding"

#     try:
#         list_price = soup.find('div',class_='listing-result__price txt-heading-2')
#         list_priceV = list_price.text.strip()
#         list_priceV = list_priceV.replace('\xa0', ' ')
#     except KeyError:
#         list_priceV = None
#     try:
#         agent_name = None
#         agent_url = None
#         agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

#         if agent_div:
#             try:
#                 agent_detail = agent_div.find('img', class_='listing-result__logo')
#                 agent_name = agent_detail.get('alt')
#                 agent_url = agent_detail.get('src')
#                 agent_id_match = re.search(r'offices/(\d+)', agent_url)
#                 if agent_id_match:
#                     agent_id = agent_id_match.group(1)
#                     agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
#             except:
#                 agent_name = "Private Seller"
#                 agent_url = None
#     except KeyError:
#         agent_name = None
#         agent_url = None

#     try:
#         size = None
#         features = soup.find_all('span', class_='listing-result__feature')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#erf-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#             elif '#property-size' in icon:
#                 size = feature.text.strip()
#                 size = size.replace('\xa0', ' ')
#     except KeyError:
#         size = None

#     script_data = soup.find('script', type='application/ld+json').string
#     json_data = json.loads(script_data)

#     try:
#         street_address = json_data['address']['streetAddress']
#     except KeyError:
#         street_address = None
#     try:
#         address_locality = json_data['address']['addressLocality']
#     except KeyError:
#         address_locality = None
#     try:
#         address_region = json_data['address']['addressRegion']
#     except KeyError:
#         address_region = None

#     try:
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             prop_ID = prop_ID_match.group(1)
#         else:
#             prop_ID = None
#     except KeyError:
#         url = None
#         prop_ID = None

#     bedroom = None
#     bathroom = None
#     garages = None

#     for prop in json_data.get('additionalProperty', []):
#         if prop['name'] == 'Bedrooms':
#             bedroom = prop['value']
#         elif prop['name'] == 'Bathrooms':
#             bathroom = prop['value']
#         elif prop['name'] == 'Garages':
#             garages = prop['value']
#     current_datetime = datetime.now().strftime('%Y-%m-%d')

#     return {
#         "Listing ID": prop_ID, "Title": title, "Property Type": property_type, "Price": list_priceV,"Street": street_address,  "Region": address_region, "Locality": address_locality,
#         "Bedrooms": bedroom, "Bathrooms": bathroom,
#         "Floor Size": size, "Garages": garages,
#         "URL": url, "Agent Name": agent_name, "Agent Url": agent_url,"Time_stamp":current_datetime}


# ######################################Functions##########################################################



# def getPages(soupPage, url):
#     try:
#         num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
#         num_pgV = num_pg.text.split('of ')[-1].replace('\xa0', '').replace(' results', '')
#         return math.ceil(int(num_pgV) / 20)
#     except Exception as e:
#         print(f"[Page Error] {url} - {e}")
#         return 0

# def process_link(link, prop_type):
#     extractor_map = {
#         10: cluster_extractor,
#         5: house_extractor,
#         2: apartment_extractor,
#         7: land_extractor,
#         1: farm_extractor,
#     }

#     try:
#         url = f"{link}?pt={prop_type}"
#         r = session.get(url)
#         soup = BeautifulSoup(r.content, "html.parser")
#         pages = getPages(soup, url)

#         import time
#         for s in range(1, pages + 1):
#             if s % 25 == 0:
#                 sleep_time = random.randint(10, 20)
#                 print(f"[Sleeping] {sleep_time} seconds...")
#                 time.sleep(sleep_time)

#             page_url = f"{url}&page={s}"
#             res = session.get(page_url)
#             x_prop = BeautifulSoup(res.content, 'html.parser')
#             listings = x_prop.find_all('a', class_='featured-listing')
#             listings.extend(x_prop.find_all('a', class_='listing-result'))

#             extractor = extractor_map[prop_type]

#             with write_lock:
#                 with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
#                     writer = csv.DictWriter(f, fieldnames=fieldnames)
#                     for prop in listings:
#                         data = extractor(prop)
#                         writer.writerow(data)
#     except Exception as e:
#         print(f"[Error processing {link} pt={prop_type}] {e}")

# def worker():
#     while not q.empty():
#         try:
#             link, pt = q.get()
#             process_link(link, pt)
#             q.task_done()
#         except Exception as e:
#             print(f"[Worker Error] {e}")
#             q.task_done()

# def main():
#     start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     print(f"[Start] {start_time}")

#     # Write CSV headers
#     with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()

#     for x in range(2, 11):
#         link = f"{base_url}/for-sale/mpumalanga/{x}"
#         for pt in [10, 5, 2, 1, 7]:
#             q.put((link, pt))

#     threads = []
#     for _ in range(20):  # 20 worker threads
#         t = threading.Thread(target=worker)
#         t.start()
#         threads.append(t)

#     for t in threads:
#         t.join()

#     end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     print(f"[Done] Start: {start_time} | End: {end_time}")

#     # Upload to Azure Blob Storage
#     blob_client = BlobClient.from_connection_string(con_str, "privateprop", filename)
#     with open(filename, "rb") as data:
#         blob_client.upload_blob(data, overwrite=True)
#     print(f"[Upload Complete] {filename} uploaded to Azure Blob.")

# if __name__ == "__main__":
#     main()
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
        num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
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
with open(csv_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")

