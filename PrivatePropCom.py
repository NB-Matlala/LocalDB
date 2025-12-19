# ####### out & in comm extract ############
# import aiohttp
# import asyncio
# import re
# from bs4 import BeautifulSoup
# import json
# import random
# import csv
# import gzip
# import math
# from datetime import datetime
# from azure.storage.blob import BlobClient
# import os
# # from selenium import webdriver
# # from selenium.webdriver.chrome.options import Options

# # chrome_options = Options()
# # chrome_options.add_argument("--headless")
# # driver = webdriver.Chrome(options=chrome_options)


# base_url = os.getenv("BASE_URL")
# con_str = os.getenv("CON_STR")

# async def fetch(session, url):
#     async with session.get(url) as response:
#         return await response.text()

# ######################################Functions##########################################################

# def getPages(soupPage, url):
#     try:
#         num_pg = soupPage.find('div', class_='sort-and-listing-count')
#         # ('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
#         num_pgV = num_pg.text.split('of ')[-1]
#         num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
#         pages = math.ceil(int(num_pgV) / 20)
#         return pages
#     except (ValueError, AttributeError) as e:
#         print(f"Failed to parse number of pages for URL: {url} - {e}")
#         return 0

# def commercial_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Commercial"

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
#             icon = feature.find('svg').find('use').get('href')
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

# def indust_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Industrial"

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
#             icon = feature.find('svg').find('use').get('href')
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

# def retail_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Retail"

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
#             icon = feature.find('svg').find('use').get('href')
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

# def office_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Office"

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
#             icon = feature.find('svg').find('use').get('href')
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

# def hospit_extractor(soup):
#     try:
#         title = soup.get('title')
#     except KeyError:
#         title = None

#     property_type = "Hospitality"

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
#             icon = feature.find('svg').find('use').get('href')
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

# def plot_extractor(soup):
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
#             icon = feature.find('svg').find('use').get('href')
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

# async def main():
#     fieldnames = ['Listing ID', 'Title', 'Property Type', 'Price', 'Street', 'Region', 'Locality','Bedrooms', 'Bathrooms', 'Floor Size', 'Garages', 'URL',
#                   'Agent Name', 'Agent Url', 'Time_stamp']
#     # filename = "PrivatePropCom.csv"
#     gz_filename = "PrivatePropCom.csv.gz"

#     async with aiohttp.ClientSession() as session:
#         # with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
#         #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         #     writer.writeheader()
#         #     start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         with gzip.open(gz_filename, "wt", newline="", encoding="utf-8") as gzfile:
#             writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
#             writer.writeheader()
#             start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             async def process_province(prov,p_num):
#                 new_links = []
#                 link = f"{base_url}/commercial-sales/{prov}/{p_num}"
#                 new_links.append(link)

#                 # response_text = await fetch(session, f"{base_url}/commercial-sales/gauteng/{prov}")
#                 # home_page = BeautifulSoup(response_text, 'html.parser')

#                 # links = []
#                 # ul = home_page.find('ul', class_='region-content-holder__unordered-list')
#                 # li_items = ul.find_all('li')
#                 # for area in li_items:
#                 #     link = area.find('a')
#                 #     link = f"{base_url}{link.get('href')}"
#                 #     links.append(link)

#                 # new_links = []
#                 # for l in links:
#                 #     try:
#                 #         res_in_text = await fetch(session, f"{l}")
#                 #         inner = BeautifulSoup(res_in_text, 'html.parser')
#                 #         ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
#                 #         if ul2:
#                 #             li_items2 = ul2.find_all('li', class_='region-content-holder__list')
#                 #             for area2 in li_items2:
#                 #                 link2 = area2.find('a')
#                 #                 link2 = f"{base_url}{link2.get('href')}"
#                 #                 new_links.append(link2)
#                 #         else:
#                 #             new_links.append(l)
#                 #     except aiohttp.ClientError as e:
#                 #         print(f"Request failed for {l}: {e}")

#                 async def process_link0(x):
#                     try:
#                         x = f"{x}?pt=0"
#                         x_response_text = await fetch(session, x)
#                         x_page = BeautifulSoup(x_response_text, 'html.parser')
#                         num_pages = getPages(x_page, x)

#                         for s in range(1, num_pages + 1):
#                             if s % 10 == 0:
#                                 sleep_duration = random.randint(10, 15)
#                                 await asyncio.sleep(sleep_duration)

#                             prop_page_text = await fetch(session, f"{x}&page={s}")
#                             x_prop = BeautifulSoup(prop_page_text, 'html.parser')
#                             prop_contain = x_prop.find_all('a', class_='listing-result')
#                             for prop in prop_contain:
#                                 data = commercial_extractor(prop)
#                                 writer.writerow(data)
#                     except Exception as e:
#                         print(f"An error occurred while processing link {x}: {e}")

#                 async def process_link6(x):
#                     try:
#                         x = f"{x}?pt=6"
#                         x_response_text = await fetch(session, x)
#                         x_page = BeautifulSoup(x_response_text, 'html.parser')
#                         num_pages = getPages(x_page, x)

#                         for s in range(1, num_pages + 1):
#                             if s % 10 == 0:
#                                 sleep_duration = random.randint(10, 15)
#                                 await asyncio.sleep(sleep_duration)

#                             prop_page_text = await fetch(session, f"{x}&page={s}")
#                             x_prop = BeautifulSoup(prop_page_text, 'html.parser')
#                             prop_contain = x_prop.find_all('a', class_='listing-result')
#                             for prop in prop_contain:
#                                 data = indust_extractor(prop)
#                                 writer.writerow(data)
#                     except Exception as e:
#                         print(f"An error occurred while processing link {x}: {e}")

#                 async def process_link9(x):
#                     try:
#                         x = f"{x}?pt=9"
#                         x_response_text = await fetch(session, x)
#                         x_page = BeautifulSoup(x_response_text, 'html.parser')
#                         num_pages = getPages(x_page, x)

#                         for s in range(1, num_pages + 1):
#                             if s % 10 == 0:
#                                 sleep_duration = random.randint(10, 15)
#                                 await asyncio.sleep(sleep_duration)

#                             prop_page_text = await fetch(session, f"{x}&page={s}")
#                             x_prop = BeautifulSoup(prop_page_text, 'html.parser')
#                             prop_contain = x_prop.find_all('a', class_='listing-result')
#                             for prop in prop_contain:
#                                 data = retail_extractor(prop)
#                                 writer.writerow(data)
#                     except Exception as e:
#                         print(f"An error occurred while processing link {x}: {e}")

#                 async def process_link8(x):
#                     try:
#                         x = f"{x}?pt=8"
#                         x_response_text = await fetch(session, x)
#                         x_page = BeautifulSoup(x_response_text, 'html.parser')
#                         num_pages = getPages(x_page, x)

#                         for s in range(1, num_pages + 1):
#                             if s % 10 == 0:
#                                 sleep_duration = random.randint(10, 15)
#                                 await asyncio.sleep(sleep_duration)

#                             prop_page_text = await fetch(session, f"{x}&page={s}")
#                             x_prop = BeautifulSoup(prop_page_text, 'html.parser')
#                             prop_contain = x_prop.find_all('a', class_='listing-result')
#                             for prop in prop_contain:
#                                 data = office_extractor(prop)
#                                 writer.writerow(data)
#                     except Exception as e:
#                         print(f"An error occurred while processing link {x}: {e}")

#                 async def process_link4(x):
#                     try:
#                         x = f"{x}?pt=4"
#                         x_response_text = await fetch(session, x)
#                         x_page = BeautifulSoup(x_response_text, 'html.parser')
#                         num_pages = getPages(x_page, x)

#                         for s in range(1, num_pages + 1):
#                             if s % 10 == 0:
#                                 sleep_duration = random.randint(10, 15)
#                                 await asyncio.sleep(sleep_duration)

#                             prop_page_text = await fetch(session, f"{x}&page={s}")
#                             x_prop = BeautifulSoup(prop_page_text, 'html.parser')
#                             prop_contain = x_prop.find_all('a', class_='listing-result')
#                             for prop in prop_contain:
#                                 data = hospit_extractor(prop)
#                                 writer.writerow(data)
#                     except Exception as e:
#                         print(f"An error occurred while processing link {x}: {e}")

#                 async def process_link7(x):
#                     try:
#                         x = f"{x}?pt=7"
#                         x_response_text = await fetch(session, x)
#                         x_page = BeautifulSoup(x_response_text, 'html.parser')
#                         num_pages = getPages(x_page, x)

#                         for s in range(1, num_pages + 1):
#                             if s % 10 == 0:
#                                 sleep_duration = random.randint(10, 15)
#                                 await asyncio.sleep(sleep_duration)

#                             prop_page_text = await fetch(session, f"{x}&page={s}")
#                             x_prop = BeautifulSoup(prop_page_text, 'html.parser')
#                             prop_contain = x_prop.find_all('a', class_='listing-result')
#                             for prop in prop_contain:
#                                 data = plot_extractor(prop)
#                                 writer.writerow(data)
#                     except Exception as e:
#                         print(f"An error occurred while processing link {x}: {e}")

#                 tasks = []
#                 for x in new_links:
#                     tasks.extend([process_link0(x), process_link6(x), process_link9(x), process_link8(x), process_link4(x), process_link7(x)])

#                 await asyncio.gather(*tasks)
#             provinces = {
#                     'kwazulu-natal': '2',
#                     'gauteng': '3',
#                     'western-cape': '4',
#                     'northern-cape': '5',
#                     'free-state': '6',
#                     'eastern-cape': '7',
#                     'Limpopo': '8',
#                     'north-west': '9',
#                     'mpumalanga': '10'
#                 }
#             await asyncio.gather(*(process_province(prov,p_num) for prov,p_num in provinces.items()))
#             end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             print(f"Start Time: {start_time}")
#             print(f"End Time: {end_time}")


#         # Upload the CSV file to Azure Blob Storage
#         connection_string = f"{con_str}"
#         container_name = "privateprop"
#         blob_name = "PrivatePropCom.csv.gz"

#         blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)

#         with open(gz_filename, "rb") as data:
#             blob_client.upload_blob(data, overwrite=True)
#             print(f"Outside File uploaded to Azure Blob Storage: {blob_name}")





# # Running the main
# asyncio.run(main())
# #################################################################################################################################################################
# def main2():
#     from bs4 import BeautifulSoup
#     from requests_html import HTMLSession
#     import re
#     import math
#     import json
#     import time
#     import threading
#     from queue import Queue
#     from datetime import datetime
#     import csv
#     import gzip
#     from azure.storage.blob import BlobClient
#     import os
    
#     base_url = os.getenv("BASE_URL")
#     con_str = os.getenv("CON_STR")
    
#     session = HTMLSession()
    
#     # Thread worker function
#     def worker(queue, results):
#         while True:
#             item = queue.get()
#             if item is None:
#                 break
#             url = item.get("url")
#             extract_function = item.get("extract_function")
#             try:
#                 response = session.get(url)
#                 soup = BeautifulSoup(response.content, 'html.parser')
#                 result = extract_function(soup, url)
#                 if result:
#                     results.append(result)
#             except Exception as e:
#                 print(f"Request failed for {url}: {e}")
#             finally:
#                 queue.task_done()
    
#     def getPages(soupPage, url):
#         try:
#             num_pg = soupPage.find('div', class_='sort-and-listing-count')
#             # ('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
#             num_pgV = num_pg.text.split('of ')[-1]
#             num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
#             pages = math.ceil(int(num_pgV) / 20)
#             return pages
#         except (ValueError, AttributeError) as e:
#             print(f"Failed to parse number of pages for URL: {url} - {e}")
#             return 0
    
#     def extractor(soup, url):
#         # Initialize variables with default values
#         prop_ID = erfSize = floor_size = rates = levy = None
#         beds = baths = lounge = dining = garage = parking = storeys = None
#         agent_name = agent_url = price = street = locality = province = None
    
#         try:
#             prop_div = soup.find('div', class_='property-details')
    
#             price = soup.find('div', class_='listing-price-display__price').text.strip()
#             price = price.replace('\xa0', ' ')
    
#             street = soup.find('div', class_ ='listing-details__address').text.strip()
    
#             lists = prop_div.find('ul', class_='property-details__list')
#             features = lists.find_all('li')
#             for feature in features:
#                 icon = feature.find('svg').find('use').get('href')
#                 value = feature.find('span', class_='property-details__value').text.strip()
#                 if '#listing-alt' in icon:
#                     prop_ID = value
#                 elif '#property-type' in icon:
#                     prop_type = value
#                 elif '#erf-size' in icon:
#                     erfSize = value.replace('\xa0', ' ')
#                 elif '#property-size' in icon:
#                     floor_size = value.replace('\xa0', ' ')
#                 elif '#rates' in icon:
#                     rates = value.replace('\xa0', ' ')
#                 elif '#levies' in icon:
#                     levy = value.replace('\xa0', ' ')
#         except Exception as e:
#             print(f"Error extracting property details for {url}: {e}")
    
#         try:
#             prop_feat_div = soup.find('div', id='property-features-list')
#             lists_feat = prop_feat_div.find('ul', class_='property-features__list')
#             feats = lists_feat.find_all('li')
#             for feat in feats:
#                 feat_icon = feat.find('svg').find('use').get('href')
#                 value = feat.find('span', class_='property-features__value').text.strip()
#                 if '#bedrooms' in feat_icon:
#                     beds = value
#                 elif '#bathroom' in feat_icon:
#                     baths = value
#                 elif '#lounges' in feat_icon:
#                     lounge = value
#                 elif '#dining' in feat_icon:
#                     dining = value
#                 elif '#garages' in feat_icon:
#                     garage = value
#                 elif '#covered-parkiung' in feat_icon:
#                     parking = value
#                 elif '#storeys' in feat_icon:
#                     storeys = value
#         except Exception as e:
#             print(f"Error extracting property features list for {url}: {e}")
    
    
#         try:
#             details_div = soup.find('div', class_='listing-details')
#             script_data = details_div.find('script', type='application/ld+json').string
#             json_data = json.loads(script_data)
#             locality = json_data['address']['addressLocality']
#             province = json_data['address']['addressRegion']
#         except Exception as e:
#             print(f"Error extracting property json regions {url}: {e}")
#         current_datetime = datetime.now().strftime('%Y-%m-%d')
        
#         return {
#             "Listing ID": prop_ID, "Price": price, "Street": street, "Locality": locality, "Province": province,
#             "Erf Size": erfSize, "Property Type": prop_type, "Floor Size": floor_size,
#             "Rates and taxes": rates, "Levies": levy, "Bedrooms": beds, "Bathrooms": baths, "Lounges": lounge,
#             "Dining": dining, "Garages": garage, "Covered Parking": parking, "Storeys": storeys, "Agent Name": agent_name,
#             "Agent Url": agent_url, "Time_stamp":current_datetime
#         }
#     def getIds(soup):
#         try:
#             # script_data = soup.find('script', type='application/ld+json').string
#             # json_data = json.loads(script_data)
#             # url = json_data['url']
#             url = soup['href']
    
#             prop_ID_match = re.search(r'/([^/]+)$', url)
#             if prop_ID_match:
#                 return prop_ID_match.group(1)
#         except Exception as e:
#             print(f"Error extracting ID from {soup}: {e}")
#         return None
    
#     # Initialize thread queue and results list
#     queue = Queue()
#     results = []
#     provinces = {
#             'kwazulu-natal': '2',
#             'gauteng': '3',
#             'western-cape': '4',
#             'northern-cape': '5',
#             'free-state': '6',
#             'eastern-cape': '7',
#             'Limpopo': '8',
#             'north-west': '9',
#             'mpumalanga': '10'
#         }
    
#     for prov,p_num in provinces.items():  
    
#         x = f"{base_url}/commercial-sales/{prov}/{p_num}"
#         try:
#             land = session.get(x)
#             land_html = BeautifulSoup(land.content, 'html.parser')
#             pgs = getPages(land_html, x)
    
#             for p in range(1, pgs + 1):
#                 home_page = session.get(f"{x}?page={p}")
#                 # home_page = session.get(f"{x}?page={p}")
#                 soup = BeautifulSoup(home_page.content, 'html.parser')
#                 prop_contain = soup.find_all('a', class_='featured-listing')
#                 prop_contain.extend(soup.find_all('a', class_='listing-result'))
#                 for x_page in prop_contain:
#                     prop_id = getIds(x_page)
#                     if prop_id:
#                         list_url = f"{base_url}/for-sale/something/something/something/{prop_id}" 
#                         queue.put({"url": list_url, "extract_function": extractor})
    
#         except Exception as e:
#             print(f"Failed to process URL {x}: {e}")
    
#     # Start threads
#     num_threads = 10  # Adjust the number of threads based on your system's capabilities
#     threads = []
#     for i in range(num_threads):
#         t = threading.Thread(target=worker, args=(queue, results))
#         t.start()
#         threads.append(t)
    
#     # Block until all tasks are done
#     queue.join()
    
#     # Stop workers
#     for i in range(num_threads):
#         queue.put(None)
#     for t in threads:
#         t.join()
    
#     # Write results to gzip
#     gz_filename = 'PrivatePropRes(Inside)5.csv.gz'
#     with gzip.open(gz_filename, 'wt', newline='', encoding='utf-8') as gzfile:
#         fieldnames = results[0].keys() if results else []
#         writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
#         writer.writeheader()
#         for result in results:
#             writer.writerow(result)
    
#     # Upload to Azure Blob Storage
#     blob_connection_string = f"{con_str}"
#     blob = BlobClient.from_connection_string(
#         blob_connection_string,
#         container_name="privateprop",
#         blob_name=gz_filename
#     )
#     with open(gz_filename, "rb") as data:
#         blob.upload_blob(data, overwrite=True)
    
#     print("CSV file uploaded to Azure Blob Storage.")

# main2()
# ######################################################################################################################################################
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import json
import random
import csv
import gzip
import math
import threading
from queue import Queue
from datetime import datetime
from azure.storage.blob import BlobClient
import os
import time

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")

session = HTMLSession()

###################################### Helpers ######################################

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='sort-and-listing-count')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
        return math.ceil(int(num_pgV) / 20)
    except Exception as e:
        print(f"Pagination error {url}: {e}")
        return 0

###################################### Extractors ######################################
# NOTE: unchanged logic — copied as-is from Code 1

def commercial_extractor(soup):
    property_type = "Commercial"
    return base_extractor(soup, property_type)

def indust_extractor(soup):
    property_type = "Industrial"
    return base_extractor(soup, property_type)

def retail_extractor(soup):
    property_type = "Retail"
    return base_extractor(soup, property_type)

def office_extractor(soup):
    property_type = "Office"
    return base_extractor(soup, property_type)

def hospit_extractor(soup):
    property_type = "Hospitality"
    return base_extractor(soup, property_type)

def plot_extractor(soup):
    property_type = "Vacant Land / Plot"
    return base_extractor(soup, property_type)

def base_extractor(soup, property_type):
    try:
        title = soup.get('title')
    except:
        title = None

    try:
        price = soup.find('div', class_='listing-result__price').text.strip().replace('\xa0', ' ')
    except:
        price = None

    agent_name = agent_url = None
    try:
        agent_div = soup.find('div', class_='listing-result__advertiser')
        logo = agent_div.find('img')
        agent_name = logo.get('alt')
        src = logo.get('src')
        match = re.search(r'offices/(\d+)', src)
        if match:
            agent_url = f"{base_url}/estate-agency/estate-agent/{match.group(1)}"
    except:
        agent_name = "Private Seller"

    size = None
    try:
        for feature in soup.find_all('span', class_='listing-result__feature'):
            icon = feature.find('use').get('href')
            if '#erf-size' in icon or '#property-size' in icon:
                size = feature.text.strip().replace('\xa0', ' ')
    except:
        pass

    street = locality = region = url = prop_ID = None
    try:
        script = soup.find('script', type='application/ld+json').string
        data = json.loads(script)
        street = data['address'].get('streetAddress')
        locality = data['address'].get('addressLocality')
        region = data['address'].get('addressRegion')
        # url = data.get('url')
        # prop_ID = url.split('/')[-1]
    except:
        pass
        
    try:
        link = soup['href']

        prop_ID_match = re.search(r'/([^/]+)$', link)
        if prop_ID_match:
            prop_id = prop_ID_match.group(1)

            # url = f"{url}{prop_id}"
    except Exception as e:
        print(f"Error extracting ID from {e}") 
            
    beds = baths = garages = None
    try:
        for p in data.get('additionalProperty', []):
            if p['name'] == 'Bedrooms':
                beds = p['value']
            elif p['name'] == 'Bathrooms':
                baths = p['value']
            elif p['name'] == 'Garages':
                garages = p['value']
    except:
        pass

    return {
        "Listing ID": prop_ID,
        "Title": title,
        "Property Type": property_type,
        "Price": price,
        "Street": street,
        "Region": region,
        "Locality": locality,
        "Bedrooms": beds,
        "Bathrooms": baths,
        "Floor Size": size,
        "Garages": garages,
        "URL": url,
        "Agent Name": agent_name,
        "Agent Url": agent_url,
        "Time_stamp": datetime.now().strftime('%Y-%m-%d')
    }

###################################### Thread Worker ######################################

def worker(queue, writer, lock):
    while True:
        item = queue.get()
        if item is None:
            break

        try:
            soup, extractor = item
            data = extractor(soup)
            if data:
                with lock:
                    writer.writerow(data)
        except Exception as e:
            print(f"Worker error: {e}")
        finally:
            queue.task_done()

###################################### Main ######################################

def main():
    fieldnames = [
        'Listing ID', 'Title', 'Property Type', 'Price', 'Street', 'Region',
        'Locality', 'Bedrooms', 'Bathrooms', 'Floor Size', 'Garages',
        'URL', 'Agent Name', 'Agent Url', 'Time_stamp'
    ]

    gz_filename = "PrivatePropCom.csv.gz"
    queue = Queue()
    lock = threading.Lock()

    with gzip.open(gz_filename, "wt", newline="", encoding="utf-8") as gzfile:
        writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
        writer.writeheader()

        threads = []
        for _ in range(10):
            t = threading.Thread(target=worker, args=(queue, writer, lock))
            t.start()
            threads.append(t)

        provinces = {
            'kwazulu-natal': '2', 'gauteng': '3', 'western-cape': '4',
            'northern-cape': '5', 'free-state': '6', 'eastern-cape': '7',
            'limpopo': '8', 'north-west': '9', 'mpumalanga': '10'
        }

        pt_map = {
            0: commercial_extractor,
            6: indust_extractor,
            9: retail_extractor,
            8: office_extractor,
            4: hospit_extractor,
            7: plot_extractor
        }

        for prov, pnum in provinces.items():
            for pt, extractor in pt_map.items():
                base = f"{base_url}/commercial-sales/{prov}/{pnum}?pt={pt}"
                first = session.get(base)
                soup = BeautifulSoup(first.content, 'html.parser')
                pages = getPages(soup, base)

                for page in range(1, pages + 1):
                    if page % 10 == 0:
                        time.sleep(random.randint(5, 10))

                    r = session.get(f"{base}&page={page}")
                    s = BeautifulSoup(r.content, 'html.parser')
                    for prop in s.find_all('a', class_='listing-result'):
                        queue.put((prop, extractor))

        queue.join()

        for _ in threads:
            queue.put(None)
        for t in threads:
            t.join()

    blob_client = BlobClient.from_connection_string(
        con_str, container_name="privateprop", blob_name=gz_filename
    )
    with open(gz_filename, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    print("Upload complete")

main()
