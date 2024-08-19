from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import math
import json
import random
import csv
import time
from datetime import datetime


session = HTMLSession()


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
    try:
        prop_ID = None
        erfSize = None
        floor_size = None
        rates = None
        levy = None

        prop_div = soup.find('div',class_='property-features')
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

    try:
        agent_name = None
        agent_url = None
        script_tag = soup.find('script', string=re.compile(r'const serverVariables'))
        if script_tag:
            script_content = script_tag.string
            script_data2 = re.search(r'const serverVariables\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
            json_data = json.loads(script_data2)
            agent_name = json_data['bundleParams']['agencyInfo']['agencyName']
            agent_url = json_data['bundleParams']['agencyInfo']['agencyPageUrl']
            agent_url = f"https://www.privateproperty.co.za{agent_url}"
    except (AttributeError, KeyError) as e:
        agent_name = None
        agent_url = None

    return {
        "Listing ID": prop_ID, "Erf Size": erfSize, "Property Type": prop_type, "Floor Size": floor_size,
        "Rates and taxes": rates, "Levies": levy, "Bedrooms": beds, "Bathrooms": baths, "Lounges": lounge,
        "Dining": dining, "Garages": garage, "Covered Parking": parking, "Storeys": storeys, "Agent name": agent_name,
        "Agent Url": agent_url}

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


last_page = 3
start_page = 1

ids = []
x = f"https://www.privateproperty.co.za/for-sale/mpumalanga/middelburg/1347"
#########

response_text = session.get(f"https://www.privateproperty.co.za/for-sale/mpumalanga/2")
home_page = BeautifulSoup(response_text.content, 'html.parser')

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
        res_in_text = session.get(f"{l}")
        inner = BeautifulSoup(res_in_text.content, 'html.parser')
        ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
        if ul2:
            li_items2 = ul2.find_all('li', class_='region-content-holder__list')
            for area2 in li_items2:
                link2 = area2.find('a')
                link2 = f"https://www.privateproperty.co.za{link2.get('href')}"
                new_links.append(link2)
        else:
            new_links.append(l)
    except :
        print(f"Request failed for {l}")

#########
# for i in range(1,6):
#     print(new_links[i])
for x in new_links:
    land = session.get(x)
    land_html = BeautifulSoup(land.content, 'html.parser')
    pgs = getPages(land_html,x)
    for p in range(1,pgs+1):
        home_page = session.get(f"{x}?page={p}")
        soup = BeautifulSoup(home_page.content, 'html.parser')
        prop_contain = soup.find_all('a', class_='listing-result')
        for x_page in prop_contain:
            ids.append(getIds(x_page))

    for list_id in ids:
        list_url = f"https://www.privateproperty.co.za/for-sale/something/something/something/{list_id}"
        listing = session.get(list_url)
        list_page = BeautifulSoup(listing.content, 'html.parser')
        print(extractor(list_page,list_url))
