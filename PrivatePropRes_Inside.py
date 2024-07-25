print("Buy inside code running......................")
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import datetime
import requests
from azure.storage.blob import BlobClient
from datetime import datetime, timedelta
import time

from datetime import datetime 

session = HTMLSession()
base_url = "https://www.property24.com/for-sale/uvongo/margate/kwazulu-natal/6359/{id}"

def extract_property_details(listing_id):
    url = listing_id
    try:
        response = session.get(url)
        if response.status != 200:
            print(f"respomse status:{response.status}")
        if response.status == 200:
            page_content = response.content
            soup = BeautifulSoup(page_content, 'html.parser')
            Timestamp = datetime.now().strftime('%Y-%m-%d')
            # Initialize dictionary to store property details
            property_details = {
                "Price": None,
                "Title": None,
                "Location": None,
                "Bedrooms": None,
                "Bathrooms": None,
                "Parking Spaces": None,
                "Floor Size": None,
                "Garages": None,
                "No Pets Allowed": None,
                "Lifestyle": None,
                "Erf Size": None,
                "Price per m²": None,
                "Pets Allowed": None,
                "Furnished": None,
                "Kitchen": None,
                "Lounge": None,
                "Garage": None,
                "Security": None,
                "Backup Water": None,
                "Agency": None,
                "Agency url": None,
                "Listing Number": None,
                "Type of Property": None,
                "Listing Date": None,
                "Levies": None,
                "Rates and Taxes": None,
                "Timestamp": Timestamp,
                "Description":None
            }

            # Extract main details
            p24_results = soup.find('div', class_='p24_listingContent')

            if p24_results:
                tile_div = p24_results.find('div', class_='p24_listingFeaturesWrapper')
                if tile_div:
                    p24_mBM_divs = tile_div.find_all('div', class_='p24_mBM')
                    if len(p24_mBM_divs) > 1 and len(p24_mBM_divs) == 2:
                        title_div = p24_mBM_divs[1]
                    else:
                        title_div = p24_mBM_divs[2]
                    property_details["Title"] = get_text_or_none(title_div.find('h1'))
                    property_details["Location"] = get_text_or_none(title_div.find_next_sibling('div', class_='p24_mBM'))
                    property_details["Timestamp"] = Timestamp

                # Extract price
                price_span = p24_results.find(class_='p24_price')
                property_details["Price"] = get_text_or_none(price_span)

                # Extract features
                icons_wrapper = p24_results.find('div', class_='p24_iconsWrapper')
                if icons_wrapper:
                    icons_list = icons_wrapper.find('ul', class_='p24_icons')
                    if icons_list:
                        feature_items = icons_list.find_all('li', class_='p24_featureDetails')
                        for item in feature_items:
                            feature_title = item.get('title')
                            feature_value = get_text_or_none(item.find('span'))
                            if feature_title in property_details:
                                property_details[feature_title] = feature_value

                # Extract key features
                key_features_containers = soup.find_all('div', class_='p24_keyFeaturesContainer')
                for container in key_features_containers:
                    listing_features = container.find_all('div', class_='p24_listingFeatures')
                    for feature in listing_features:
                        feature_name = get_text_or_none(feature.find('span', class_='p24_feature'))
                        feature_value = get_text_or_none(feature.find('span', class_='p24_featureAmount'))
                        if feature_name in property_details:
                            property_details[feature_name] = feature_value

                # Extract property overview
                property_overview = p24_results.find('div', class_='p24_listingCard p24_propertyOverview')

                def extract_features(container):
                    features = {}
                    rows = container.find_all('div', class_='row p24_propertyOverviewRow')
                    for row in rows:
                        key = get_text_or_none(row.find('div', class_='p24_propertyOverviewKey'))
                        value_divs = row.find_all('div', class_='p24_info')
                        value = ", ".join([get_text_or_none(v) for v in value_divs])
                        features[key] = value
                    return features

                if property_overview:
                    panels = property_overview.find_all('div', class_='panel')
                    for panel in panels:
                        heading = get_text_or_none(panel.find('div', class_='panel-heading'))
                        panel_body = panel.find('div', class_='panel-body')
                        if panel_body:
                            features = extract_features(panel_body)
                            for feature, value in features.items():
                                if feature in property_details:
                                    property_details[feature] = value

                # Extract agency name
                agency_name_elem = soup.find('a', class_='p24_agencyLogoName')
                property_details["Agency"] = get_text_or_none(agency_name_elem.find('span') if agency_name_elem else None)

                # Extract agency URL
                agency_url_elem = soup.find('a', class_='p24_agencyLogo')
                property_details["Agency_url"] = "https://www.property24.com" + (agency_url_elem['href'] if agency_url_elem else '')

                comment_div = soup.find('div', class_='js_expandedText p24_expandedText hide')
                property_details["Description"] = get_text_or_none(comment_div)



            return property_details
        else:
            #print(f"Failed to retrieve details for listing ID {listing_id}: Status code {response.status}")
            return None
    except Exception as e:
        #print(f"An error occurred while fetching details for listing ID {listing_id}: {str(e)}")
        return None


    # Convert property details list to DataFrame
    print(property_details_list)

