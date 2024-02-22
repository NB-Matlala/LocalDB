from bs4 import BeautifulSoup
from datetime import datetime
import csv
import time
import random
import threading
from requests_html import HTMLSession

session = HTMLSession()
base_url = "https://www.cars.co.za"
filename = 'CarsExtract2.csv'


fieldnames = ['Car_ID', 'Title', 'Region', 'Make', 'Model', 'Variant', 'Suburb', 'Province', 'Price',
              'ExpectedPaymentPerMonth',
              'CarType', 'RegistrationYear', 'Mileage', 'Transmission', 'FuelType', 'PriceRating', 'Dealer',
              'LastUpdated',
              'PreviousOwners', 'ManufacturersColour', 'BodyType', 'ServiceHistory', 'WarrantyRemaining',
              'IntroductionDate',
              'EndDate', 'ServiceIntervalDistance', 'EnginePosition', 'EngineDetail', 'EngineCapacity',
              'CylinderLayoutAndQuantity',
              'FuelTypeEngine', 'FuelCapacity', 'FuelConsumption', 'FuelRange', 'PowerMaximum',
              'TorqueMaximum', 'Acceleration', 'MaximumSpeed', 'CO2Emissions', 'Version', 'DealerUrl', 'Timestamp']

with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for page in range(start_page, last_page + 1):
        if page % 10 == 0:
            # Generate a random sleep duration between 120 and 200 seconds
            sleep_duration = random.randint(30, 45)

            # Sleep for the random duration
            time.sleep(sleep_duration)

        response = session.get(f"https://www.cars.co.za/usedcars/?P={page}")
        home_page = BeautifulSoup(response.content, 'html.parser')
        # Find all the car listings on the page
        divs_with_p_relative = home_page.findAll('div', style='position:relative')
        # print(home_page)
        count = 0
        for div in divs_with_p_relative:
            #       # Find the link to the car listing
            link = div.find('a')
            if link:
                count += 1
                try:
                    found_link = (base_url + link['href'])
                except Exception as e:
                    print(f"Error: {e}")

                import re
                # print(home_page)
                car_id_match = re.search(r'/(\d+)/', found_link)

                if car_id_match:
                    car_id = car_id_match.group(1)

                res = session.get(found_link)
                # res = requests.get(found_link)
                html_content = res.content.decode('utf-8')  # Decode the content to a string
                try:
                    # Define a regex pattern to match emojis
                    emoji_pattern = re.compile(
                        "["
                        "\U0001F600-\U0001F64F"  # Emoticons
                        "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
                        "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
                        "\U0001F700-\U0001F77F"  # Alchemical Symbols
                        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                        "\U0001FA00-\U0001FA6F"  # Chess Symbols
                        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                        "\U00002702-\U000027B0"  # Dingbats
                        "\U000024C2-\U0001F251"
                        "]+",
                        flags=re.UNICODE,
                    )

                    # Remove emojis using the regex pattern
                    cleaned_html = emoji_pattern.sub(r'', html_content)

                    # Parse the cleaned HTML content
                    car_page = BeautifulSoup(cleaned_html, 'html.parser')

                    # Append the parsed HTML content to a list
                    parsed_html_list = [car_page]

                    # Find the script element containing the JSON-like data
                    script_element = parsed_html_list[0].find('script', {'id': '__NEXT_DATA__'})

                    # Extract the JSON-like data as a string
                    json_data = script_element.string
                    # Parse the JSON-like data into a Python dictionary
                    import json
                    data_dict = json.loads(json_data)

                    Car_ID = data_dict['props']['pageProps']['vehicle']['id']
                    title = data_dict['props']['pageProps']['vehicle']['attributes']['title']
                    location = data_dict['props']['pageProps']['vehicle']['attributes']['agent_locality']

                    # Handle the case when location is None
                    if location is None:
                        dealer_info_div = car_page.find('div', class_='mantine-1bxn7ei')

                        if dealer_info_div:
                            # Extract the text from the relevant <p> elements
                            dealership = dealer_info_div.find('p', class_='mb-1 text-bold').text.strip()
                            location_and_province = dealer_info_div.find('p', class_='mb-0').text.strip()

                            # Split the location_and_province into location
                            location = location_and_province.split(',')[0].strip()

                    brand = data_dict['props']['pageProps']['vehicle']['attributes']['make']
                    model = data_dict['props']['pageProps']['vehicle']['attributes']['model']
                    variant = data_dict['props']['pageProps']['vehicle']['attributes']['variant']
                    suburb = None
                    price = data_dict['props']['pageProps']['vehicle']['attributes']['price']
                    expected_payment = None
                    car_type = data_dict['props']['pageProps']['vehicle']['attributes']['new_or_used']
                    registration_year = re.search(r'\d+', title)
                    registration_year = registration_year.group()
                    mileage = data_dict['props']['pageProps']['vehicle']['attributes']['mileage']
                    transmission = data_dict['props']['pageProps']['vehicle']['attributes']['transmission']
                    fuel_type = data_dict['props']['pageProps']['vehicle']['attributes']['fuel_type']
                    price_rating = None
                    dealer = data_dict['props']['pageProps']['vehicle']['attributes']['agent_name']
                    last_updated = data_dict['props']['pageProps']['vehicle']['attributes']['date_time']
                    previous_owners = None
                    manufacturers_colour = data_dict['props']['pageProps']['vehicle']['attributes']['colour']
                    body_type = data_dict['props']['pageProps']['vehicle']['attributes']['body_type']
                    service_history = None
                    warranty_remaining = None
                    introduction_date = data_dict['props']['pageProps']['vehicle']['attributes']['date']
                    end_date = None
                    service_interval_distance = None

                    engine_position = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Engine":
                            for attr in spec['attrs']:
                                if attr['label'] == "Engine Position / Location":
                                    engine_position = attr['value']
                                    break

                    engine_detail = None
                    engine_capacity = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Engine":
                            for attr in spec['attrs']:
                                if attr['label'] == "Engine Size":
                                    engine_capacity = attr['value']
                                    break

                    cylinder_layout = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Engine":
                            for attr in spec['attrs']:
                                if attr['label'] == "Cylinders":
                                    cylinder_layout = attr['value']
                                    break

                    fuel_type_engine = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Engine":
                            for attr in spec['attrs']:
                                if attr['label'] == "Fuel Type":
                                    fuel_type_engine = attr['value']
                                    break

                    fuel_capacity = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Economy":
                            for attr in spec['attrs']:
                                if attr['label'] == "Fuel tank capacity":
                                    fuel_capacity = attr['value']
                                    break

                    fuel_consumption = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Summary":
                            for attr in spec['attrs']:
                                if attr['label'] == "Average Fuel Economy":
                                    fuel_consumption = attr['value']
                                    break

                    fuel_range = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Economy":
                            for attr in spec['attrs']:
                                if attr['label'] == "Fuel range":
                                    fuel_range = attr['value']
                                    break

                    power_max = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Summary":
                            for attr in spec['attrs']:
                                if attr['label'] == "Power Maximum Total":
                                    power_max = attr['value']
                                    break

                    torque_max = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Engine":
                            for attr in spec['attrs']:
                                if attr['label'] == "Torque Max":
                                    torque_max = attr['value']
                                    break

                    acceleration = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Performance":
                            for attr in spec['attrs']:
                                if attr['label'] == "0-100Kph":
                                    acceleration = attr['value']
                                    break

                    maximum_speed = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Performance":
                            for attr in spec['attrs']:
                                if attr['label'] == "Top speed":
                                    maximum_speed = attr['value']
                                    break

                    co2_emissions = None
                    for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                        if spec['title'] == "Economy":
                            for attr in spec['attrs']:
                                if attr['label'] == "Co2":
                                    co2_emissions = attr['value']
                                    break

                    current_datetime = datetime.now().strftime('%Y-%m-%d')
                    province_name = data_dict['props']['pageProps']['vehicle']['attributes']['province']
                    # Define a dictionary to hold the data
                    dealer_link = car_page.find('a', class_='ClickableCard_card__EHFn3')
                    basedealer = "https://www.cars.co.za"
                    if dealer_link:
                        link_dealer = dealer_link
                        DealerUrl = (basedealer + link_dealer['href'])

                    else:
                        DealerUrl = None

                    car_data = {
                        'Car_ID': car_id,
                        'Title': title,
                        'Region': location,
                        'Make': brand,
                        'Model': model,
                        'Variant': variant,
                        'Suburb': suburb,
                        'Province': province_name,
                        'Price': price,
                        'ExpectedPaymentPerMonth': expected_payment,
                        'CarType': car_type,
                        'RegistrationYear': registration_year,
                        'Mileage': mileage,
                        'Transmission': transmission,
                        'FuelType': fuel_type,
                        'PriceRating': price_rating,
                        'Dealer': dealer,
                        'LastUpdated': last_updated,
                        'PreviousOwners': previous_owners,
                        'ManufacturersColour': manufacturers_colour,
                        'BodyType': body_type,
                        'ServiceHistory': service_history,
                        'WarrantyRemaining': warranty_remaining,
                        'IntroductionDate': introduction_date,
                        'EndDate': end_date,
                        'ServiceIntervalDistance': service_interval_distance,
                        'EnginePosition': engine_position,
                        'EngineDetail': engine_detail,
                        'EngineCapacity': engine_capacity,
                        'CylinderLayoutAndQuantity': cylinder_layout,
                        'FuelTypeEngine': fuel_type_engine,
                        'FuelCapacity': fuel_capacity,
                        'FuelConsumption': fuel_consumption,
                        'FuelRange': fuel_range,
                        'PowerMaximum': power_max,
                        'TorqueMaximum': torque_max,
                        'Acceleration': acceleration,
                        'MaximumSpeed': maximum_speed,
                        'CO2Emissions': co2_emissions,
                        'Version': 1,
                        'DealerUrl': DealerUrl,
                        'Timestamp': current_datetime
                    }
                    print(f"Page {page} Car {count}: {car_id}")
                    writer.writerow(
                        {'Car_ID':car_data['Car_ID'], 'Title': car_data['Title'], 'Region' : car_data['Region'],
                             'Make' : car_data['Make'], 'Model' : car_data['Model'], 'Variant' : car_data['Variant'],
                             'Suburb' : car_data['Suburb'], 'Province' : car_data['Province'], 'Price' : car_data['Price'],
                             'ExpectedPaymentPerMonth' : car_data['ExpectedPaymentPerMonth'], 'CarType':car_data['CarType'],
                             'RegistrationYear' : car_data['RegistrationYear'], 'Mileage' : car_data['Mileage'],
                             'Transmission' : car_data['Transmission'], 'FuelType' : car_data['FuelType'],
                             'PriceRating':car_data['PriceRating'], 'Dealer':car_data['Dealer'],
                             'LastUpdated':car_data['LastUpdated'], 'PreviousOwners':car_data['PreviousOwners'],
                             'ManufacturersColour':car_data['ManufacturersColour'], 'BodyType':car_data['BodyType'],
                             'ServiceHistory':car_data['ServiceHistory'], 'WarrantyRemaining':car_data['WarrantyRemaining'],
                             'IntroductionDate':car_data['IntroductionDate'], 'EndDate':car_data['EndDate'],
                             'ServiceIntervalDistance':car_data['ServiceIntervalDistance'],
                             'EnginePosition':car_data['EnginePosition'], 'EngineDetail':car_data['EngineDetail'],
                             'EngineCapacity':car_data['EngineCapacity'],
                             'CylinderLayoutAndQuantity':car_data['CylinderLayoutAndQuantity'],
                             'FuelTypeEngine':car_data['FuelTypeEngine'], 'FuelCapacity':car_data['FuelCapacity'],
                             'FuelConsumption':car_data['FuelConsumption'], 'FuelRange':car_data['FuelRange'],
                             'PowerMaximum':car_data['PowerMaximum'], 'TorqueMaximum':car_data['TorqueMaximum'],
                             'Acceleration':car_data['Acceleration'], 'MaximumSpeed':car_data['MaximumSpeed'],
                             'CO2Emissions':car_data['CO2Emissions'], 'Version':'1', 'DealerUrl':car_data['DealerUrl'],
                             'Timestamp': car_data['Timestamp']})
                except Exception as e:
                    print(f"Final Error: {e}")
				
    
