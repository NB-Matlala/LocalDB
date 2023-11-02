import csv
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import random
import time

session = HTMLSession()
filename = 'Series_Data.csv'
web_base = "https://web.soap-2day.to"

fieldnames = ['Title', 'Category', 'Release Date', 'Genre', 'Country', 'Casts', 'IMDB Rating', 'Link']


def get_ser_last_page():
    # Get Series last page
    respon = session.get('https://web.soap-2day.to/tv-shows')
    home = BeautifulSoup(respon.content, 'html.parser')
    page_div = home.find('div', class_='pre-pagination mt-5 mb-5')
    total_pages = page_div.find_all('a', class_='swchItem')
    last_page = int(total_pages[-2].text)
    return last_page

start_page = 1
ser_last = get_ser_last_page()

with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()  
    for ser_page in range(start_page, ser_last + 1):
        # Check if 10 pages have been scraped
        if ser_page % 25 == 0:
            # Generate a random sleep duration between 120 and 200 seconds
            sleep_duration = random.randint(50, 89)

            # Sleep for the random duration
            time.sleep(sleep_duration)

        ser_response = session.get(f"https://web.soap-2day.to/tv-shows?page{ser_page}")
        ser_home_page = BeautifulSoup(ser_response.content, 'html.parser')
        # Find all the Series listings on the page
        series_containers = ser_home_page.find_all('div', class_='entryBlock')
        ser_count = 0

        # Isolate each Movie Listed
        for series in series_containers:
            ser_count += 1
            series_link = series.find('a', href=True)
            series_link = series_link['href']
            series_title = series.find('a', title=True)
            series_title = series_title['title']

            try:
                ser_link = f"{web_base}{series_link}"
                ser_genres = []

                # Accessing Series Link
                res = session.get(ser_link)
                html_content = BeautifulSoup(res.content, 'html.parser')

                ser_details = html_content.find_all('div', class_='dp-element')

                # Finding specific series details
                ser_rating = ser_details[-2].find('span', itemprop='ratingValue')
                ser_rating = ser_rating.text
                ser_release = ser_details[-6].find('div', itemprop='dateCreated')
                ser_release = ser_release.text.strip()

                # Get all series genre
                ser_genre_list = ser_details[-5].find_all('a', class_='entAllCats')
                for gen in ser_genre_list:
                    ser_genres.append(gen.text)
                ser_category = ser_genres.pop(0)
                ser_genre1 = ", ".join(ser_genres)

                ser_country = ser_details[-4].find('div', class_='dpe-content')
                ser_country = ser_country.text.strip()
                ser_casts = ser_details[-3].find('div', class_='dpe-content')
                ser_casts = ser_casts.text.strip()

                # print(f"\nFound {count}:\nTitle: {series_title}\nSeries Rating = {rating}")
                # print(f'Release = {release}')
                # print(f'Category = {category}')
                # print(f'Genre = {genre1}')
                # print(f'Country = {country}')
                # print(f'Casts = {casts}')

            except Exception as e:
                print(f"Error on page {ser_page}, link {ser_link}:\n{e}")

            writer.writerow({
                'Title': series_title, 'Category': ser_category, 'Release Date': ser_release, 'Genre': ser_genre1, 'Country': ser_country,
                'Casts': ser_casts, 'IMDB Rating': ser_rating, 'Link': ser_link})

            print(f"Movie {ser_count}: Page {ser_page}")

