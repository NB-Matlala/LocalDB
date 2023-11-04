import csv
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import random
import time

session = HTMLSession()
filename = 'Movie_Data.csv'
web_base = "https://web.soap-2day.to"

fieldnames = ['Title', 'Category', 'Release Date', 'Genre', 'Country', 'Casts', 'IMDB Rating', 'Link']



def get_mov_last_page():
    # Get Movie last page
    respon = session.get('https://web.soap-2day.to/movies')
    home = BeautifulSoup(respon.content, 'html.parser')
    page_div = home.find('div', class_='pre-pagination mt-5 mb-5')
    total_pages = page_div.find_all('a', class_='swchItem')
    last_page = int(total_pages[-2].text)
    return last_page

start_page = 1
mov_last = get_mov_last_page()

with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for page in range(start_page, mov_last + 1):
        # Check if 10 pages have been scraped
        if page % 18 == 0:
            # Generate a random sleep duration between 120 and 200 seconds
            sleep_duration = random.randint(125, 200)

            # Sleep for the random duration
            time.sleep(sleep_duration)

        response = session.get(f"https://web.soap-2day.to/movies?page{page}")
        home_page = BeautifulSoup(response.content, 'html.parser')
        # Find all the Movie listings on the page
        movie_containers = home_page.find_all('div', class_='entryBlock')
        count = 0

        # Isolate each Movie Listed
        for movie in movie_containers:
            count += 1
            movie_link = movie.find('a', href=True)
            movie_link = movie_link['href']
            movie_title = movie.find('a', title=True)
            movie_title = movie_title['title']

            try:
                link = f"{web_base}{movie_link}"
                genres = []
                # details = {}

                # Accessing Movie Link
                res = session.get(link)
                html_content = BeautifulSoup(res.content, 'html.parser')

                details = html_content.find_all('div', class_='dp-element')

                # Finding specific movie details
                if len(details) >= 6:
                    rating = details[6].find('span', itemprop='ratingValue')
                    rating = rating.text
                    release = details[2].find('div', itemprop='dateCreated')
                    release = release.text.strip()
    
                    # Get all movie genre
                    genre_list = details[3].find_all('a', class_='entAllCats')
                    for gen in genre_list:
                        genres.append(gen.text)
                    category = genres.pop(0)
                    genre1 = ", ".join(genres)
    
                    country = details[4].find('div', class_='dpe-content')
                    country = country.text.strip()
                    casts = details[5].find('div', class_='dpe-content')
                    casts = casts.text.strip()

                # rest of your code
                else:
                # Handle the case where there are not enough elements in 'details'
                    print("Not enough elements in 'details'")
                    print(f"Element found: {details}")
                # print(f"\nFound {count}:\nTitle: {movie_title}\nMovie Rating = {rating}")
                # print(f'Release = {release}')
                # print(f'Category = {category}')
                # print(f'Genre = {genre1}')
                # print(f'Country = {country}')
                # print(f'Casts = {casts}')

            except Exception as e:
                print(f"Error on page {page}, link {link}:\n{e}")

            writer.writerow({
                'Title': movie_title, 'Category': category, 'Release Date': release, 'Genre': genre1, 'Country': country,
                'Casts': casts, 'IMDB Rating': rating, 'Link': link})
            
            print(f"Movie {count}: Page {page}")
