import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup 

# Global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

MAX_THREADS = 10
CSV_FILE = 'movies.csv'

def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = requests.get(movie_link, headers=headers)
    
    if response.status_code == 200:
        movie_soup = BeautifulSoup(response.content, 'html.parser')

        title = None
        date = None
        
        # Finding the specific section
        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
        
        if page_section is not None:
            # Finding all divs within the section
            divs = page_section.find_all('div', recursive=False)
            
            if len(divs) > 1:
                target_div = divs[1]
                
                # Finding the movie title
                title_tag = target_div.find('h1')
                if title_tag:
                    title = title_tag.find('span').get_text()
                
                # Finding the release date
                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()
                
                # Finding the movie rating
                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None
                
                # Finding the movie plot
                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None
                
                # Writing to CSV file
                if all([title, date, rating, plot_text]):
                    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
                        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        movie_writer.writerow([title, date, rating, plot_text])
    else:
        print(f"Failed to retrieve {movie_link}")

def extract_movies(soup):
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)

def main():
    start_time = time.time()

    # Creating a CSV file with header
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        movie_writer.writerow(['Title', 'Release Date', 'Rating', 'Plot'])

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        extract_movies(soup)
    else:
        print("Failed to retrieve the main page")

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

if __name__ == '__main__':
    main()