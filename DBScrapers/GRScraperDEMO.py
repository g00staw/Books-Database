import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import re


# Funkcja do pobrania i parsowania strony książki na Goodreads (używając Selenium)
def get_book_details(isbn):
    url = f'https://www.goodreads.com/search?q={isbn}'

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)

    # Czekaj na załadowanie strony i zamknięcie pop-upu
    time.sleep(3)

    try:
        # Zamknij pop-up
        close_button = driver.find_element(By.CSS_SELECTOR,
                                           "button.Button--tertiary.Button--medium.Button--rounded[aria-label='Close']")
        close_button.click()
        time.sleep(1)
    except:
        print("Pop-up nie został zamknięty.")

    # Pobierz HTML strony po załadowaniu
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    book_details = {}

    # Pobierz ocenę Goodreads
    try:
        book_details['rating_goodreads'] = soup.select_one("div.RatingStatistics__rating").text.strip()
    except AttributeError:
        book_details['rating_goodreads'] = None

    # Pobierz autorów
    try:
        authors = soup.select("span.ContributorLink__name")
        book_details['authors'] = ', '.join([author.text.strip() for author in authors])
    except AttributeError:
        book_details['authors'] = None

    # Pobierz kategorię
    try:
        categories = soup.select("span.BookPageMetadataSection__genreButton a.Button")
        book_details['category'] = ', '.join([category.text.strip() for category in categories])
    except AttributeError:
        book_details['category'] = None

    # Pobierz liczbę stron
    try:
        book_details['num_pages'] = soup.select_one('p[data-testid="pagesFormat"]').text.strip().split()[0]
    except AttributeError:
        book_details['num_pages'] = None

    # Pobierz datę publikacji i wydawnictwo
    try:
        publication_info = soup.select_one('p[data-testid="publicationInfo"]').text.strip().split("by")
        book_details['publication_date'] = publication_info[0].strip()
        book_details['publisher'] = publication_info[-1].strip() if len(publication_info) > 1 else None
    except AttributeError:
        book_details['publication_date'] = book_details['publisher'] = None

    # Pobierz język
    try:
        book_details['language_code'] = soup.select_one("div.TruncatedContent__text").text.strip()
    except AttributeError:
        book_details['language_code'] = None

    driver.quit()
    return book_details


# Wczytanie pliku CSV
df = pd.read_csv('../databases/bookstest.csv')


# Funkcja do uzupełniania brakujących danych i wypisania wyników na terminalu
def fill_missing_data(row):
    # Upewnij się, że ISBN jest traktowane jako ciąg znaków, w tym początkowe 0
    isbn = str(row['isbn']).zfill(10)  # ISBN jest teraz ciągiem znaków, zachowujemy początkowe zera
    print(f"Pobieranie danych dla ISBN: {isbn}")
    book_details = get_book_details(isbn)

    # Uzupełnianie brakujących danych w kolumnach
    if pd.isna(row['authors']) and book_details['authors']:
        row['authors'] = book_details['authors']
    if pd.isna(row['rating_goodreads']) and book_details['rating_goodreads']:
        row['rating_goodreads'] = book_details['rating_goodreads']
    if pd.isna(row['language_code']) and book_details['language_code']:
        row['language_code'] = book_details['language_code']
    if pd.isna(row['num_pages']) and book_details['num_pages']:
        row['num_pages'] = book_details['num_pages']
    if pd.isna(row['publication_date']) and book_details['publication_date']:
        row['publication_date'] = book_details['publication_date']
    if pd.isna(row['publisher']) and book_details['publisher']:
        row['publisher'] = book_details['publisher']
    if pd.isna(row['category']) and book_details['category']:
        row['category'] = book_details['category']

    # Wypisywanie danych na terminalu
    print(f"ISBN: {row['isbn']}")
    print(f"Title: {row['title']}")
    print(f"Authors: {row['authors']}")
    print(f"Goodreads Rating: {row['rating_goodreads']}")
    print(f"Language: {row['language_code']}")
    print(f"Number of Pages: {row['num_pages']}")
    print(f"Publication Date: {row['publication_date']}")
    print(f"Publisher: {row['publisher']}")
    print(f"Category: {row['category']}")
    print("=" * 50)

    return row


# Uzupełnianie danych w całym DataFrame
df = df.apply(fill_missing_data, axis=1)

# Zapisanie wyników do nowego pliku CSV
df.to_csv('../databases/bookstest_final_updated.csv', index=False)

print("Dane zostały zapisane do pliku 'books_updated.csv'.")
