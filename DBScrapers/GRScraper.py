import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed


# Funkcja do pobierania danych z Goodreads
def get_goodreads_data(isbn):
    print(f"Scraping data for ISBN: {isbn}")  # Logowanie, że rozpoczynamy scraping dla danej książki

    url = f"https://www.goodreads.com/book/show/{isbn}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # Wykonanie zapytania HTTP
    response = requests.get(url, headers=headers)

    # Logowanie statusu zapytania HTTP
    print(f"HTTP status for ISBN {isbn}: {response.status_code}")

    # Jeśli odpowiedź jest OK
    if response.status_code == 200:
        print("RESPONSE 200")
        soup = BeautifulSoup(response.content, "html.parser")

        # Szukamy pierwszego wyniku książki w wyszukiwarce
        book = soup.find("a", class_="bookTitle")
        if book:
            book_url = "https://www.goodreads.com" + book["href"]

            # Wczytanie strony książki
            book_response = requests.get(book_url, headers=headers)
            print(f"Book page HTTP status for ISBN {isbn}: {book_response.status_code}")

            if book_response.status_code == 200:
                book_soup = BeautifulSoup(book_response.content, "html.parser")

                # Pobranie danych
                author = book_soup.find("span", class_="ContributorLink__name").text if book_soup.find("span", class_="ContributorLink__name") else None
                rating = book_soup.find("div", class_="RatingStatistics__rating").text.strip() if book_soup.find("div", class_="RatingStatistics__rating") else None
                language = book_soup.find("div", class_="TruncatedContent__text TruncatedContent__text--small",
                                          tabindex="-1", data_testid="contentContainer").text.strip() if book_soup.find(
                    "div", class_="TruncatedContent__text TruncatedContent__text--small", tabindex="-1",
                    data_testid="contentContainer") else None
                num_pages = \
                book_soup.find("div", class_="TruncatedContent__text TruncatedContent__text--small", tabindex="-1",
                               data_testid="contentContainer").text.strip().split(',')[0] if book_soup.find("div",
                                                                                                            class_="TruncatedContent__text TruncatedContent__text--small",
                                                                                                            tabindex="-1",
                                                                                                            data_testid="contentContainer") else None
                publication_date = book_soup.find("div", class_="TruncatedContent__text TruncatedContent__text--small",
                                                  tabindex="-1",
                                                  data_testid="contentContainer").text.strip() if book_soup.find("div",
                                                                                                                 class_="TruncatedContent__text TruncatedContent__text--small",
                                                                                                                 tabindex="-1",
                                                                                                                 data_testid="contentContainer") else None
                publisher = publication_date.split('by')[1].strip() if publication_date else None
                category = book_soup.find("span", class_="Button__labelItem").text if book_soup.find("span",
                                                                                                     class_="Button__labelItem") else None

                # Logowanie: Wypisanie danych książki, które udało się scrapować
                print(f"Scraped data for ISBN {isbn}:")
                print(f"Author: {author}")
                print(f"Rating: {rating}")
                print(f"Language: {language}")
                print(f"Num Pages: {num_pages}")
                print(f"Publication Date: {publication_date}")
                print(f"Publisher: {publisher}")
                print(f"Category: {category}")

                return {
                    "isbn": isbn,
                    "authors": author,
                    "rating_goodreads": rating,
                    "language_code": language,
                    "num_pages": num_pages,
                    "publication_date": publication_date,
                    "publisher": publisher,
                    "category": category
                }
    print(f"No data found for ISBN: {isbn}")  # Logowanie, jeśli brak danych
    return None  # Jeśli książka nie została znaleziona


# Funkcja do uzupełniania brakujących danych w CSV za pomocą wątków
def update_books_with_scraper(df):
    print("Starting to update books with missing data...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Filtrujemy książki, które mają puste dane w wymaganych kolumnach
        missing_data_books = df[df['rating_goodreads'].isna() | df['authors'].isna() | df['publication_date'].isna()]

        print(f"Found {len(missing_data_books)} books with missing data. Starting scraping process.")

        # Tworzymy zadania do równoległego wykonania tylko dla książek, które wymagają uzupełnienia danych
        future_to_isbn = {executor.submit(get_goodreads_data, isbn): isbn for isbn in missing_data_books['isbn']}

        for future in as_completed(future_to_isbn):
            isbn = future_to_isbn[future]
            try:
                # Przed scrapowaniem wyświetlamy dane książki
                old_data = df.loc[df['isbn'] == isbn]
                print(f"Before scraping (ISBN: {isbn}):\n{old_data[['isbn', 'title', 'authors', 'rating_goodreads', 'publication_date']]}")

                scraped_data = future.result()
                if scraped_data:
                    # Logowanie przed zapisaniem do DataFrame
                    print(f"Scraping result for ISBN {isbn}: {scraped_data}")

                    # Zaktualizowanie danych w df
                    df.loc[df['isbn'] == isbn, 'rating_goodreads'] = scraped_data.get("rating_goodreads", np.nan)
                    df.loc[df['isbn'] == isbn, 'publication_date'] = scraped_data.get("publication_date", np.nan)
                    df.loc[df['isbn'] == isbn, 'authors'] = scraped_data.get("author", np.nan)
                    df.loc[df['isbn'] == isbn, 'title'] = scraped_data.get("title", np.nan)
                    df.loc[df['isbn'] == isbn, 'language_code'] = scraped_data.get("language_code", np.nan)
                    df.loc[df['isbn'] == isbn, 'num_pages'] = scraped_data.get("num_pages", np.nan)
                    df.loc[df['isbn'] == isbn, 'publisher'] = scraped_data.get("publisher", np.nan)
                    df.loc[df['isbn'] == isbn, 'category'] = scraped_data.get("category", np.nan)

                    # Po scrapowaniu wyświetlamy zaktualizowane dane książki
                    new_data = df.loc[df['isbn'] == isbn]
                    print(f"After scraping (ISBN: {isbn}):\n{new_data[['isbn', 'title', 'authors', 'rating_goodreads', 'publication_date']]}")

                else:
                    print(f"No data found for ISBN {isbn}.")

            except Exception as e:
                print(f"Error processing ISBN {isbn}: {e}")

            # Losowe opóźnienie po każdej operacji
            time.sleep(random.uniform(1, 2))

    return df


# Wczytanie pliku CSV
csv_path = "../databases/bookstest.csv"
books_df = pd.read_csv(csv_path)

# Zaktualizowanie danych za pomocą scrappera
updated_books_df = update_books_with_scraper(books_df)

# Zapisanie zaktualizowanego pliku CSV
updated_books_df.to_csv("../databases/bookstest_final_updated.csv", index=False)
print("Zaktualizowano brakujące dane za pomocą scrappera!")
