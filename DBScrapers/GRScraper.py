import csv
import requests
from bs4 import BeautifulSoup
import time
import re


# Funkcja pobierająca dane z Goodreads na podstawie ISBN
def get_book_data(isbn):
    url = f"https://www.goodreads.com/book/show/{isbn}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Błąd pobierania dla ISBN {isbn}: status {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Błąd połączenia dla ISBN {isbn}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Ekstrakcja autorów
    authors_tags = soup.find_all("a", class_="authorName")
    authors = ", ".join([tag.text.strip() for tag in authors_tags]) if authors_tags else ""

    # Ekstrakcja oceny z Goodreads
    rating_tag = soup.find("span", itemprop="ratingValue")
    rating_goodreads = rating_tag.text.strip() if rating_tag else ""

    # Znajdź sekcję szczegółów
    details = soup.find("div", id="details")
    language_code = num_pages = publication_date = publisher = ""
    if details:
        # Ekstrakcja języka
        lang_tag = details.find("div", class_="infoBoxRowItem", string=re.compile("Edition Language"))
        if lang_tag:
            language_code = lang_tag.find_next_sibling("div").text.strip()

        # Ekstrakcja liczby stron
        pages_tag = details.find("span", itemprop="numberOfPages")
        if pages_tag:
            num_pages = pages_tag.text.strip().split()[0]

        # Ekstrakcja daty publikacji i wydawcy
        pub_info = details.find("div", string=re.compile("Published"))
        if pub_info:
            pub_text = pub_info.text.strip()
            match = re.search(r"Published\s+(.+?)\s+by\s+(.+)", pub_text)
            if match:
                publication_date = match.group(1).strip()
                publisher = match.group(2).strip()

    # Ekstrakcja kategorii (pierwszy gatunek)
    genre_tags = soup.find_all("a", class_="bookPageGenreLink")
    category = genre_tags[0].text.strip() if genre_tags else ""

    return {
        "authors": authors,
        "rating_goodreads": rating_goodreads,
        "language_code": language_code,
        "num_pages": num_pages,
        "publication_date": publication_date,
        "publisher": publisher,
        "category": category
    }


# Ścieżki do plików
input_file = r"..\databases\bookstest.csv"
output_file = r"..\databases\bookstest_final_updated.csv"

# Wczytanie pliku CSV
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    books = list(reader)
    fieldnames = reader.fieldnames  # Zachowaj nagłówki

# Przetwarzanie każdej książki
for book in books:
    isbn = book['isbn']
    print(f"Przetwarzam ISBN: {isbn}")

    # Pobierz dane tylko jeśli istnieją puste kolumny do uzupełnienia
    data = get_book_data(isbn)
    if data:
        # Uzupełnij tylko puste pola
        for key in data:
            if not book[key]:  # Jeśli pole jest puste
                book[key] = data[key] if data[key] else ""

    time.sleep(2)  # Opóźnienie 2 sekundy między żądaniami

# Zapis do nowego pliku CSV
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(books)

print(f"Zaktualizowane dane zapisano do {output_file}")