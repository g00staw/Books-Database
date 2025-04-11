import pandas as pd
import numpy as np
import logging

# Setup log
logging.basicConfig(filename='../databases/merge_warnings.log', level=logging.WARNING)

def safe_read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        logging.warning(f"Error reading {path}: {e}")
        return pd.DataFrame()

def add_missing_columns(df, required_columns):
    # Funkcja uzupełniająca brakujące kolumny jako NaN
    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan  # Dodajemy kolumnę z NaN, jeśli nie ma jej w DataFrame
    return df

# ---------------------
# 1. Wczytanie db3/books.csv i przekształcenie danych
books_data3 = safe_read_csv("../databases/db3/books.csv", encoding='ISO-8859-1', engine='c', on_bad_lines='skip')

# Usuwamy dodatkowych autorów i zmieniamy nazwę kolumny
books_data3["authors"] = books_data3["authors"].str.split("/").str[0]  # Bierzemy tylko pierwszego autora
books_data3 = books_data3[["isbn", "title", "authors", "average_rating", "language_code", "num_pages", "publication_date", "publisher"]]
books_data3 = books_data3.rename(columns={"average_rating": "rating_goodreads"})

# Uzupełnianie rating_google (dodatkowo z tej samej bazy db3)
books_data3["rating_google"] = books_data3["rating_goodreads"]  # Przyjmujemy rating_goodreads jako rating_google na razie

# ---------------------
# 2. Wczytanie db1/books.csv i usunięcie książek, które już istnieją w db3
books1 = safe_read_csv("../databases/db1/books.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip')

# Przekształcenie i dopasowanie kolumn
books1 = books1[["ISBN", "Book-Title", "Book-Author", "Year-Of-Publication", "Publisher"]]
books1 = books1.rename(columns={
    "ISBN": "isbn",
    "Book-Title": "title",
    "Book-Author": "authors",
    "Year-Of-Publication": "year_of_publication",
    "Publisher": "publisher"
})

# Usunięcie książek, które już istnieją w db3 (po isbn)
books1_filtered = books1[~books1["isbn"].isin(books_data3["isbn"])]

# ---------------------
# 3. Wczytanie db1/ratings.csv i połączenie opinii z książkami
ratings = safe_read_csv("../databases/db1/ratings.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip')
ratings = ratings.rename(columns={"ISBN": "isbn", "Book-Rating": "rating"})

# Połączenie ocen z książkami z db1
books_with_ratings = pd.merge(books1_filtered, ratings, on="isbn", how="left")
books_with_ratings["rating_amazon"] = books_with_ratings["rating"].fillna(books_with_ratings["rating"].mean())

# ---------------------
# 4. Wczytanie db2/books.csv i połączenie tytułów i kategorii
books_data2 = safe_read_csv("../databases/db2/books.csv", encoding='ISO-8859-1', engine='c', on_bad_lines='skip')

# Łączenie 'title' i 'subtitle' w jedną kolumnę 'title'
if 'title' in books_data2.columns and 'subtitle' in books_data2.columns:
    books_data2["title"] = books_data2["title"] + " " + books_data2["subtitle"]
else:
    logging.warning("Brak jednej z kolumn 'title' lub 'subtitle' w db2/books.csv.")

# Wybieramy tylko potrzebne kolumny
required_columns_db2 = ["isbn10", "title", "authors", "num_pages", "average_rating", "categories"]
books_data2 = books_data2[required_columns_db2]

# Zmieniamy nazwę kolumny 'isbn10' na 'isbn'
books_data2 = books_data2.rename(columns={"isbn10": "isbn", "average_rating": "rating_goodreads"})

# Dodajemy brakującą kolumnę 'publisher' w db2, jeśli jej brak
if 'publisher' not in books_data2.columns:
    books_data2['publisher'] = np.nan  # Dodajemy pustą kolumnę, jeśli jej brak

# Dodanie kolumny 'category' (pierwsza kategoria z 'categories')
if 'categories' in books_data2.columns:
    books_data2['category'] = books_data2['categories'].str.split(",").str[0]  # Bierzemy tylko pierwszą kategorię
else:
    logging.warning("Brak kolumny 'categories' w db2/books.csv.")

# ---------------------
# 5. Dodanie brakujących kolumn do wszystkich danych
required_columns = ["isbn", "title", "authors", "rating_goodreads", "language_code", "num_pages", "publication_date", "publisher", "rating_amazon", "rating_google", "category"]

# Uzupełniamy brakujące kolumny w każdej z baz danych
books_data3 = add_missing_columns(books_data3, required_columns)
books_with_ratings = add_missing_columns(books_with_ratings, required_columns)
books_data2 = add_missing_columns(books_data2, required_columns)

# ---------------------
# 6. Połączenie wszystkich danych
final_books = pd.concat([books_data3, books_with_ratings, books_data2], ignore_index=True)

# Usunięcie duplikatów po isbn, zachowując tylko pierwsze wystąpienie
final_books = final_books.drop_duplicates(subset="isbn", keep="first")

# Finalne kolumny, dopasowanie do docelowej struktury
final_books = final_books[["isbn", "title", "authors", "rating_goodreads", "language_code", "num_pages", "publication_date", "publisher", "rating_amazon", "rating_google", "category"]]

# Uzupełnienie brakujących danych (np. brakujących kolumn w db2)
final_books["rating_amazon"] = final_books["rating_amazon"].fillna(final_books["rating_amazon"].mean())
final_books["rating_google"] = final_books["rating_google"].fillna(final_books["rating_google"].mean())

final_books["rating_amazon"] = final_books["rating_amazon"].round(2)
final_books["rating_google"] = final_books["rating_google"].round(2)

# ---------------------
# 7. Zapis do pliku CSV
final_books.to_csv("../databases/merged_books_final.csv", index=False)
print("Zapisano finalny plik: merged_books_final.csv")
