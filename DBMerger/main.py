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

# Wczytanie db3/books.csv
books_data3 = safe_read_csv("../databases/db3/books.csv", encoding='ISO-8859-1', engine='c', on_bad_lines='skip')
books_data3 = books_data3[["isbn", "title", "authors", "average_rating", "language_code", "num_pages"]]
books_data3 = books_data3.rename(columns={"average_rating": "rating_3"})

# Wczytanie db1/books.csv
books1 = safe_read_csv("../databases/db1/books.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip')
books1 = books1[["ISBN", "Book-Title", "Book-Author", "Year-Of-Publication", "Publisher"]]
books1 = books1.rename(columns={
    "ISBN": "isbn",
    "Book-Title": "title",
    "Book-Author": "authors",
    "Year-Of-Publication": "year_of_publication",
    "Publisher": "publisher"
})
books1_filtered = books1[~books1["isbn"].isin(books_data3["isbn"])]

# Wczytanie db1/ratings.csv
ratings = safe_read_csv("../databases/db1/ratings.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip')
ratings = ratings.rename(columns={"ISBN": "isbn", "Book-Rating": "rating"})
books_with_ratings = pd.merge(books1_filtered, ratings, on="isbn", how="left")
books_with_ratings["rating"] = books_with_ratings["rating"].fillna(books_with_ratings["rating"].mean())

# Połączenie książek z db3 i db1
final_books = pd.concat([books_data3, books_with_ratings], ignore_index=True)

# Zapis do pliku CSV
final_books.to_csv("../databases/merged_books_final.csv", index=False)
print("Zapisano finalny plik: merged_books_final.csv")
