import pandas as pd
import numpy as np
import logging
import csv

# Setup log
logging.basicConfig(filename='../databases/merge_warnings.log', level=logging.WARNING)

def safe_read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        logging.warning(f"Error reading {path}: {e}")
        return pd.DataFrame()

def add_missing_columns(df, required_columns):
    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan
    return df

def format_authors(author_str):
    if not isinstance(author_str, str):
        return ""
    authors = author_str.replace(";", ",").replace("/", ",").split(",")
    cleaned = [a.strip() for a in authors if a.strip()]
    return ", ".join(cleaned)

# Mapa języków
LANGUAGE_MAP = {
    "ara": "Arabic", "en-CA": "English", "eng": "English", "en-GB": "English", "enm": "English", "en-US": "English",
    "fre": "French", "ger": "German", "gla": "Scottish Gaelic", "glg": "Galician", "grc": "Ancient Greek",
    "ita": "Italian", "jpn": "Japanese", "lat": "Latin", "msa": "Malay", "mul": "Multiple", "nl": "Dutch",
    "nor": "Norwegian", "por": "Portuguese", "rus": "Russian", "spa": "Spanish", "srp": "Serbian",
    "swe": "Swedish", "tur": "Turkish", "wel": "Welsh", "zho": "Chinese"
}

# ---------------------
# 1. db3
books_data3 = safe_read_csv("../databases/db3/books.csv", encoding='utf-8', engine='c', on_bad_lines='skip', low_memory=False)

books_data3["authors"] = books_data3["authors"].apply(format_authors)
books_data3["title"] = books_data3["title"].str.replace('"', '').str.strip()
books_data3["publication_date"] = pd.to_datetime(books_data3["publication_date"], errors='coerce').dt.year

books_data3 = books_data3[["isbn", "title", "authors", "average_rating", "language_code", "num_pages", "publication_date", "publisher"]]
books_data3 = books_data3.rename(columns={"average_rating": "rating_goodreads", "language_code": "language"})
books_data3["rating_google"] = books_data3["rating_goodreads"]
books_data3["language"] = books_data3["language"].map(LANGUAGE_MAP).fillna("Unknown")

# ---------------------
# 2. db1
books1 = safe_read_csv("../databases/db1/books.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip', low_memory=False)
books1 = books1[["ISBN", "Book-Title", "Book-Author", "Year-Of-Publication", "Publisher"]]
books1 = books1.rename(columns={
    "ISBN": "isbn",
    "Book-Title": "title",
    "Book-Author": "authors",
    "Year-Of-Publication": "publication_date",
    "Publisher": "publisher"
})

books1["authors"] = books1["authors"].apply(format_authors)
books1["title"] = books1["title"].str.replace('"', '').str.strip()
books1["publication_date"] = pd.to_numeric(books1["publication_date"], errors="coerce")
books1["language"] = "Unknown"

books1_filtered = books1[~books1["isbn"].isin(books_data3["isbn"])]

ratings = safe_read_csv("../databases/db1/ratings.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip', low_memory=False)
ratings = ratings.rename(columns={"ISBN": "isbn", "Book-Rating": "rating"})
books_with_ratings = pd.merge(books1_filtered, ratings, on="isbn", how="left")
books_with_ratings["rating_amazon"] = books_with_ratings["rating"].fillna(books_with_ratings["rating"].mean())

# ---------------------
# 3. db2
books_data2 = safe_read_csv("../databases/db2/books.csv", encoding='ISO-8859-1', engine='c', on_bad_lines='skip', low_memory=False)

if 'title' in books_data2.columns and 'subtitle' in books_data2.columns:
    books_data2["title"] = books_data2["title"].fillna("") + " " + books_data2["subtitle"].fillna("")
else:
    logging.warning("Brak jednej z kolumn 'title' lub 'subtitle' w db2/books.csv.")

required_columns_db2 = ["isbn10", "title", "authors", "num_pages", "average_rating", "categories"]
books_data2 = books_data2[required_columns_db2]

books_data2 = books_data2.rename(columns={"isbn10": "isbn", "average_rating": "rating_goodreads"})
books_data2["authors"] = books_data2["authors"].apply(format_authors)
books_data2["title"] = books_data2["title"].str.replace('"', '').str.strip()
books_data2["publication_date"] = np.nan
books_data2["language"] = "Unknown"

if 'publisher' not in books_data2.columns:
    books_data2['publisher'] = np.nan

if 'categories' in books_data2.columns:
    books_data2['category'] = books_data2['categories'].fillna("").str.split(",").str[0]
else:
    logging.warning("Brak kolumny 'categories' w db2/books.csv.")

# ---------------------
# 4. Uzupełnianie braków
required_columns = ["isbn", "title", "authors", "rating_goodreads", "language", "num_pages", "publication_date", "publisher", "rating_amazon", "rating_google", "category"]

books_data3 = add_missing_columns(books_data3, required_columns)
books_with_ratings = add_missing_columns(books_with_ratings, required_columns)
books_data2 = add_missing_columns(books_data2, required_columns)

# ---------------------
# 5. Łączenie i zapis
final_books = pd.concat([books_data3, books_with_ratings, books_data2], ignore_index=True)
final_books = final_books.drop_duplicates(subset="isbn", keep="first")

final_books = final_books[["isbn", "title", "authors", "rating_goodreads", "language", "num_pages", "publication_date", "publisher", "rating_amazon", "rating_google", "category"]]
final_books["rating_amazon"] = final_books["rating_amazon"].fillna(final_books["rating_amazon"].mean())
final_books["rating_google"] = final_books["rating_google"].fillna(final_books["rating_google"].mean())
final_books["rating_amazon"] = final_books["rating_amazon"].round(2)
final_books["rating_google"] = final_books["rating_google"].round(2)

# 🔐 Zapis z pełnym quotingiem tekstu
# Naprawiamy typy: bez ".0" w liczbach
final_books["publication_date"] = final_books["publication_date"].astype("Int64")
final_books["num_pages"] = final_books["num_pages"].astype("Int64")

# Zamień pustą kategorię na pusty string BEZ cudzysłowu
final_books["category"] = final_books["category"].replace("", pd.NA).fillna("")

# Tylko wybrane kolumny tekstowe mają być w cudzysłowie
# CSV bez automatycznego quotingowania
final_books.to_csv(
    "../databases/merged_books_final.csv",
    index=False,
    quoting=csv.QUOTE_MINIMAL,  # tylko jeśli musowo
    quotechar='"',
    na_rep='',  # brak wartości = pusto
    doublequote=True
)
print("Zapisano finalny plik: merged_books_final.csv (bez zbędnych cudzysłowów)")
