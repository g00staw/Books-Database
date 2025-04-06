
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

# Funkcja do wczytywania dużych plików CSV w kawałkach
def load_large_csv(filepath, chunksize=100000):
    chunks = []
    try:
        for chunk in pd.read_csv(filepath, chunksize=chunksize, low_memory=False, encoding='ISO-8859-1', engine='c', on_bad_lines='skip'):
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True)
    except Exception as e:
        logging.warning(f"Error reading large CSV {filepath}: {e}")
        return pd.DataFrame()

# ---------------------
books1 = safe_read_csv("../databases/db1/books.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip', low_memory=False)
books1.columns = books1.columns.str.strip().str.replace('"', '')
books1 = books1.rename(columns={
    "ISBN": "isbn",
    "Book-Title": "title",
    "Book-Author": "author",
    "Year-Of-Publication": "year_of_publication",
    "Publisher": "publisher"
})
books1["year_of_publication"] = books1["year_of_publication"].astype(str)
books1 = books1[["isbn", "title", "author", "year_of_publication", "publisher"]]

ratings1 = safe_read_csv("../databases/db1/ratings.csv", sep=';', engine='c', encoding='ISO-8859-1', on_bad_lines='skip')
ratings1.columns = ratings1.columns.str.strip().str.replace('"', '')
ratings1 = ratings1.rename(columns={"ISBN": "isbn", "Book-Rating": "rating"})
ratings1["rating"] = pd.to_numeric(ratings1["rating"], errors='coerce')
ratings1 = ratings1.groupby("isbn")["rating"].mean().reset_index()

merged1 = pd.merge(books1, ratings1, on="isbn", how="left")

# ---------------------
books_rating2 = load_large_csv("../databases/db2/Books_rating.csv", chunksize=500000)

if not books_rating2.empty:
    books_rating2.columns = books_rating2.columns.astype(str).str.strip()

if not books_rating2.empty:
    books_rating2.columns = books_rating2.columns.astype(str).str.strip()
    if "review/score" in books_rating2.columns:
        books_rating2 = books_rating2.rename(columns={"Id": "isbn", "review/score": "rating_amazon"})
        books_rating2["rating_amazon"] = pd.to_numeric(books_rating2["rating_amazon"], errors='coerce')
        rating_amazon_avg = books_rating2.groupby("isbn")["rating_amazon"].mean().reset_index()
    else:
        logging.warning("Kolumna 'review/score' nie została znaleziona w ratings2.csv — pominięto rating_amazon.")
        rating_amazon_avg = pd.DataFrame(columns=["isbn", "rating_amazon"])
else:
    rating_amazon_avg = pd.DataFrame(columns=["isbn", "rating_amazon"])

rating_amazon_avg = books_rating2.groupby("isbn")["rating_amazon"].mean().reset_index()

# ---------------------
books_data2 = safe_read_csv("../databases/db2/books_data.csv", encoding='ISO-8859-1', engine='c', on_bad_lines='skip')
books_data2 = books_data2.rename(columns={
    "Title": "title",
    "authors": "author",
    "publisher": "publisher",
    "publishedDate": "year_of_publication",
    "ratingsCount": "ratings_count"
})[["title", "author", "publisher", "year_of_publication", "ratings_count"]]

# ---------------------
books_data3 = safe_read_csv("../databases/db3/books.csv", encoding='ISO-8859-1', engine='c', on_bad_lines='skip')
expected_cols = {
    "title": "title",
    "authors": "author",
    "language_code": "language",
    "num_pages": "num_pages",
    "average_rating": "rating_3",
    "publication_date": "year_of_publication",
    "publisher": "publisher",
    "isbn": "isbn"
}
available_cols = {k: v for k, v in expected_cols.items() if k in books_data3.columns}
books_data3 = books_data3.rename(columns=available_cols)[list(available_cols.values())]

# ---------------------
# Merge po ISBN
df = pd.merge(merged1, rating_amazon_avg, on="isbn", how="outer")
df = pd.merge(df, books_data3, on="isbn", how="outer", suffixes=("", "_3"))

# Merge dodatkowy po title jeśli isbn jest pusty
df_missing = df[df["title"].notna() & df["isbn"].isna()]
books_data2["title"] = books_data2["title"].str.strip().str.lower()
df_missing["title"] = df_missing["title"].str.strip().str.lower()
df_fallback = pd.merge(df_missing, books_data2, on="title", how="left", suffixes=("", "_from_title"))

# Scal oryginalny i fallback
df_final = pd.concat([df[df["isbn"].notna()], df_fallback], ignore_index=True)

# Uśrednianie ocen
df_final["rating"] = df_final[["rating", "rating_amazon", "rating_3"]].mean(axis=1)

# Finalne kolumny
cols_final = ["isbn", "title", "author", "language", "num_pages", "year_of_publication", "publisher", "rating", "rating_amazon"]
for col in cols_final:
    if col not in df_final.columns:
        df_final[col] = np.nan

final = df_final[cols_final]
final.loc[:, "rating_polish"] = np.nan

# Zapis
final.to_csv("../databases/merged_books_final.csv", index=False)
print("Zapisano finalny plik: merged_books_final.csv")
print("Błędy zapisano do: merge_warnings.log")
