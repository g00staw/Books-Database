
import pandas as pd
import numpy as np

# Funkcja do wczytywania dużych plików CSV w kawałkach
def load_large_csv(filepath, chunksize=100000):
    chunks = []
    for chunk in pd.read_csv(filepath, chunksize=chunksize, low_memory=False):
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)

# ---------------------
# Wczytywanie i czyszczenie books1.csv
books1 = pd.read_csv("books1.csv", sep=';', engine='python')
books1.columns = books1.columns.str.strip().str.replace('"', '')
books1 = books1.rename(columns={
    "ISBN": "isbn",
    "Book-Title": "title",
    "Book-Author": "author",
    "Year-Of-Publication": "year_of_publication",
    "Publisher": "publisher"
})[["isbn", "title", "author", "year_of_publication", "publisher"]]

# Wczytanie ratings1.csv
ratings1 = pd.read_csv("ratings1.csv", sep=';', engine='python')
ratings1.columns = ratings1.columns.str.strip().str.replace('"', '')
ratings1 = ratings1.rename(columns={"ISBN": "isbn", "Book-Rating": "rating"})
ratings1["rating"] = pd.to_numeric(ratings1["rating"], errors='coerce')
ratings1 = ratings1.groupby("isbn")["rating"].mean().reset_index()

# Połączenie books1 z ratings1
merged1 = pd.merge(books1, ratings1, on="isbn", how="left")

# ---------------------
# Wczytanie Books_rating2.csv
books_rating2 = load_large_csv("Books_rating2.csv", chunksize=500000)
books_rating2.columns = books_rating2.columns.str.strip()
books_rating2 = books_rating2.rename(columns={"Id": "isbn", "review/score": "rating_amazon"})
books_rating2["rating_amazon"] = pd.to_numeric(books_rating2["rating_amazon"], errors='coerce')
rating_amazon_avg = books_rating2.groupby("isbn")["rating_amazon"].mean().reset_index()

# ---------------------
# Wczytanie books_data2.xlsx
books_data2 = pd.read_excel("books_data2.xlsx")
books_data2 = books_data2.rename(columns={
    "Title": "title",
    "authors": "author",
    "publisher": "publisher",
    "publishedDate": "year_of_publication",
    "ratingsCount": "ratings_count"
})[["title", "author", "publisher", "year_of_publication", "ratings_count"]]

# ---------------------
# Wczytanie books_data3.csv
books_data3 = pd.read_csv("books_data3.csv")
books_data3 = books_data3.rename(columns={
    "title": "title",
    "authors": "author",
    "language_code": "language",
    "num_pages": "num_pages",
    "average_rating": "rating_3",
    "publication_date": "year_of_publication",
    "publisher": "publisher",
    "isbn": "isbn"
})[["isbn", "title", "author", "language", "num_pages", "year_of_publication", "publisher", "rating_3"]]

# ---------------------
# Scalanie wszystkich baz
df = pd.merge(merged1, rating_amazon_avg, on="isbn", how="outer")
df = pd.merge(df, books_data3, on="isbn", how="outer", suffixes=("", "_3"))

# Uśrednianie ocen z różnych źródeł
df["rating"] = df[["rating", "rating_amazon", "rating_3"]].mean(axis=1)

# Finalny wybór kolumn
final = df[["isbn", "title", "author", "language", "num_pages", "year_of_publication", "publisher", "rating", "rating_amazon"]]
final["rating_polish"] = np.nan  # do uzupełnienia scrapowaniem

# Zapis do pliku
final.to_csv("merged_books_final.csv", index=False)
print("Zapisano finalny plik merged_books_final.csv")
