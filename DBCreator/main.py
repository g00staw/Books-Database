from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import re

# Łączenie z bazą danych Neo4j
uri = "bolt://localhost:7687"  # Neo4j lokalnie
username = "neo4j"  # Domyślny użytkownik Neo4j
password = "password"  # Hasło do Neo4j (zmień w razie potrzeby)

# Połączenie z Neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()

# Funkcja do tworzenia węzła w bazie Neo4j
def create_book_node(tx, isbn, title, num_pages, publication_date, rating_goodreads):
    query = (
        f"CREATE (b:Book {{isbn: '{isbn}', title: '{title}', num_pages: {num_pages}, "
        f"publication_date: {publication_date}, rating_goodreads: {rating_goodreads}}})"
    )
    tx.run(query)

def create_author_node(tx, author_name):
    query = f"MERGE (a:Author {{name: '{author_name}'}})"
    tx.run(query)

def create_genre_node(tx, genre_name):
    query = f"MERGE (g:Genre {{name: '{genre_name}'}})"
    tx.run(query)

def create_publisher_node(tx, publisher_name):
    query = f"MERGE (p:Publisher {{name: '{publisher_name}'}})"
    tx.run(query)

def create_language_node(tx, language_name):
    query = f"MERGE (l:Language {{name: '{language_name}'}})"
    tx.run(query)

def create_year_node(tx, year):
    query = f"MERGE (y:Year {{year: {year}}})"
    tx.run(query)

# Funkcja do tworzenia relacji między węzłami
def create_relationships(tx, isbn, authors, genre, publisher, language, publication_date):
    # Relacje między książką a autorem
    for author in authors.split(','):
        author_name = author.strip()
        tx.run(f"MATCH (b:Book {{isbn: '{isbn}'}}), (a:Author {{name: '{author_name}'}}) "
               "MERGE (b)-[:AUTHORED_BY]->(a)")  # MERGE zamiast CREATE

    # Relacja między książką a gatunkiem
    tx.run(f"MATCH (b:Book {{isbn: '{isbn}'}}), (g:Genre {{name: '{genre}'}}) "
           "MERGE (b)-[:BELONGS_TO_GENRE]->(g)")  # MERGE zamiast CREATE

    # Relacja między książką a wydawnictwem
    tx.run(f"MATCH (b:Book {{isbn: '{isbn}'}}), (p:Publisher {{name: '{publisher}'}}) "
           "MERGE (b)-[:PUBLISHED_BY]->(p)")  # MERGE zamiast CREATE

    # Relacja między książką a językiem
    tx.run(f"MATCH (b:Book {{isbn: '{isbn}'}}), (l:Language {{name: '{language}'}}) "
           "MERGE (b)-[:HAS_LANGUAGE]->(l)")  # MERGE zamiast CREATE

    # Relacja między książką a rokiem wydania
    tx.run(f"MATCH (b:Book {{isbn: '{isbn}'}}), (y:Year {{year: {publication_date}}}) "
           "MERGE (b)-[:PUBLISHED_IN]->(y)")  # MERGE zamiast CREATE


# Wczytanie pliku CSV
df = pd.read_csv('../databases/bookstest_final_updated.csv',
                 dtype={'isbn': str})  # Upewniamy się, że ISBN jest traktowane jako string

# Przetworzenie danych w celu zapewnienia odpowiedniego formatu
df['title'] = df['title'].apply(lambda x: f'"{x}"' if not x.startswith('"') else x)  # Tytuł w cudzysłowie
df['authors'] = df['authors'].apply(
    lambda x: ', '.join([author.strip() for author in x.split(',')]))  # Ujednolicenie autorów
df['publication_date'] = pd.to_numeric(df['publication_date'], errors='coerce')  # Rok wydania na int
df['num_pages'] = pd.to_numeric(df['num_pages'], errors='coerce')  # Liczba stron na int

# Debugowanie: Sprawdzamy, ile książek jest w pliku
print(f"Liczba książek w pliku CSV: {len(df)}")

# Przekształcenie danych i import do Neo4j dla każdej książki w pliku
for index, row in df.iterrows():
    isbn = row['isbn']
    title = row['title']
    num_pages = row['num_pages'] if pd.notna(row['num_pages']) else 0  # 0 jeśli brak
    publication_date = int(row['publication_date']) if pd.notna(row['publication_date']) else 0  # 0 jeśli brak
    rating_goodreads = row['rating_goodreads'] if pd.notna(row['rating_goodreads']) else 0.0
    authors = row['authors']

    # Sprawdzamy, czy wartość w kolumnie 'category' jest NaN (brakująca)
    category = row['category'] if pd.notna(row['category']) else ""  # Jeśli NaN, przypisujemy pusty ciąg
    genre = category.split(',')[0]  # Bierzemy pierwszy gatunek

    publisher = row['publisher']
    language = row['language']

    with session.begin_transaction() as tx:
        # Tworzymy węzły
        create_book_node(tx, isbn, title, num_pages, publication_date, rating_goodreads)
        create_genre_node(tx, genre)
        create_publisher_node(tx, publisher)
        create_language_node(tx, language)
        create_year_node(tx, publication_date)

        # Tworzymy relacje
        create_relationships(tx, isbn, authors, genre, publisher, language, publication_date)

    # Debugowanie: Sprawdzamy postęp
    print(f"Zaimportowano książkę: {title} ({isbn})")

# Zamykanie sesji
session.close()
print("Dane zostały zaimportowane do Neo4j.")
