import pandas as pd
from neo4j import GraphDatabase


class Neo4jBooksImporter:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def clear_database(self, delete_constraints=True):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Wszystkie węzły i relacje zostały usunięte.")

            if delete_constraints:
                result = session.run("SHOW CONSTRAINTS")
                constraints = [record["name"] for record in result]

                for constraint in constraints:
                    try:
                        session.run(f"DROP CONSTRAINT {constraint}")
                    except Exception as e:
                        print(f"Błąd podczas usuwania ograniczenia {constraint}: {e}")

                print("Wszystkie ograniczenia zostały usunięte.")

    def import_books(self, csv_file):
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
            print(f"Wczytano plik CSV: {csv_file}")
            print(f"Liczba wierszy: {len(df)}")
            print(f"Kolumny: {', '.join(df.columns)}")

            required_columns = ['isbn', 'title']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Brakujące wymagane kolumny w pliku CSV: {', '.join(missing_columns)}")

            with self.driver.session() as session:
                self._create_constraints(session)

                imported_count = 0
                error_count = 0
                for _, row in df.iterrows():
                    try:
                        self._process_book(session, row)
                        imported_count += 1
                    except Exception as e:
                        error_count += 1
                        print(f"Błąd podczas przetwarzania książki: {e}")

                        try:
                            isbn = row.get('isbn', 'Nieznany')
                            print(f"Problematyczny ISBN: {isbn}")
                        except:
                            pass

            print(f"Zaimportowano {imported_count} książek do Neo4j")
            if error_count > 0:
                print(f"Wystąpiło {error_count} błędów podczas importu")

        except Exception as e:
            print(f"Błąd podczas importu książek: {e}")
            raise

    def _create_constraints(self, session):
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Book) REQUIRE b.isbn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publisher) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (y:Year) REQUIRE y.year IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Language) REQUIRE l.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE"
        ]

        for constraint in constraints:
            try:
                session.run(constraint)
            except Exception as e:
                print(f"Problem przy tworzeniu ograniczenia: {e}")

    def _process_book(self, session, row):
        try:
            isbn = str(row['isbn']).strip()
            if pd.isna(isbn) or isbn == '' or isbn == 'nan':
                isbn = f"unknown_{hash(str(row))}"
                print(f"Ostrzeżenie: Znaleziono książkę bez ISBN. Wygenerowano ID: {isbn}")

            title = str(row['title']).strip() if not pd.isna(row['title']) else "Nieznany tytuł"
        except Exception as e:
            raise ValueError(f"Błąd podczas przetwarzania podstawowych danych książki: {e}")

        publisher = row.get('publisher', '')
        if pd.isna(publisher) or publisher == '':
            publisher = 'Nieznany'
        else:
            publisher = str(publisher).strip()

        pub_date = row.get('publication_date', '')
        if pd.isna(pub_date) or pub_date == '':
            year = 'Nieznany'
        else:
            try:
                year = str(int(pub_date))
            except:
                year = str(pub_date).strip()

        language = row.get('language', '')
        if pd.isna(language) or language == '':
            language = 'Nieznany'
        else:
            language = str(language).strip()

        try:
            num_pages = int(row.get('num_pages', 0)) if not pd.isna(row.get('num_pages', 0)) else 0
        except:
            num_pages = 0

        try:
            rating_goodreads = float(row.get('rating_goodreads', 0)) if not pd.isna(
                row.get('rating_goodreads', 0)) else None
            rating_amazon = float(row.get('rating_amazon', 0)) if not pd.isna(row.get('rating_amazon', 0)) else None
            rating_google = float(row.get('rating_google', 0)) if not pd.isna(row.get('rating_google', 0)) else None
        except Exception as e:
            print(f"Ostrzeżenie: Problem z konwersją ocen dla ISBN {isbn}: {e}")
            rating_goodreads = None
            rating_amazon = None
            rating_google = None

        book_query = """
        MERGE (b:Book {isbn: $isbn})
        SET b.title = $title,
            b.num_pages = $num_pages
        """

        params = {
            "isbn": isbn,
            "title": title,
            "num_pages": num_pages
        }

        if rating_goodreads is not None:
            book_query += ", b.rating_goodreads = $rating_goodreads"
            params["rating_goodreads"] = rating_goodreads

        if rating_amazon is not None:
            book_query += ", b.rating_amazon = $rating_amazon"
            params["rating_amazon"] = rating_amazon

        if rating_google is not None:
            book_query += ", b.rating_google = $rating_google"
            params["rating_google"] = rating_google

        book_query += " RETURN b"

        try:
            session.run(book_query, **params)
        except Exception as e:
            raise ValueError(f"Błąd podczas tworzenia węzła Book: {e}")

        if publisher and publisher != 'Nieznany':
            try:
                publisher_query = """
                MERGE (p:Publisher {name: $publisher})
                WITH p
                MATCH (b:Book {isbn: $isbn})
                MERGE (b)-[:PUBLISHED_BY]->(p)
                """
                session.run(publisher_query, publisher=publisher, isbn=isbn)
            except Exception as e:
                print(f"Ostrzeżenie: Problem z przetwarzaniem wydawcy dla ISBN {isbn}: {e}")

        if year and year != 'Nieznany':
            try:
                year_query = """
                MERGE (y:Year {year: $year})
                WITH y
                MATCH (b:Book {isbn: $isbn})
                MERGE (b)-[:PUBLISHED_IN]->(y)
                """
                session.run(year_query, year=year, isbn=isbn)
            except Exception as e:
                print(f"Ostrzeżenie: Problem z przetwarzaniem roku dla ISBN {isbn}: {e}")

        if language and language != 'Nieznany':
            try:
                language_query = """
                MERGE (l:Language {name: $language})
                WITH l
                MATCH (b:Book {isbn: $isbn})
                MERGE (b)-[:WRITTEN_IN]->(l)
                """
                session.run(language_query, language=language, isbn=isbn)
            except Exception as e:
                print(f"Ostrzeżenie: Problem z przetwarzaniem języka dla ISBN {isbn}: {e}")

        authors_raw = row.get('authors', '')
        if not pd.isna(authors_raw) and authors_raw != '':
            try:
                if isinstance(authors_raw, str) and authors_raw.startswith('"') and authors_raw.endswith('"'):
                    authors_content = authors_raw[1:-1]
                else:
                    authors_content = str(authors_raw)

                author_list = [a.strip() for a in authors_content.split(',')]

                for author in author_list:
                    if not author or author == 'nan':
                        continue

                    author_query = """
                    MERGE (a:Author {name: $author})
                    WITH a
                    MATCH (b:Book {isbn: $isbn})
                    MERGE (b)-[:WRITTEN_BY]->(a)
                    """
                    session.run(author_query, author=author, isbn=isbn)
            except Exception as e:
                print(f"Ostrzeżenie: Problem z przetwarzaniem autorów dla ISBN {isbn}: {e}")

        categories = row.get('category', '')
        if not pd.isna(categories) and categories != '':
            try:
                genre_list = [g.strip() for g in str(categories).split(',')]

                for genre in genre_list:
                    if not genre or genre == 'nan':
                        continue

                    genre_query = """
                    MERGE (g:Genre {name: $genre})
                    WITH g
                    MATCH (b:Book {isbn: $isbn})
                    MERGE (b)-[:BELONGS_TO]->(g)
                    """
                    session.run(genre_query, genre=genre, isbn=isbn)
            except Exception as e:
                print(f"Ostrzeżenie: Problem z przetwarzaniem kategorii dla ISBN {isbn}: {e}")


# Usage example
if __name__ == "__main__":
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"

    importer = Neo4jBooksImporter(uri, username, password)

    try:
        importer.import_books("../databases/Books1KPlus.csv")
    finally:
        importer.close()