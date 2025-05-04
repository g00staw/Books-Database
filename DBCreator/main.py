import pandas as pd
from neo4j import GraphDatabase


class Neo4jBooksImporter:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def import_books(self, csv_file):
        df = pd.read_csv(csv_file)

        with self.driver.session() as session:
            self._create_constraints(session)

            for _, row in df.iterrows():
                self._process_book(session, row)

        print(f"Imported {len(df)} books into Neo4j")

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
                print(f"Constraint creation issue: {e}")

    def _process_book(self, session, row):
        isbn = str(row['isbn'])
        title = row['title']

        publisher = row.get('publisher', '')
        if pd.isna(publisher):
            publisher = 'Unknown'

        pub_date = row.get('publication_date', '')
        if pd.isna(pub_date):
            year = 'Unknown'
        else:
            year = str(pub_date)

        language = row.get('language', '')
        if pd.isna(language):
            language = 'Unknown'

        book_query = """
        MERGE (b:Book {isbn: $isbn})
        SET b.title = $title
        RETURN b
        """
        session.run(book_query, isbn=isbn, title=title)

        if publisher and publisher != 'Unknown':
            publisher_query = """
            MERGE (p:Publisher {name: $publisher})
            WITH p
            MATCH (b:Book {isbn: $isbn})
            MERGE (b)-[:PUBLISHED_BY]->(p)
            """
            session.run(publisher_query, publisher=publisher, isbn=isbn)

        if year and year != 'Unknown':
            year_query = """
            MERGE (y:Year {year: $year})
            WITH y
            MATCH (b:Book {isbn: $isbn})
            MERGE (b)-[:PUBLISHED_IN]->(y)
            """
            session.run(year_query, year=year, isbn=isbn)

        if language and language != 'Unknown':
            language_query = """
            MERGE (l:Language {name: $language})
            WITH l
            MATCH (b:Book {isbn: $isbn})
            MERGE (b)-[:WRITTEN_IN]->(l)
            """
            session.run(language_query, language=language, isbn=isbn)

        authors_raw = row.get('authors', '')
        if not pd.isna(authors_raw):
            if authors_raw.startswith('"') and authors_raw.endswith('"'):
                authors_content = authors_raw[1:-1]
            else:
                authors_content = authors_raw

            author_list = [a.strip() for a in authors_content.split(',')]

            for author in author_list:
                if author:
                    author_query = """
                    MERGE (a:Author {name: $author})
                    WITH a
                    MATCH (b:Book {isbn: $isbn})
                    MERGE (b)-[:WRITTEN_BY]->(a)
                    """
                    session.run(author_query, author=author, isbn=isbn)

        categories = row.get('category', '')
        if not pd.isna(categories):
            genre_list = [g.strip() for g in categories.split(',')]

            for genre in genre_list:
                if genre:
                    genre_query = """
                    MERGE (g:Genre {name: $genre})
                    WITH g
                    MATCH (b:Book {isbn: $isbn})
                    MERGE (b)-[:BELONGS_TO]->(g)
                    """
                    session.run(genre_query, genre=genre, isbn=isbn)


# Usage example
if __name__ == "__main__":
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"

    importer = Neo4jBooksImporter(uri, username, password)

    try:
        importer.import_books("bookstest_final_updated.csv")
    finally:
        importer.close()