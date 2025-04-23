import pandas as pd
from neo4j import GraphDatabase


class Neo4jBooksImporter:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def import_books(self, csv_file):
        # Read CSV file
        df = pd.read_csv(csv_file)

        # Clean up and process each book
        with self.driver.session() as session:
            # Create constraints for unique nodes (if they don't exist)
            self._create_constraints(session)

            # Process each book row
            for _, row in df.iterrows():
                self._process_book(session, row)

        print(f"Imported {len(df)} books into Neo4j")

    def _create_constraints(self, session):
        # Create constraints to ensure uniqueness
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
        # Extract data from row
        isbn = str(row['isbn'])
        title = row['title']

        # Handle potential missing/NaN values
        publisher = row.get('publisher', '')
        if pd.isna(publisher):
            publisher = 'Unknown'

        # Handle publication year
        pub_date = row.get('publication_date', '')
        if pd.isna(pub_date):
            year = 'Unknown'
        else:
            year = str(pub_date)

        # Handle language
        language = row.get('language', '')
        if pd.isna(language):
            language = 'Unknown'

        # Create Book node
        book_query = """
        MERGE (b:Book {isbn: $isbn})
        SET b.title = $title
        RETURN b
        """
        session.run(book_query, isbn=isbn, title=title)

        # Create Publisher node and relationship
        if publisher and publisher != 'Unknown':
            publisher_query = """
            MERGE (p:Publisher {name: $publisher})
            WITH p
            MATCH (b:Book {isbn: $isbn})
            MERGE (b)-[:PUBLISHED_BY]->(p)
            """
            session.run(publisher_query, publisher=publisher, isbn=isbn)

        # Create Year node and relationship
        if year and year != 'Unknown':
            year_query = """
            MERGE (y:Year {year: $year})
            WITH y
            MATCH (b:Book {isbn: $isbn})
            MERGE (b)-[:PUBLISHED_IN]->(y)
            """
            session.run(year_query, year=year, isbn=isbn)

        # Create Language node and relationship
        if language and language != 'Unknown':
            language_query = """
            MERGE (l:Language {name: $language})
            WITH l
            MATCH (b:Book {isbn: $isbn})
            MERGE (b)-[:WRITTEN_IN]->(l)
            """
            session.run(language_query, language=language, isbn=isbn)

        # Process authors - FIXED to handle multiple authors correctly
        authors_raw = row.get('authors', '')
        if not pd.isna(authors_raw):
            # Parse authors properly
            if authors_raw.startswith('"') and authors_raw.endswith('"'):
                # This is a quoted string with multiple authors
                authors_content = authors_raw[1:-1]  # Remove outer quotes
            else:
                # Single author or already processed string
                authors_content = authors_raw

            # Split by comma and handle each author individually
            author_list = [a.strip() for a in authors_content.split(',')]

            for author in author_list:
                if author:  # Ensure we don't add empty authors
                    author_query = """
                    MERGE (a:Author {name: $author})
                    WITH a
                    MATCH (b:Book {isbn: $isbn})
                    MERGE (b)-[:WRITTEN_BY]->(a)
                    """
                    session.run(author_query, author=author, isbn=isbn)

        # Process genres/categories - FIXED to handle properly
        categories = row.get('category', '')
        if not pd.isna(categories):
            # Split categories by comma
            genre_list = [g.strip() for g in categories.split(',')]

            for genre in genre_list:
                if genre:  # Ensure we don't add empty genres
                    genre_query = """
                    MERGE (g:Genre {name: $genre})
                    WITH g
                    MATCH (b:Book {isbn: $isbn})
                    MERGE (b)-[:BELONGS_TO]->(g)
                    """
                    session.run(genre_query, genre=genre, isbn=isbn)


# Usage example
if __name__ == "__main__":
    # Replace with your Neo4j connection details
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"

    importer = Neo4jBooksImporter(uri, username, password)

    try:
        importer.import_books("bookstest_final_updated.csv")
    finally:
        importer.close()