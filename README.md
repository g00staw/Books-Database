# Dokumentacja projektu bazy grafowej książek w Neo4j

## Spis treści
1. [Wprowadzenie](#wprowadzenie)
2. [Struktura projektu](#struktura-projektu)
3. [Opis komponentów](#opis-komponentów)
   - [DBMerger.py - Łączenie danych](#dbmergerpy---łączenie-danych)
   - [DBScraper.py - Uzupełnianie danych](#dbscraperpy---uzupełnianie-danych)
   - [DBCreator.py - Tworzenie bazy grafowej](#dbcreatorpy---tworzenie-bazy-grafowej)
4. [Model bazy danych](#model-bazy-danych)
5. [Instrukcja uruchomienia](#instrukcja-uruchomienia)
6. [Wymagania systemowe](#wymagania-systemowe)
7. [Najczęstsze problemy](#najczęstsze-problemy)

## Wprowadzenie

Projekt ma na celu stworzenie bazy grafowej książek w Neo4j, wykorzystując dane pochodzące z różnych źródeł. Proces przebiega w trzech głównych etapach:
1. Łączenie danych z kilku źródeł CSV (DBMerger.py)
2. Uzupełnianie brakujących informacji poprzez scrapowanie danych z Goodreads (DBScraper.py) 
3. Import danych do bazy Neo4j (DBCreator.py)

## Struktura projektu

```
project/
│
├── databases/                   # Katalog z plikami źródłowymi i wynikowymi
│   ├── db1/                     # Pierwsza baza danych
│   │   ├── books.csv            # Dane książek
│   │   └── ratings.csv          # Oceny książek
│   ├── db2/                     # Druga baza danych
│   │   └── books.csv
│   ├── db3/                     # Trzecia baza danych
│   │   └── books.csv
│   ├── merge_warnings.log       # Plik z ostrzeżeniami z procesu łączenia
│   ├── merged_books_final.csv   # Finalny plik CSV po połączeniu zbiorów
│   └── bookstest_final_updated.csv # Plik po wzbogaceniu danymi ze scrapera
│
├── src/                         # Kod źródłowy
│   ├── DBMerger.py              # Skrypt do łączenia danych
│   ├── DBScraper.py             # Skrypt do scrapowania danych z Goodreads
│   └── DBCreator.py             # Skrypt do importu danych do Neo4j
│
└── README.md                    # Ten plik
```

## Opis komponentów

### DBMerger.py - Łączenie danych

Skrypt odpowiedzialny za połączenie danych z trzech różnych źródeł CSV i ujednolicenie ich formatu.

**Główne funkcje:**
- Wczytywanie danych z trzech różnych źródeł CSV
- Normalizacja nazw kolumn
- Ujednolicenie formatu autorów (zamiana różnych separatorów na jednolity format)
- Mapowanie kodów języków na pełne nazwy
- Usuwanie duplikatów na podstawie ISBN
- Uzupełnianie brakujących wartości
- Zapisywanie połączonych danych do pliku `merged_books_final.csv`

**Obsługa błędów:**
- Logowanie ostrzeżeń do pliku `merge_warnings.log`
- Bezpieczne wczytywanie plików CSV z różnymi kodowaniami
- Obsługa różnych formatów separatorów w pliku

### DBScraper.py - Uzupełnianie danych

Skrypt do uzupełniania brakujących informacji o książkach poprzez scrapowanie danych z serwisu Goodreads.

**Główne funkcje:**
- Wyszukiwanie książek na Goodreads na podstawie ISBN
- Pobieranie szczegółowych informacji:
  - Oceny Goodreads
  - Autorów
  - Kategorii/gatunków
  - Liczby stron
  - Daty publikacji
  - Wydawnictwa
  - Języka
- Uzupełnianie brakujących wartości w istniejących danych
- Obsługa pop-upów i dynamicznych elementów strony

**Wymagania techniczne:**
- Selenium WebDriver
- Chrome/ChromeDriver
- BeautifulSoup
- Pandas

### DBCreator.py - Tworzenie bazy grafowej

Skrypt odpowiedzialny za import danych do bazy Neo4j i utworzenie struktury grafowej.

**Główne funkcje:**
- Tworzenie ograniczeń unikalności dla węzłów (constraints)
- Import danych z pliku CSV
- Tworzenie różnych typów węzłów:
  - Book (Książka)
  - Author (Autor)
  - Publisher (Wydawca)
  - Language (Język)
  - Genre (Gatunek)
- Tworzenie relacji między węzłami:
  - WRITTEN_BY (Napisane przez)
  - PUBLISHED_BY (Wydane przez)
  - WRITTEN_IN (Napisane w języku)
  - BELONGS_TO (Należy do gatunku)

## Model bazy danych

### Węzły
- **Book**: zawiera informacje o książce (ISBN, tytuł)
- **Author**: zawiera informacje o autorze (imię i nazwisko)
- **Publisher**: zawiera informacje o wydawcy (nazwa)
- **Language**: zawiera język, w którym napisana jest książka
- **Genre**: zawiera gatunek/kategorię książki

### Relacje
- **WRITTEN_BY**: łączy książkę z autorem
- **PUBLISHED_BY**: łączy książkę z wydawcą
- **WRITTEN_IN**: łączy książkę z językiem
- **BELONGS_TO**: łączy książkę z gatunkiem

### Diagram modelu danych

```
                  (Author)
                     ^
                     |
                 WRITTEN_BY
                     |
                     v
(Genre) <--- BELONGS_TO --- (Book) --- PUBLISHED_BY ---> (Publisher)
                             |
                             |
                       WRITTEN_IN
                             |
                             v
                        (Language)
```

## Instrukcja uruchomienia

### 1. Przygotowanie środowiska
```bash
# Instalacja wymaganych pakietów
pip install pandas neo4j selenium beautifulsoup4 webdriver-manager requests

# Upewnij się, że masz zainstalowaną przeglądarkę Chrome
```

### 2. Uruchomienie Neo4j
```bash
# Uruchom instancję Neo4j (lokalnie lub w kontenerze Docker)
# Upewnij się, że serwer jest dostępny pod adresem neo4j://localhost:7687
```

### 3. Łączenie danych
```bash
# Przejdź do katalogu src
cd src

# Uruchom skrypt łączący dane
python DBMerger.py
```

### 4. Uzupełnianie danych (opcjonalnie)
```bash
# Uruchom skrypt scrapujący dane
python DBScraper.py
```

### 5. Import danych do Neo4j
```bash
# Uruchom skrypt importujący dane do Neo4j
python DBCreator.py
```

## Wymagania systemowe

- Python 3.7 lub nowszy
- Neo4j 4.x lub nowszy
- Przeglądarka Chrome (dla DBScraper.py)
- Biblioteki Python:
  - pandas
  - neo4j
  - selenium
  - beautifulsoup4
  - webdriver-manager
  - requests

## Źródła plików csv
 - db1/books.csv   (https://www.kaggle.com/datasets/saurabhbagchi/books-dataset)
 - db1/ratings.csv

 - db2/books.csv   (https://www.kaggle.com/datasets/dylanjcastillo/7k-books-with-metadata)

 - db3/books.csv   (https://www.kaggle.com/datasets/jealousleopard/goodreadsbooks)

## Najczęstsze problemy

1. **Problem z połączeniem do Neo4j**
   - Upewnij się, że serwer Neo4j jest uruchomiony
   - Sprawdź, czy podane dane logowania (username, password) są poprawne
   - Sprawdź, czy adres URI jest poprawny

2. **Problemy z scrapowaniem danych**
   - Upewnij się, że masz zainstalowaną przeglądarkę Chrome
   - Upewnij się, że twoja wersja ChromeDriver jest kompatybilna z wersją Chrome
   - Jeśli strona Goodreads zmieni swoją strukturę, konieczna może być aktualizacja selektorów CSS

3. **Problemy z formatem CSV**
   - Sprawdź, czy pliki CSV mają poprawne kodowanie (UTF-8, ISO-8859-1)
   - Sprawdź separatory używane w plikach CSV (przecinek, średnik)

4. **Błędy importu do Neo4j**
   - Sprawdź logi Neo4j pod kątem szczegółowych informacji o błędach
   - Upewnij się, że masz wystarczające uprawnienia do tworzenia ograniczeń i węzłów
