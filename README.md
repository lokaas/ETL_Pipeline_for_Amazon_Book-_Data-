# ETL_Pipeline_for_Amazon_Book-_Data-
# Data Engineering Books Scraper

## Project Overview
This project is designed to collect book data from Amazon, specifically targeting the "Data Engineering Books" category, using web scraping techniques and ETL (Extract, Transform, Load) processes. The scraped data, which includes details such as book title, author, price, and rating, is then loaded into a PostgreSQL database for further analysis. The system is automated and periodically executed using Apache Airflow, ensuring efficient and regular data collection.

## Technologies and Tools

### 1. **Apache Airflow**
- **Role**: Orchestrates the workflow (DAG) of extracting, transforming, and loading data.
- **Usage**: 
  - Schedules the periodic execution of tasks.
  - Provides powerful mechanisms for monitoring, logging, and error handling.

### 2. **Web Scraping (BeautifulSoup and Requests)**
- **Role**: Collects data from Amazon search result pages.
- **Usage**:
  - **BeautifulSoup**: Parses the HTML structure to extract book details.
  - **Requests**: Sends HTTP requests to retrieve the web pages for scraping.

### 3. **PostgreSQL**
- **Role**: Stores the collected book data in a structured format.
- **Usage**:
  - Stores book information such as title, author, price, and rating.
  - **PostgresOperator** in Airflow interacts with the database to load the data.

### 4. **Pandas**
- **Role**: Cleans and transforms the scraped data.
- **Usage**:
  - Removes duplicates.
  - Ensures data consistency and the required format before loading it into the database.

## Project Structure

### 1. **Airflow DAG**
The project follows an ETL process defined by a Directed Acyclic Graph (DAG) in Airflow, which consists of three main tasks:

#### **Extract**
- Scrapes book data from Amazon for a specified category ("Data Engineering Books").
- Extracts details such as:
  - Title
  - Author
  - Price
  - Rating
- Extracts data from multiple pages to gather a specified number of books (e.g., 300 books).

#### **Transform**
- After extraction, the data is cleaned and transformed using Pandas:
  - Removes duplicate entries.
  - Ensures consistency in the data format (e.g., correct price formatting).

#### **Load**
- Loads the cleaned and transformed data into a PostgreSQL database.
- Uses the **PostgresOperator** in Airflow to execute database insertions.

## Setup Instructions

### Prerequisites
- Python 3.x
- PostgreSQL
- Apache Airflow



