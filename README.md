# ETL_Pipeline_for_Amazon_Book-_Data-
# Amazon Book Data ETL Pipeline using Airflow

### Key Features:
- **Web Scraping**: Extracts book details such as title, author, price, and rating from Amazon.
- **Data Cleaning**: Ensures data integrity by removing duplicates.
- **Data Storage**: Loads the clean data into a PostgreSQL database.
- **Automation**: The pipeline is orchestrated using Apache Airflow and scheduled to run daily.

---

## Project Architecture

1. **Apache Airflow**:
   - Manages the workflow (DAG) and schedules tasks.
   - Includes PythonOperator and PostgresOperator for data processing and database interaction.

2. **Web Scraping**:
   - Uses `requests` and `BeautifulSoup` to scrape Amazon book data.

3. **PostgreSQL**:
   - Stores the scraped book data in a structured table.

4. **Python**:
   - Custom Python functions are used for data extraction, cleaning, and insertion into the database.

---





