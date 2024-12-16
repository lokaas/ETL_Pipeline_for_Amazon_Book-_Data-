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

## File Structure

```plaintext
.
├── dags
│   └── amazon_books_dag.py        # Airflow DAG for ETL pipeline
├── README.md                      # Project documentation
└── requirements.txt               # Project dependencies



---

## Requirements
Tools and Libraries
To run this project, ensure the following tools and libraries are installed:

Apache Airflow (2.x or later)
PostgreSQL
Python (3.8+)
Required Python Libraries:
pandas
requests
beautifulsoup4
apache-airflow
apache-airflow-providers-postgres

