# Import libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Headers for Amazon scraping to simulate a real browser request
headers = {
    "Referer": "https://www.amazon.com/",
    "Sec-Ch-Ua": '"Not A; Brand"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}

# Function to fetch book data from Amazon
def get_amazon_data_books(num_books, ti):
    base_url = "https://www.amazon.com/s?k=data+engineering+books"
    books = []  # List to store the fetched book data
    seen_titles = set()  # Set to store titles to avoid duplicates
    page = 1  # Start from page 1

    # Loop to scrape books until we fetch the requested number of books
    while len(books) < num_books:
        # Construct the URL with page number
        url = f"{base_url}&page={page}"

        # Send HTTP request to the URL
        response = requests.get(url, headers=headers)

        # If the request is successful, proceed with scraping
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all book containers on the page
            book_containers = soup.find_all("div", {"class": "s-result-item"})

            # Loop through each container and extract book details
            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("span", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                # Check if all required details are found
                if title and author and price and rating:
                    book_title = title.text.strip()

                    # Avoid duplicates
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip()
                        })
                        if len(books) >= num_books:
                            break
            # Move to the next page
            page += 1
        else:
            print("Failed to retrieve the page")
            break  # Exit the loop if the request fails

    # Limit to the required number of books
    books = books[:num_books]

    # Convert the list of books to DataFrame
    df = pd.DataFrame(books)

    # Drop duplicates based on Title
    df.drop_duplicates(subset="Title", inplace=True)

    # Push the cleaned data to XCom
    ti.xcom_push(key="book_data", value=df.to_dict("records"))

# Function to insert data into PostgreSQL
def insert_book_data_into_postgres(ti):
    # Pull book data from XCom
    book_data = ti.xcom_pull(key="book_data", task_ids="fetch_book_data")

    # Raise error if no data
    if not book_data:
        raise ValueError("No book data found")

    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="books_connection")

    # Insert Query
    insert_query = """
    INSERT INTO books (title, author, price, rating)
    VALUES (%s, %s, %s, %s)
    """

    # Insert each book into the table
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(
            book["Title"], book["Author"], book["Price"], book["Rating"]
        ))

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "fetch_and_store_amazon_books",
    default_args=default_args,
    description="A simple DAG to fetch book data from Amazon and store it in Postgres",
    schedule_interval=timedelta(days=1)
)

# Task 1: Fetch book data
fetch_book_data_task = PythonOperator(
    task_id="fetch_book_data",
    python_callable=get_amazon_data_books,
    op_args=[300],  # Fetch 300 books
    dag=dag
)

# Task 2: Create the 'books' table if it does not exist
create_table_task = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="books_connection",
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        author TEXT,
        price TEXT,
        rating TEXT
    )
    """,
    dag=dag
)

# Task 3: Insert the fetched data  into  the PostgreSQL data base
insert_book_data_task = PythonOperator(
    task_id="insert_book_data", #The task ID
    python_callable=insert_book_data_into_postgres, # The function execute
    dag=dag #DAG to which task belongs
)

# Define the  Task dependencies
#the fetch_book_data_task must run before the create_table_task , Which in turn runs before the insert _book_data_task
 fetch_book_data_task >> create_table_task >> insert_book_data_task
