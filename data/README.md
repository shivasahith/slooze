It collects product listings from [IndiaMART](https://dir.indiamart.com/), processes the raw data into a structured format, and performs **exploratory data analysis (EDA)**.

**Project Structure**
slooze/
 - crawler.py # Web crawler to scrape product listings
 - etl.py # ETL pipeline (cleaning & transformation)
 - eda.ipynb # Jupyter notebook with data analysis & visualizations
 - requirements.txt # Python dependencies
 - README.md # Project documentation
 - data/
     - products.csv # Raw collected data
     - products_clean.csv # Cleaned dataset
     - page_<keyword>_<n>.html # Saved raw HTML pages

**How to Run**
create a virtual environment
activate it
Install dependencies (requirements.txt)
Run Crawler
Run ETL
Run EDA

