**Slooze ETL & EDA Project**

It collects product listings from [IndiaMART](https://dir.indiamart.com/), processes the raw data into a structured format, and performs **Exploratory data analysis (EDA)**.

**Project Structure**

slooze/

 |- crawler.py # Web crawler to scrape product listings
 
 |- etl.py # ETL pipeline (cleaning & transformation)
 
 |- eda.ipynb # Jupyter notebook with data analysis & visualizations
 
 |- requirements.txt # Python dependencies
 
 |- README.md # Project documentation
 
 |- data/
 
     |- products.csv # Raw collected data
     
     |- products_clean.csv # Cleaned dataset
     
     |- page_<keyword>_<n>.html # Saved raw HTML pages
     

**How to Run**

1. Create a Virtual Environment

2. Activate it

3. Install dependencies (requirements.txt)

4. Run Crawler

5. Run ETL

6. Run EDA(run all cells)
