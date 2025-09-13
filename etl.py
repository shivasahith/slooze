import pandas as pd
import re

INPUT_FILE = "data/products.csv"
OUTPUT_FILE = "data/products_clean.csv"

def extract():
    """Read raw products CSV."""
    return pd.read_csv(INPUT_FILE)

def transform(df):
    """Clean and standardize data."""
    print("\n🔹 Starting Transformation...")

    # Original stats
    print(f"📊 Original Rows: {len(df)}")
    print(f"📊 Original Columns: {list(df.columns)}")

    # Strip whitespace
    for col in ["product_name", "product_url", "seller_name", "location", "price_text"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Standardize location
    if "location" in df.columns:
        df["location"] = df["location"].str.title().replace({"Nan": None})

    # Convert price_text -> price_value
    def parse_price(text):
        if not text or text.lower() in ["nan", "none"]:
            return None
        digits = re.findall(r"[\d,]+", text)
        if not digits:
            return None
        try:
            return float(digits[0].replace(",", ""))
        except:
            return None

    if "price_text" in df.columns:
        df["price_value"] = df["price_text"].apply(parse_price)

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["product_url"])
    after = len(df)
    print(f"🧹 Removed {before - after} duplicate rows")

    # Missing value summary
    print("\n📉 Missing Values:")
    print(df.isna().sum())

    return df

def load(df):
    """Save cleaned data to CSV."""
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Cleaned data saved to {OUTPUT_FILE}")
    print(f"📊 Final Rows: {len(df)} | Columns: {list(df.columns)}")

if __name__ == "__main__":
    df_raw = extract()
    df_clean = transform(df_raw)
    load(df_clean)
