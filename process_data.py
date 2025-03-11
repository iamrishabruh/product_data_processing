# process_data.py
import pandas as pd
import re

def extract_size_tokens(text):
    """
    Given a string, splits it into tokens and separates out 'size' tokens
    based on common patterns (numeric units, pack of X, etc.).
    Returns (cleaned_text, extracted_size).

    1) Splits the string by whitespace.
    2) Merges multi-word tokens like "Pack of 2" into one token (e.g. "Pack_of_2")
       before matching.
    3) Uses a regex to detect tokens that look like sizes.
    4) Moves matched tokens into 'extracted_size' and leaves the rest in 'cleaned_text'.
    """
    if not text:
        return "", ""
    
    # 1) Split into tokens
    raw_tokens = text.split()
    
    # 2) Merge multi-word sequences (e.g., "Pack of 2")
    merged_tokens = []
    skip_next = False
    i = 0
    while i < len(raw_tokens):
        if skip_next:
            skip_next = False
            i += 1
            continue
        
        token = raw_tokens[i]
        lower_token = token.lower()
        
        if lower_token in ["pack", "case", "set", "box"] and (i + 2) < len(raw_tokens):
            next_token = raw_tokens[i+1].lower()
            after_next = raw_tokens[i+2]
            if next_token == "of" and re.match(r"^\d+(\.\d+)?$", after_next):
                merged_tokens.append(f"{token}_{next_token}_{after_next}")
                i += 3
                continue
            else:
                merged_tokens.append(token)
        else:
            merged_tokens.append(token)
        i += 1
    
    # 3) Regex for size tokens:
    size_pattern = re.compile(r"""
        ^(?:
            \d+(\.\d+)?\s*
            (?:oz\.?|lb\.?|g|grams?|kg|gm|ml|mL|L|liter|litre|fl\.?\s*oz\.?|syringes|shots|bottles?|count|ct|pcs?|pieces?|set|kit|jar|trays?){1}
            (?:\s*\(.*?\))?  |
            (?:pack|case|set|box)_of_\d+  |
            x\s*\d+(\.\d+)?  |
            \d+(\.\d+)?\s*x
        )$
    """, re.IGNORECASE | re.VERBOSE)
    
    kept_tokens = []
    size_tokens = []
    
    for token in merged_tokens:
        if size_pattern.match(token.strip()):
            size_tokens.append(token)
        else:
            kept_tokens.append(token)
    
    return " ".join(kept_tokens), " ".join(size_tokens)

def process_raw_data(raw_csv_path):
    """
    Reads the raw CSV, transforms it, and returns a final DataFrame with columns:
    product_id, upc_ean, brand, product_name, category, subcategory, size.
    
    If the raw data includes a 'size' column, its value is appended verbatim
    to the extracted size tokens from the title. If blank, only the extracted size is used.
    """
    # Load the raw CSV data
    df = pd.read_csv(raw_csv_path)
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Check required columns exist
    needed = ['barcode_number', 'barcode_formats', 'title', 'category']
    missing = set(needed) - set(df.columns)
    if missing:
        raise KeyError(f"The following required columns are missing: {missing}")
    
    # Filter rows: keep only rows with category matching beauty/personal care keywords
    allowed_keywords = ['beauty', 'personal care', 'cosmetics', 'skincare']
    df_filtered = df[df['category'].astype(str).str.lower().apply(
        lambda x: any(keyword in x for keyword in allowed_keywords)
    )].copy()
    
    # =============== TRANSFORMATIONS ===============
    # 1) product_id = barcode_number (verbatim)
    df_filtered['product_id'] = df_filtered['barcode_number'].astype(str)
    
    # 2) upc_ean = barcode_formats (verbatim)
    df_filtered['upc_ean'] = df_filtered['barcode_formats'].astype(str)
    
    # 3) Split title into brand (first two words) and product_name (remaining words)
    def split_title_into_brand_productname(title):
        if not isinstance(title, str) or not title.strip():
            return "", ""
        words = title.split()
        first_two = words[:2]
        rest = words[2:]
        return " ".join(first_two), " ".join(rest)
    
    df_filtered[['brand', 'product_name']] = df_filtered['title'].apply(
        lambda x: pd.Series(split_title_into_brand_productname(x))
    )
    
    # 4) Extract size tokens from brand and product_name, then combine with raw size if available.
    def extract_size_from_brand_product_and_raw(row):
        # Extract size tokens from the title-split fields:
        cleaned_brand, size_brand = extract_size_tokens(row['brand'])
        cleaned_product, size_product = extract_size_tokens(row['product_name'])
        extracted_size = " ".join(filter(None, [size_brand, size_product])).strip()
        
        # Get raw size from the raw CSV column if it exists
        raw_size = ""
        if 'size' in row:
            raw_size = str(row['size']).strip() if pd.notna(row['size']) else ""
        
        if raw_size:
            combined_size = raw_size + (" " + extracted_size if extracted_size else "")
        else:
            combined_size = extracted_size
        
        return cleaned_brand, cleaned_product, combined_size
    
    df_filtered[['brand', 'product_name', 'size']] = df_filtered.apply(
        lambda row: pd.Series(extract_size_from_brand_product_and_raw(row)),
        axis=1
    )
    
    # 5) Split category into category (first three segments) and subcategory (the rest)
    def split_category(cat):
        if not isinstance(cat, str):
            return "", ""
        segments = [c.strip() for c in cat.split('>')]
        if len(segments) <= 3:
            return " > ".join(segments), ""
        else:
            return " > ".join(segments[:3]), " > ".join(segments[3:])
    
    df_filtered[['category_temp', 'subcategory']] = df_filtered['category'].apply(
        lambda x: pd.Series(split_category(x))
    )
    df_filtered['category'] = df_filtered['category_temp']
    df_filtered.drop(columns=['category_temp'], inplace=True)
    
    # =============== FINAL COLUMNS ===============
    final_columns = [
        'product_id',
        'upc_ean',
        'brand',
        'product_name',
        'category',
        'subcategory',
        'size'
    ]
    
    for col in final_columns:
        if col not in df_filtered.columns:
            df_filtered[col] = ""
    
    df_final = df_filtered[final_columns]
    return df_final

def split_into_chunks(df, chunk_size=1000):
    """
    Splits the DataFrame into chunks of up to chunk_size rows.
    """
    num_chunks = (len(df) + chunk_size - 1) // chunk_size
    return [df.iloc[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]
