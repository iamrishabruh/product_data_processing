# main.py
import os
from dotenv import load_dotenv
from process_data import process_raw_data, split_into_chunks
import db
import sheets

def main():
    load_dotenv()
    
    # Path to your raw CSV file
    raw_csv_path = 'raw_product_data.csv'  # Update this path if necessary
    
    print("Processing raw data...")
    df_final = process_raw_data(raw_csv_path)
    print(f"Processed data with {len(df_final)} rows.")
    
    # Create PostgreSQL table and insert processed data
    print("Creating database table (if not exists)...")
    db.create_table()
    print("Inserting processed data into database...")
    db.insert_data(df_final)
    
    # Split processed data into chunks and generate deliverables.
    # For testing, set max_deliverables = 1; when ready, set to None to process all chunks.
    chunk_size = 1000
    max_deliverables = 1  # Change to None for full processing
    chunks = split_into_chunks(df_final, chunk_size)
    
    for idx, chunk in enumerate(chunks, start=1):
        if max_deliverables is not None and idx > max_deliverables:
            break
        csv_filename = f'deliverable_{idx}.csv'
        chunk.to_csv(csv_filename, index=False)
        print(f"Saved CSV deliverable: {csv_filename} with {len(chunk)} rows.")
        
        # Upload each chunk to a new Google Sheet
        sheet_title = f"Deliverable {idx}"
        sheet_id = sheets.upload_chunk_to_sheet(chunk, sheet_title)
    
if __name__ == "__main__":
    main()
