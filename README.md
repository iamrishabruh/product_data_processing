# Product Data Processing Pipeline

This project processes raw product data by inserting it into a PostgreSQL database, splitting it into CSV deliverables, and uploading those deliverables to Google Sheets. Follow these simple steps to clone, configure, and run the project.

1. **Clone the Repository**  

   Open your terminal and run:
   ```bash
   git clone git@github.com:iamrishabruh/product_data_processing.git
   cd product_data_processing
   ```
   
3. **Set Up Your Python Environment**

   Create and activate a virtual environment:

   On macOS/Linux:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
   On Windows:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```
   
   Then install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure Environment Variables**

   Copy the sample environment file and update it with your credentials:
   ```bash
   cp .env.example .env
   ```

   Open .env and fill in the details (for example):
    ```bash
    PGHOST=localhost
    PGDATABASE=your_database_name
    PGUSER=your_username
    PGPASSWORD=your_password
    PGPORT=5432
    GOOGLE_SHEETS_CREDENTIALS=credentials.json
    (Make sure .env is added to .gitignore so your credentials aren’t pushed to GitHub.)
   ```

7. **Set Up PostgreSQL**

   macOS: Install PostgreSQL using Homebrew:
   ```bash
   brew install postgresql
   brew services start postgresql
   ```

9. **Create a Database**

   Open your terminal and run:
   ```bash
   psql -U postgres
   CREATE DATABASE your_database_name;
   \q
   (On Windows, install PostgreSQL from the official website and use pgAdmin or psql to create a database.)
   ```

11. **Set Up Google Sheets API**

   1) Go to the Google Cloud Console, create a new project, and enable the Google Sheets API.
   2) Create OAuth credentials (select "Desktop App"), download the JSON file, rename it to credentials.json, and place it in the project root.
   3) If your app is still in testing mode, add any test user emails as needed under the OAuth consent screen.
   4) Run the Pipeline

   With your environment configured, run:
   ```bash
   python main.py
   ```
   This command will process the raw CSV data (in raw_product_data.csv), update your PostgreSQL database, generate CSV deliverables, and upload each deliverable to a new Google Sheet.

11. **GitHub Actions (FUTURE ADDITION)**

   A GitHub Actions workflow will be provided in .github/workflows/ci.yml to run this pipeline automatically on pushes or pull requests. To use it, add your environment variables as secrets in your GitHub repository settings.
