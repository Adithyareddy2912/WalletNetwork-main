import os
import re  # Import the re module
import mysql.connector  # Import mysql.connector
from dotenv import load_dotenv

# Manually specify the path to .env file
dotenv_path = r"C:\Users\Adithya Reddy\Downloads\WalletNetwork-main\WalletNetwork-main\url.env"

# Load the .env file
if load_dotenv(dotenv_path=dotenv_path):
    print(f".env file loaded successfully from: {dotenv_path}")
else:
    print(f"Failed to load .env file from: {dotenv_path}")

# Retrieve DB_URL from environment variables
db_url = os.getenv('DB_URL')

# Debugging: Print DB_URL to ensure it's loaded correctly
print(f"DB_URL: {db_url}")


# Your existing database connection logic
class DB:
    db = None

    @staticmethod
    def getDB():
        if DB.db is None or not DB.db.is_connected():
            if db_url is None:
                raise Exception("DB_URL is not set in the environment variables")

            # Proceed with the rest of the code
            data = re.findall("mysql:\/\/(\w+):(\w+)@([\w\.]+):([\d]+)\/([\w]+)", db_url)

            if len(data) > 0:
                user, password, host, port, database = data[0]
                try:
                    print("Connecting to the database...")
                    DB.db = mysql.connector.connect(
                        host=host,
                        user=user,
                        password=password,
                        database=database,
                        port=port,
                        connection_timeout=10
                    )
                    DB.db.autocommit = True
                    print("Connected to the database successfully.")
                except mysql.connector.Error as e:
                    print(f"Error while connecting to MySQL: {e}")
            else:
                raise Exception("Invalid DB_URL format")
        return DB.db


# Testing DB connection
if __name__ == "__main__":
    try:
        db = DB.getDB()
        cursor = db.cursor()
        cursor.execute("SELECT 'test' AS test_column")
        result = cursor.fetchone()
        print(f"Query result: {result}")
        cursor.close()
    except Exception as e:
        print(f"Error: {e}")
