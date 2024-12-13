import os
import re  # Import the re module
import mysql.connector  # Import mysql.connector
from enum import Enum
from mysql.connector import Error
import json
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

class CRUD(Enum):
    CREATE = 1,
    READ = 2,
    UPDATE = 3,
    DELETE = 4, 
    ALTER = 5

class DBResponse:
    def __init__(self, status, row=None, rows=None):
        self.status = status
        if row is not None:
            self.row = row
        else:
            self.row = None  # return none
        if rows is not None:
            self.rows = rows
        else:
            self.rows = []  # return empty list

    def __str__(self):
        return json.dumps(self.__dict__)

class DB:
    db = None

    @staticmethod
    def __runQuery(op, isMany, queryString, args=None):
        response = None

        try:
            db = DB.getDB()
            cursor = db.cursor(dictionary=True)
            status = False
            if not isMany or op == CRUD.READ:
                if args is not None and len(args) > 0:
                    # convert dict for named placeholder mapping
                    if type(args[0]) is dict:
                        args = {k: v for d in args for k, v in d.items()}
                    status = cursor.execute(queryString, args)
                else:
                    status = cursor.execute(queryString)
            else:
                if args is not None and len(args) > 0:
                    status = cursor.executemany(queryString, args)
                else:
                    status = cursor.executemany(queryString)
            if op == CRUD.READ:
                if not isMany:
                    result = cursor.fetchone()
                    status = True if status is None else False
                    response = DBResponse(status, result)
                else:
                    result = cursor.fetchall()
                    status = True if status is None else False
                    response = DBResponse(status, None, result)
            else:
                status = True if status is None else False
                response = DBResponse(status)
            try:
                cursor.close()
            except Exception as ce:
                print("cursor close error", ce)

        except Error as e:
            if e.errno == -1:
                print("closing due to error")
                DB.close()
            # converting to a plain exception so other modules don't need to import mysql.connector.Error
            raise Exception(e)

        return response

    @staticmethod
    def delete(queryString, *args):
        return DB.__runQuery(CRUD.DELETE, False, queryString, args)

    @staticmethod
    def update(queryString, *args):
        return DB.__runQuery(CRUD.UPDATE, False, queryString, args)

    @staticmethod
    def query(queryString):
        if "CREATE TABLE" in queryString.upper():
            return DB.__runQuery(CRUD.CREATE, False, queryString)
        elif queryString.upper().startswith("ALTER"):
            return DB.__runQuery(CRUD.ALTER, False, queryString)
        elif queryString.upper().startswith("INSERT"):
            return DB.insertOne(queryString, )
        else:
            return DB.__runQuery(CRUD.ALTER, False, queryString)

    @staticmethod
    def insertMany(queryString, data):
        return DB.__runQuery(CRUD.CREATE, True, queryString, data)

    @staticmethod
    def insertOne(queryString, *args):
        return DB.__runQuery(CRUD.CREATE, False, queryString, args)

    @staticmethod
    def selectAll(queryString, *args):
        return DB.__runQuery(CRUD.READ, True, queryString, args)

    @staticmethod
    def selectOne(queryString, *args):
        return DB.__runQuery(CRUD.READ, False, queryString, args)

    @staticmethod
    def close():
        try:
            DB.db.close()
        except:
            pass
        DB.db = None

    @staticmethod
    def getDB():
        if DB.db is None or DB.db.is_connected() == False:
            if db_url is None:
                raise Exception("DB_URL is not set in the environment variables")

            data = re.findall("mysql:\/\/(\w+):(\w+)@([\w\.]+):([\d]+)\/([\w]+)", db_url)
            print(data)
            if len(data) > 0:
                data = data[0]
                if len(data) >= 5:
                    try:
                        user, password, host, port, database = data
                        DB.db = mysql.connector.connect(host=host, user=user, password=password, database=database, port=port,
                                                         connection_timeout=10)
                        DB.db.autocommit = True
                        print("Connected to the database successfully.")
                    except Error as e:
                        print("Error while connecting to MySQL", e)
                else:
                    raise Exception("Missing connection details")
            else:
                raise Exception("Invalid DB_URL format")
        return DB.db

if __name__ == "__main__":
    # Verifies connection works
    print(DB.selectOne("SELECT 'test' FROM dual"))
