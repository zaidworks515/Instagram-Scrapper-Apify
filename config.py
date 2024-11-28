from dotenv import load_dotenv
import os

env = load_dotenv()
port = os.getenv('PORT')
token = os.getenv('TOKEN')

# host = os.getenv('HOST')
# user = os.getenv('USER')
# password = os.getenv('PASSWORD')
# database = os.getenv('DATABASE')
