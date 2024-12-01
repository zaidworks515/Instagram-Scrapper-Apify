from dotenv import load_dotenv
import os

env = load_dotenv()
port = os.getenv('PORT')
token = os.getenv('TOKEN')
cloudinary_secret_key = os.getenv('cloudinary_secret_key')
cloudinary_api_key = os.getenv('cloudinary_api_key')
cloudinary_name = os.getenv('cloudinary_name')


# host = os.getenv('HOST')
# user = os.getenv('USER')
# password = os.getenv('PASSWORD')
# database = os.getenv('DATABASE')
