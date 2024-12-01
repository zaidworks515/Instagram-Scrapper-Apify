import os
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from apify_client import ApifyClient
from config import port, token, cloudinary_secret_key, cloudinary_api_key, cloudinary_name
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from db_queries import insert_data, update_status, check_status
import uuid
import asyncio
import logging
import time
import cloudinary
import cloudinary.uploader


app = FastAPI()

logging.basicConfig(level=logging.ERROR)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



def data_scrapping(session_id, token, result_limit=None, hashtag1=None, hashtag2=None, hashtag3=None, min_followers=None):

    cloudinary_url = None

    client = ApifyClient(token)
    run_input = None
    if hashtag1:
        run_input = {
            "hashtags": [hashtag1],
            "resultsLimit": result_limit,
        }
    elif hashtag1 and hashtag2:
        run_input = {
            "hashtags": [hashtag1, hashtag2],
            "resultsLimit": result_limit,
        }
    elif hashtag1 and hashtag2 and hashtag3:
        run_input = {
            "hashtags": [hashtag1, hashtag2, hashtag3],
            "resultsLimit": result_limit,
        }
    
    run = client.actor("reGe1ST3OBgYZSsZJ").call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    columns_list = list(items[0].keys())
    dataframe = pd.DataFrame(items)
    dataframe = dataframe.reindex(columns=columns_list, fill_value=None)


    user_names = preprocessing_dataframe1(dataframe, session_id)
    if user_names:
        profiles_data = fetch_profiles(session_id, token, user_names, min_followers) #scrapper 2 (only for profiles)
        print('profiles data extracted successfully.....')

        max_tries = 5
        while max_tries > 0:
            try:
                """TO UPLOAD THE DATAFRAME INTO CLOUDINARY"""
                cloudinary.config( 
                cloud_name = cloudinary_name, 
                api_key = cloudinary_api_key, 
                api_secret = cloudinary_secret_key, secure=True)

                upload_result = cloudinary.uploader.upload(f"scrapped data/{session_id}.csv",
                                                public_id=session_id, resource_type="raw",
                                                timeout=120000)
                if upload_result:
                    max_tries = 0
            except Exception as e:
                print(f"try no {max_tries} failed.. retrying")
            max_tries -= 1
            time.sleep(1)
        
        if upload_result:
            cloudinary_url = upload_result['url']

        update_status(session_id=session_id, new_status=1, cloudinary_url=cloudinary_url)
        print('status updated to 1', session_id)


        file_path = f'scrapped data/{session_id}.csv'

        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File {file_path} has been removed successfully.")
        else:
            print(f"File {file_path} not found.")

        return profiles_data
    else:
        print('Sorry, No profile data is made..')
        return None


def preprocessing_dataframe1(dataframe, session_id):
    try:
        file_path = f'scrapped data/{session_id}.csv'

        if 'ownerUsername' not in dataframe.columns:
            raise ValueError("The input DataFrame does not contain the required 'ownerUsername' column.")

        if os.path.exists(file_path):
            existed_dataframe = pd.read_csv(file_path)
            merged_data = pd.concat([existed_dataframe, dataframe], ignore_index=True)
        else:
            merged_data = dataframe

        merged_data = merged_data.drop_duplicates(subset='ownerUsername', keep='first')
        user_names = merged_data['ownerUsername'].to_list()

        if not user_names:
            logging.warning("No unique usernames found after preprocessing.")
            return []

        return user_names

    except Exception as e:
        logging.error(f"Error in preprocessing_dataframe1: {str(e)}")
        raise


def fetch_profiles(session_id, token, user_names, min_followers):
    print('executing fetch_profiles')
    try:
        client = ApifyClient(token)
        run_input = { "usernames": user_names}
        run = client.actor("dSCLg0C3YEZ83HzYX").call(run_input=run_input)

        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if items:
            columns_list = list(items[0].keys())

            dataframe = pd.DataFrame(items)
            dataframe = dataframe.reindex(columns=columns_list, fill_value=None) 
            dataframe = dataframe[['username', 'fullName', 'biography', 'externalUrl', 'followersCount', 'followsCount', 'hasChannel',
                                'highlightReelCount', 'isBusinessAccount', 'joinedRecently', 'businessCategoryName', 
                                'private', 'verified', 'postsCount']]

            dataframe = dataframe.drop_duplicates(keep='first')
            dataframe = dataframe[dataframe['followersCount'] >= min_followers]

            dataframe.to_csv(f'scrapped data/{session_id}.csv', index=False)
        return dataframe
    
    except Exception as e:
        return e


@app.post("/scrape_data")
async def scrape_data(hashtag1: str, hashtag2: str = None, 
                      hashtag3: str = None, result_limit: int = 10, 
                      min_followers: int = None, 
                      background_tasks: BackgroundTasks = None):

    try:
        session_id = str(uuid.uuid4())
        file_path = f'scrapped data/{session_id}.csv'
        response_message = 'Data is being scrapped, and path will be saved in DB.'
        
        insert_data(session_id=session_id, file_path=file_path, creation_status=0)

        # with ThreadPoolExecutor(max_workers=20) as executor:
        #     executor.submit(data_scrapping, session_id, token, result_limit, hashtag1, hashtag2, hashtag3, min_followers)

        background_tasks.add_task(data_scrapping, session_id, token, result_limit, hashtag1, hashtag2, hashtag3, min_followers)

        return JSONResponse(content={'message': response_message, 'session_id': session_id}, status_code=200)

    except Exception as e:
        logging.error(f"Error in scrapping data: {str(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred")


@app.post("/get_data")
async def get_data(session_id: str):
    try:
        start_time = time.time()
        timeout = 10  # Timeout value in seconds

        while True:
            if time.time() - start_time > timeout:
                raise HTTPException(status_code=408, detail="Timeout waiting for data to be ready")

            # Use asyncio to run the blocking task (check_status) in a separate thread via the executor
            loop = asyncio.get_running_loop()
            status = await loop.run_in_executor(None, check_status, session_id)

            if status['creation_status'] == 1:
                try:
                    cloudinary_url = status['cloudinary_url']
                    return JSONResponse(content={'cloudinary_url': cloudinary_url}, status_code=200)
                except FileNotFoundError:
                    logging.error(f"File for session {session_id} not found.")
                    return JSONResponse(content={'cloudinary_url': 'not found'}, status_code=404)
                except Exception as e:
                    logging.error(f"Error reading file for session {session_id}: {str(e)}")
                    return JSONResponse(content={'cloudinary_url': 'error reading file'}, status_code=500)

            await asyncio.sleep(2)  # Sleep before trying again

    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred")


@app.on_event("shutdown")
async def shutdown_event():
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.shutdown(wait=True)


if __name__ == "__main__":
    import uvicorn
    try:
        port = int(port)
        uvicorn.run("app:app", host='0.0.0.0', port=port, reload=True, log_level="debug")
    except Exception as e:
        logging.error(f"An error occurred when starting the app: {str(e)}")
        print(f"An error occurred: {str(e)}")