from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from apify_client import ApifyClient
from config import port, token
from concurrent.futures import ThreadPoolExecutor
import logging
import pandas as pd


class DataRequest(BaseModel):
    hashtag1: str  
    hashtag2: str  
    hashtag3: str  
    

app = FastAPI()

logging.basicConfig(level=logging.ERROR)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


executor = ThreadPoolExecutor(max_workers=10)  


def data_scrapping(token, hashtag1=None, hashtag2=None, hashtag3=None):
    client = ApifyClient(token)
    run_input = None
    if hashtag1:
        run_input = {
        "hashtags": [hashtag1],
        "resultsLimit": 5,
        }
    elif hashtag1 and hashtag2:
        run_input = {
        "hashtags": [hashtag1,hashtag2],
        "resultsLimit": 10,
        }
    elif hashtag1 and hashtag2 and hashtag3:
        run_input = {
        "hashtags": [hashtag1,hashtag2,hashtag3],
        "resultsLimit": 10,
        }
    
    
    run = client.actor("reGe1ST3OBgYZSsZJ").call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    columns_list = list(items[0].keys())

    dataframe = pd.DataFrame(items)
    dataframe = dataframe.reindex(columns=columns_list, fill_value=None)


    return dataframe

def preprocessing_dataframe(dataframe):
    try:
        existed_dataframe = pd.read_csv('scrapped data/data.csv')
        merged_data = pd.concat([existed_dataframe, dataframe], ignore_index=True)
        merged_data.drop_duplicates(inplace=True)
        merged_data.to_csv('scrapped data/data.csv', index=False)
    except:
        new_dataframe = dataframe
        new_dataframe.to_csv('scrapped data/data.csv', index=False)
    
    
@app.post("/scrape_data")
async def scrape_data(request: DataRequest):
    try:
        hashtag1 = request.hashtag1
        hashtag2 = request.hashtag2
        hashtag3 = request.hashtag3
        
        response_message = 'data is being scrapped and will be saved in db'
    
        scrapped_dataframe = executor.submit(data_scrapping, token, hashtag1, hashtag2, hashtag3)  
        
        # yhaa'n db mein image_path save krwana hai. if 'executor'
        preprocessing_dataframe(scrapped_dataframe) # currently saving as csv

        return JSONResponse(content={'status': True, 'message': response_message}, status_code=200)
        
    
    except Exception as e:
        logging.error(f"Error in face_swap: {str(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred")
    

if __name__ == "__main__":
    import uvicorn
    try:
        port = int(port)
        uvicorn.run("app:app", host='0.0.0.0', port=port, reload=True, log_level="debug")
    except Exception as e:
        logging.error(f"An error occurred when starting the app: {str(e)}")
        print(f"An error occurred: {str(e)}")