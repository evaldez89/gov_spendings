import asyncio
from data_scraper.scrape import get_json_response, get_json_response_from_test_file
from database.mongo_client import MongoClient
from fastapi import FastAPI


app = FastAPI()
mongo_client = MongoClient('localhost', 27017)


async def get_spendings():
    return await get_json_response_from_test_file()


@app.get('/get_entities')
async def entities():
    return await mongo_client.get_spending_entities()


# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()

#     # spendings_items = loop.run_until_complete(get_json_response())
#     spendings_items = loop.run_until_complete(get_json_response_from_test_file())

#     mongo_client = MongoClient('localhost', 27017)
#     loop.run_until_complete(mongo_client.save_items(spendings_items))
