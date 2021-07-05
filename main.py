import asyncio
from data_scraper.scrape import get_json_response, get_json_response_from_test_file
from database.mongo_client import MongoClient
from fastapi import FastAPI
from decouple import config
from services.onespan import OnespanManager

app = FastAPI()
mongo_client = MongoClient('localhost', 27017)


async def get_spendings():
    return await get_json_response_from_test_file()


@app.get('/get-entities')
async def entities():
    await mongo_client.save_items(get_json_response_from_test_file())
    return await mongo_client.get_spending_entities()


@app.post('/create-segments')
async def segments():
    onespan = OnespanManager()
    segments = await mongo_client.get_spending_entities()
    await onespan.register_number(segments)
    return [{"message": True}]


# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()

#     # spendings_items = loop.run_until_complete(get_json_response())
#     spendings_items = loop.run_until_complete(get_json_response_from_test_file())

#     mongo_client = MongoClient('localhost', 27017)
#     loop.run_until_complete(mongo_client.save_items(spendings_items))
