import logging
from dataclasses import dataclass, field
from typing import List, NoReturn

from motor.motor_asyncio import (AsyncIOMotorClient, AsyncIOMotorCollection,
                                 AsyncIOMotorDatabase)

logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')
NOTIFICATIONS_COLLECTION_NAME = 'notifications'
SPENDINGS_DB_NAME = 'spendings'
SPENDINGS_COLLECTION_NAME = 'items'


@dataclass
class MongoClient():
    host: str
    port: int
    client: AsyncIOMotorClient = field(init=False)

    def __post_init__(self):
        self.client = AsyncIOMotorClient(f'mongodb://{self.host}:{self.port}')

    def get_database(self, databse_name: str) -> AsyncIOMotorDatabase:
        return self.client[databse_name]

    def get_collection(self, database_name: str, collection_name: str) -> AsyncIOMotorCollection:
        return self.client[database_name][collection_name]

    @staticmethod
    def _split_into_chunks(differences: list, chunk_size: int = 2):
        for index in range(0, len(differences), chunk_size):
            yield differences[index:index + chunk_size]

    async def send_notifications(self, notification: dict):
        message = {
            'title': f'Cambio en {notification.get("ContractingAuthority")}',
            'body': f"Valor para '{notification.get('FieldName')}' " +
                    f"cambió de '{notification.get('OldValue')}' a '{notification.get('NewValue')}'"
        }

        self.get_collection(SPENDINGS_DB_NAME, NOTIFICATIONS_COLLECTION_NAME).update_one(
            {'_id': notification.get('_id')}, {'$set': {'IsSent': True}}
        )

    def create_notification(self, item: dict, differences: set):
        notifications = []
        for diff in self._split_into_chunks(sorted(differences)):
            notifications += [{
                "Reference": item.get('Reference'),
                "ContractingAuthority": item.get('ContractingAuthority'),
                "FieldName": value[0],
                "NewValue": value[1],
                "OldValue": item.get(value[0]),
                "IsSent": False
                } for value in diff if value[1] != item.get(value[0])]

        self.get_collection(SPENDINGS_DB_NAME, NOTIFICATIONS_COLLECTION_NAME).insert_many(notifications)

    async def create_updates_notifications(self, collection: AsyncIOMotorCollection, items: list) -> NoReturn:
        for db_item in await collection.find({}).to_list(length=None):

            db_item_id = db_item.pop('_id', None)
            item: dict = next(filter(lambda i: i.get('Reference') == db_item.get('Reference'), items), None)

            if item != db_item:
                logging.info(f"{db_item_id} Item with reference {item.get('Reference')} has changed")
                differences = item.items() ^ db_item.items()
                logging.info(f"Differences:\n{differences}")

                self.create_notification(db_item, differences)

    async def get_items_to_insert(self, collection: AsyncIOMotorCollection, items: list) -> List[dict]:
        if await collection.count_documents({}) == 0:
            return items

        group_pipeline = [
            {
                "$group": {
                    "_id": "$Reference"
                    }
            }
        ]

        item_references = await collection.aggregate(group_pipeline).to_list(length=None)
        references = [ref.get('_id') for ref in item_references]

        return [item for item in items if item.get('Reference') not in references]

    async def get_spending_entities(self):
        collection = self.get_collection(SPENDINGS_DB_NAME, SPENDINGS_COLLECTION_NAME)
        group_pipeline = [
            {
                "$group": {
                    "_id": "$ContractingAuthority",
                    "ActiveSpendings": {"$sum": 1}
                    }
            },
            {
                "$project": {
                    "ContractingAuthority": "$_id",
                    "ActiveSpendings": 1,
                    "_id": 0
                }
            }
        ]

        return await collection.aggregate(group_pipeline).to_list(length=None)

    async def save_items(self, incomming_items: list):
        collection = self.get_collection(SPENDINGS_DB_NAME, SPENDINGS_COLLECTION_NAME)

        items_to_insert = await self.get_items_to_insert(collection, incomming_items)

        collection.insert_many(items_to_insert)

        await self.create_updates_notifications(collection, incomming_items)

        notification_collection = self.get_collection(SPENDINGS_DB_NAME, NOTIFICATIONS_COLLECTION_NAME)
        pending_notifications = notification_collection.find({"IsSent": {"$ne": True}})
        for item_notification in await pending_notifications.to_list(length=None):
            # Notify
            await self.send_notifications(item_notification)

            # Update

            self.get_collection(SPENDINGS_DB_NAME, SPENDINGS_COLLECTION_NAME).update_one(
                {'Reference': item_notification.get('Reference')},
                {
                    '$set': {
                        item_notification.get('FieldName'): item_notification.get('NewValue')
                    }
                }
            )
