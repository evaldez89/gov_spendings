from onesignal_sdk.client import AsyncClient
from dataclasses import dataclass, field
from decouple import config


@dataclass
class OnespanManager():
    client: AsyncClient = field(init=False)

    def __post_init__(self):
        self.client = AsyncClient(app_id=config('ONESIGNAL_APP_ID'),
                                  rest_api_key=config('ONESIGNAL_RESTAPI_KEY'),
                                  user_auth_key=config('ONESIGNAL_USER_AUTH_KEY'))

    async def register_number(self, segments: list):
        for segment in segments:
            body = {
                'device_type': 14,
                'identifier': '18093409836',
            }

            self.client.add_device()
