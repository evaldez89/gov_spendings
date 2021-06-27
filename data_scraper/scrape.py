from httpx import AsyncClient
from bs4 import BeautifulSoup

from .data_parser import to_json


URL = 'https://comunidad.comprasdominicana.gob.do/Public/Tendering/ContractNoticeManagement/Index?currentLanguage=en'
REQUEST_TIMEOUT = 60.0


async def get_response():
    content = False
    async with AsyncClient() as client:
        response = await client.get(URL, timeout=REQUEST_TIMEOUT)
        content = BeautifulSoup(response.text, 'html.parser')
    return content


async def get_json_response():
    content = await get_response()
    return to_json(content)


async def get_json_response_from_test_file():
    content = None
    with open("tests_files/Buscar proceso.html") as fp:
        content = BeautifulSoup(fp, 'html.parser')
    return to_json(content)
