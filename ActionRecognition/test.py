import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import datetime
import pytz

async def fetch(session, url, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    upload_date = soup.select_one("meta[itemprop='uploadDate']")
                    return upload_date["content"] if upload_date else 'Video not found'
                else:
                    print(f"Failed to retrieve {url}, status code: {response.status}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        return 'Failed to retrieve the page'
        except Exception as e:
            print(f"Exception occurred while fetching {url}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                return 'Failed to retrieve the page'

async def get_upload_dates(urls):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)

def str_to_datetime(str):
    if str == 'Video not found' or str == 'Failed to retrieve the page':
        return 'Video not found'

    else:
        date_object = datetime.strptime(str, "%Y-%m-%dT%H:%M:%S%z")
        utc_date_object = date_object.astimezone(pytz.UTC)
        return utc_date_object

urls = []
def read_jsonl(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield json.loads(line)


file_path = 'hdvila/hdvila_part10.jsonl'
# print(df.shape)
begin = datetime.now()
print(begin)
for record in read_jsonl(file_path):
    urls.append(record['url'])
# for vid in df['video_id']:
#     urls.append(f"https://www.youtube.com/watch?v={vid}")
df = pd.DataFrame(urls, columns=['url'])
print(df.shape)
print(df.head())
print(f'urls done: {len(urls)}')
lst = urls
n_pieces = 15
avg_piece_size = len(lst) // n_pieces
remainder = len(lst) % n_pieces

pieces = []
start = 0

for i in range(n_pieces):
    end = start + avg_piece_size + (1 if i < remainder else 0)
    pieces.append(lst[start:end])
    start = end
print(len(pieces))
timelst = []
c = 1
for url in pieces:
    timelst += asyncio.run(get_upload_dates(url))
    print(f'Done Iteration: {c}, at {datetime.now()}')
    c += 1

print(len(timelst))
upload_lst = list(map(str_to_datetime, timelst))

df['year'] = [None if type(date) == str else date.year for date in upload_lst]
df['month'] = [None if type(date) == str else date.month for date in upload_lst]
df['day'] = [None if type(date) == str else date.day for date in upload_lst]
df['time'] = [None if type(date) == str else date.time() for date in upload_lst]

print(df.shape)
# for url, date in zip(urls, upload_dates):
#     print(f"{url}: {date}")
final = datetime.now()
print(final)
print(f'total: {final - begin}')
#
df.to_csv('DatasetsWithTime/hdvilla100m0011_withtime.csv')