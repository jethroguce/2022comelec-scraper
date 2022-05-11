import asyncio
import aiohttp
import itertools
import aiofiles
import aiocsv
import time

from candidates import CANDIDATES

RESULTS_URL = 'https://2022electionresults.comelec.gov.ph/data/results'
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36'
headers = {
    'user-agent': UA,
    'referer': 'https://2022electionresults.comelec.gov.ph/',
    'authority': '2022electionresults.comelec.gov.ph'
}

async def get_data(url, retry=0):
    if retry > 2:
        return []
    time.sleep(retry*2)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as res:
                try:
                    data = await res.json()
                    return data['rs']
                except Exception as e:
                    print(f'Error: {url} {e} {retry}')
                    print(res.text)
                    return []

    except Exception as e:
        print(f'Retrying: {url} {e} {retry}')
        return await get_data(url, retry+1)

async def process_data(source, cluster, cc=[5587, 5588]):
    for data in source:
        if data['cc'] in cc:
            candidate = CANDIDATES[data['bo']]
            cluster[candidate['bon']] = data['v']
            cluster['total'] = data['tot']
            cluster['serial'] = data['ser']

    return [cluster]

async def write_tofile(filename, data=None, writeheader=False):
    mode = 'a'
    if writeheader:
        mode = 'w+'

    async with aiofiles.open(filename, mode=mode) as f:
        headers = ['country','region','province','town','barangay']
        headers += ['ppc','ppcc','ppn','vbc','pre','cpre','url','type','cs']
        headers += ['total','serial']
        headers += [candidate['bon'] for candidate in CANDIDATES.values()]
        writer = aiocsv.AsyncDictWriter(f, headers)
        if writeheader:
            await writer.writeheader()

        if data:
            await writer.writerows(data)

async def process_cluster(cluster, x):
    url = f'{RESULTS_URL}/{cluster["url"]}.json'
    data = await get_data(url)
    for candidate in CANDIDATES.values():
        cluster[candidate['bon']] = 0
    cluster['total'] = 0
    cluster['serial'] = ""
    clean_data = await process_data(data, cluster)
    await write_tofile('results.csv', clean_data)
    print(f'Completed {x}')
    

async def main():
    x = 0
    await write_tofile('results.csv', writeheader=True)

    async with aiofiles.open('clusters.csv', 'r') as f:
        tasks = []
        async for row in aiocsv.AsyncDictReader(f):
            x+=1
            print(f'Started {x}')
            task = asyncio.create_task(process_cluster(row, x))
            tasks.append(task)

        results = await asyncio.gather(*tasks)



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    session = loop.run_until_complete(main())
