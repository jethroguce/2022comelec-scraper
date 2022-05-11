import asyncio
import aiohttp
import itertools
import aiofiles
import aiocsv

BASE_URL = 'https://2022electionresults.comelec.gov.ph/data/regions'
RESULTS_URL = 'https://2022electionresults.comelec.gov.ph/data/contests'
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36'

headers = {
    'user-agent': UA,
    'referer': 'https://2022electionresults.comelec.gov.ph/',
    'authority': '2022electionresults.comelec.gov.ph'
}

async def get_data(url, key='srs', retry=0):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as res:
                data = await res.json()
                retval = data[key]
                if key == 'srs':
                    retval = data[key]
                return retval
    except Exception as e:
        print(f'Retrying: {url} {e}')
        return await get_data(url, key, retry+1)

async def get_clusters(region, province, town, bgy):
    print('========= CLUSTERS =========')
    tasks = []
    for key, bgy in bgy.items():
        url = f'{BASE_URL}/{bgy["url"]}.json'
        clusters = await get_data(url, 'pps')
        task = asyncio.create_task(process_data(clusters, region, province, town, bgy))
        tasks.append(task)
    await asyncio.gather(*tasks)

async def get_barangays(region, province, towns):
    print('========= BARANGAYS =========')
    tasks = []
    for key, town in towns.items():
        url = f'{BASE_URL}/{town["url"]}.json'
        bgy = await get_data(url)
        task = asyncio.create_task(get_clusters(region, province, town, bgy))
        tasks.append(task)
    await asyncio.gather(*tasks)

async def get_towns(region, provinces):
    print('========= TOWNS =========')
    tasks = []
    for key, province in provinces.items():
        url = f'{BASE_URL}/{province["url"]}.json'
        towns = await get_data(url)
        task = asyncio.create_task(get_barangays(region, province, towns))
        tasks.append(task)
    await asyncio.gather(*tasks)

async def get_provinces(regions):
    print('========= PROVINCES =========')
    tasks = []
    for key, region in regions.items():
        url = f'{BASE_URL}/{region["url"]}.json'
        provinces = await get_data(url)
        task = asyncio.create_task(get_towns(region, provinces))
        tasks.append(task)
    await asyncio.gather(*tasks)

async def process_data(clusters, region, province, town, bgy):
    for cluster in clusters:
        cluster['country'] = 'PH'
        cluster['region'] = region['rn']
        cluster['province'] = province['rn']
        cluster['town'] = town['rn']
        cluster['barangay'] = bgy['rn']
        vbs = cluster.pop('vbs')[0]
        cluster.update(vbs)

    await write_tofile('clusters.csv', clusters)

async def write_tofile(filename, data=None, writeheader=False):
    mode = 'a'
    if writeheader:
        mode = 'w+'

    async with aiofiles.open(filename, mode=mode) as f:
        headers = ['country','region','province','town','barangay']
        headers += ['ppc','ppcc','ppn','vbc','pre','cpre','url','type','cs']
        writer = aiocsv.AsyncDictWriter(f, headers)
        if writeheader:
            await writer.writeheader()

        if data:
            await writer.writerows(data)

async def main():
    await write_tofile('clusters.csv', writeheader=True)
    print('========= REGIONS =========')
    root_url = f'{BASE_URL}/root.json'
    regions = await get_data(root_url)
    await get_provinces(regions)
            


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    session = loop.run_until_complete(main())
