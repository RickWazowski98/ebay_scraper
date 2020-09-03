import pandas as pd
import re
import csv
import pymongo
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
from helpers.downloader_helper import DownloaderHelper

downloader = DownloaderHelper(use_proxy=True, client_name='all')


def get_collection():
    try:
        connection = pymongo.MongoClient('localhost', 27017)
        db = connection['slava']
        collection = db['ebay_group_deal_store']
    except Exception as e:
        raise e
    return collection


def get_product_to_scrape():
    collection = get_collection()
    result = list(collection.find({"ebay_search_result.fitment": ""}, {'Product': 1, '_id': 0}))
    products_to_scrape = {str(source_item['Product']): source_item for source_item in result}

    # get items to process
    items_to_process = []
    for product_id in products_to_scrape.keys():
        items_to_process.append(products_to_scrape[product_id])
    print(len(items_to_process))
    return items_to_process


def chunkify(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def write_to_csv(output_list, method, delimiter):
    output_file = '/home/slava/Slava_Projects/New_Ebay/FIles/output_file_copy.csv'

    with open(output_file, method) as f:
        writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writerows([output_list])


def get_page(url):
    cookies = {'_ga': 'GA1.2.415030213.1540820804', '_gid': 'GA1.2.1929604581.1540820804',
               'AMCVS_A71B5B5B54F607AB0A4C98A2%40AdobeOrg': '1',
               'bm_sv': '2B0EAE81F8211718FF0CBEB7B77D1638~R5jZTqLv3jspGpIq/lpHpneJ3ICLd+YsnA0ziCJ5aU9dYnNFqNc3YFywpJvOLo5dQPnX3qmMZDa0DtNQLjGDTgxMZ4WWdsnb427VvuFjeZIDBQO/j+48A5HhZJveqS7wnWho9Wr2ADppgNItZv5BNBcRyL8wQB8q7yVznGI03G4',
               'ak_bmsc': '1AABA9E4B8BAF724D36F7ADB269CD1F4685E645DB4660000250DD75BD7C74F0A~pl0KjlYyCW59HjQRYcqlEmDZB0T++X8CD2inbLIVr8n88mds81Z+cbD/yFEIvQopX1FvfQ0RhbHpOIqcZ02BPY5HUvCZrYR+9FZK59JJM6F5mAeuRgo63IlXS9T5PgpJfMSlcHafxhDXsyC9Y81TChf6ZU4gqjrMDfYZi7BkYKd0tlCis7lCYK3laKcGGQauwzl4MerH1emkegsunHjbvN6BIvr+q274H9iyFe5jR99kU',
               'cid': '3za4G3Gj8Gn7nEfV%23551347784', 'ds1': 'ats/1540834013526',
               'aam_uuid': '65016836551818795772119429020039847006', 'cssg': 'ab4d76f61660aad9a112c56afe64d3f6',
               'JSESSIONID': 'C3BF53B00AC731F21BE0FD513B88B8B8',
               '__gads': 'ID=ee6ba28b70854516:T=1544705983:S=ALNI_MYHEqtZ0goNC9Y-72RAyWojBgEemA',
               'AMCV_A71B5B5B54F607AB0A4C98A2%40AdobeOrg': '-1758798782%7CMCIDTS%7C17878%7CMCMID%7C65004536900471973482118213068408661475%7CMCAAMLH-1545242975%7C6%7CMCAAMB-1545311304%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCCIDH%7C-141992339%7CMCOPTOUT-1544713704s%7CNONE%7CMCAID%7CNONE',
               'npii': 'btguid/ab4d76f61660aad9a112c56afe64d3f65df38d57^cguid/ab4d76ff1660aad9a112c56afe64d3f05df38d57^',
               'ns1': 'BAQAAAWd7qgEFAAaAAKUADV3zjWwxODU2MzQ3ODcwLzA7ANgAWF3zjWxjODR8NjAxXjE1NDA4MzQwMTQwMDZeYVhad1pYUXhOUT09XjFeM3wyfDV8NHw3fDExXjFeMl40XjNeMTJeMTJeMl4xXjFeMF4xXjBeMV42NDQyNDU5MDc1JPhAofINc4vakRyi2ER3jRI09L8*',
               'dp1': 'bkms/in5fd4c0ec^u1f/Ivan5df38d6c^u1p/aXZwZXQxNQ**5df38d6c^bl/USen-US5fd4c0ec^expt/00015415880860125cd35e36^pbf/%23e000e000008180c20000045df38d6c^',
               's': 'CgAD4ACBcE6tsYWI0ZDc2ZjYxNjYwYWFkOWExMTJjNTZhZmU2NGQzZjYA7gCfXBOrbDMGaHR0cHM6Ly93d3cuZWJheS5jb20vc2NoL2kuaHRtbD9fZnJvbT1SNDAmX3Rya3NpZD1tNTcwLmwxMzEzJl9ua3c9QUMxMDAwMTc3UiZfc2FjYXQ9MCZMSF9UaXRsZURlc2M9MCZfb3NhY2F0PTAmX29ka3c9QUMxMDAwMTc3JkxIX1RpdGxlRGVzYz0wI2l0ZW0zNjIzODBiNTY2B86xLdY*',
               'nonsession': 'BAQAAAWd7qgEFAAaAAJ0ACF3zjWwwMDAwMDAwMQFkAARd841sIzA4YQAIABxcOebsMTU0NDcwNTk4MXgyMzI1MjM4NzE1OTB4MHgyWQAzAA5d841sMTEyMjAtMTcxNCxVU0EAywACXBJg9DkxAEAAB13zjWxpdnBldDE1ABAAB13zjWxpdnBldDE1AMoAIGV4W2xhYjRkNzZmNjE2NjBhYWQ5YTExMmM1NmFmZTY0ZDNmNgAEAAdduHZdaXZwZXQxNQCcADhd841sblkrc0haMlByQm1kajZ3Vm5ZK3NFWjJQckEyZGo2QU1sWVNpREplS3BneWRqNng5blkrc2VRPT1BS2xSDGgmYGXqrN2V0G6ArYeFWA**',
               'ebay': '%5EsfLMD%3D0%5Esin%3Din%5Edv%3D5bd73943%5Esbf%3D%2340400000000010000000004%5Ecos%3D2%5Ecv%3D15555%5Ejs%3D1%5Epsi%3DArqzCyjo*%5E',
               'ds2': 'sotr/b8_5azzzzzzz^'}
    attempts = 20
    while attempts > 0:
        attempts -= 1
        try:
            overview_page = downloader.get_page(url, cookies)
            soup = BeautifulSoup(overview_page, 'lxml')
            return soup
        except:
            pass


def get_links(sku):
    items_links = []
    soup = get_page('https://www.ebay.com/sch/i.html?_from=R40&_trksid=m570.l1313&_nkw='+str(sku)+'&_sacat=0')
    try:
        results = soup.find('h1', class_='srp-controls__count-heading').text.split(' ')[0]
    except:
        pass
    if results != '0':
        try:
            items = soup.find('ul', id='ListViewInner')
            links_list = items.find_all('li', class_=re.compile('^sresult lvresult clearfix li'))
        except:
            try:
                items = soup.find('ul', class_='srp-results srp-list clearfix')
                links_list = items.find_all('li', class_='s-item')
            except:
                pass
        try:
            for link in links_list:
                items_links.append(link.find('div', class_='s-item__info clearfix').find('a')['href'])
        except:
            pass

    return items_links


def found_fitment(link):
    fitment = False
    ebay_id = None
    soup = get_page(link)
    fitments = soup.find('div', id='vi-ilComp')
    if fitments:
        fitment = True
        ebay_id = soup.find('div', id='descItemNumber').text
    return fitment, ebay_id


def get_fitment(item):
    # if item['Match'] is None:
    fitment_found = False
    sku = item['sku']
    item_links_sku = get_links(sku)
    if item_links_sku:
        for item_link in item_links_sku:
            fitment, ebay_id = found_fitment(item_link)
            if fitment:
                item['Match'] = ebay_id
                fitment_found = True
                break
    if not fitment_found:
        sku2 = item['sku 2']
        item_links_sku2 = get_links(sku2)
        if item_links_sku2:
            for item_link in item_links_sku2:
                fitment, ebay_id = found_fitment(item_link)
                if fitment:
                    item['Match'] = ebay_id
                    fitment_found = True
                    break

    if not fitment_found:
        item['Match'] = 'Not found'
    return item


def get_product_title(link):
    pass


def get_fit_and_id(product_to_scrap):
    collection = get_collection()
    for product in product_to_scrap.keys():
        try:
            for link in get_links(product_to_scrap[product]):
                fit, ebay_id = found_fitment(link)
                if fit:
                    break
            search_result = {"fitment": fit, "ebay_id": ebay_id}
        except:
            pass
        try:
            collection.update_one(
                {"Product": product_to_scrap[product]},
                {"$set": {
                    "ebay_search_result": search_result,
                }}
            )
        except UnboundLocalError as err:
            continue
        print("%s - product was update" % product_to_scrap[product])
        print(search_result)


def main():
    pool = ThreadPool(30)
    data = pool.map(get_fit_and_id, get_product_to_scrape())


if __name__ == '__main__':
    main()
