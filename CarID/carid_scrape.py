import csv
import json
import pymongo
import logging
import pandas as pd
import requests
import ast
from bs4 import BeautifulSoup
from time import gmtime, strftime
from multiprocessing.pool import ThreadPool
from helpers.downloader_helper_carid import PoolDownload


logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level=logging.DEBUG)#, filename=f'/home/slava/dataforest/GroupDealStore/carid_log/carid_log_{strftime("%Y-%m-%d-%H-%M", gmtime())}.log')


class CaridScraper():
    """Collect products info from CARiD.

    The main logic is divided into two parts:
        - links collection
        - data collection on these links.
    """

    # create pool of downloaders
    manager = PoolDownload(5)

    # connect to a database
    try:
        connection = pymongo.MongoClient('localhost', 27017)
        db = connection['slava']
        collection = db['ebay_group_deal_store']
    except Exception as e:
        logging.error("ERROR: %s" % e)
        logging.info("I am unable to connect to the database.")


    def check_db_data(self):
        val = list(self.collection.aggregate([{
            "$group" : { "_id": "$Product", "count": { "$sum": 1 } } },
            {"$match": {"_id" :{ "$ne" : None } , "count" : {"$gt": 1} } }, 
            {"$project": {"Product" : "$_id", "_id" : 0} }
        ]))


    def scrape_carid(self, search_scrape_mode=False, product_scrape_mode=False):
        """Collect product data from CARiD.

        @:param self:
        @:param search_scrape_mode: enable or disable mode for search scrape
        @:param product_scrape_mode: enable or disable mode for product scrape

        @:type self: CaridScraper
        @:type search_scrape_mode: bool
        @:type product_scrape_mode: bool

        The function can work in 3 different modes:
            - search_scrape_mode
            - product_scrape_mode
            - all modes enabled

        In search_scrape_mode occurs collecting product links.
        In product_scrape_mode occures collecting product data from product page.
        If all modes was enable -> A collection of product links and collection of product data are performed in a consistent manner.

        @:returns: True
        @:rtype: bool
        """
        

        pool = ThreadPool(5)
        if search_scrape_mode:
            result = list(self.collection.find({}, {'Product': 1,'_id': 0}))
            products_to_scrape = {str(source_item['Product']): source_item for source_item in result}

            # get items to process
            items_to_process = []
            for product_id in products_to_scrape.keys():
                items_to_process.append(products_to_scrape[product_id])

            logging.info(f'Found {len(items_to_process)} product for search scrape.')
            pool.map(self.search_scrape_master, items_to_process)
        if product_scrape_mode:
            # get products from db
            result = list(self.collection.find({"search_result": {'$ne': None}}, {'Product': 1, 'search_result': 1, '_id': 0}))
            #result = list(self.collection.find({"search_result": {'$ne': None}}, {'Product': 1, 'search_result': {"$elemMatch": {"url": {"$ne": None}}, '_id': 0}}))

            products_to_scrape = {str(source_item['Product']): source_item for source_item in result}

            # get items to process
            items_to_process = []
            for item_id in products_to_scrape.keys():
                    items_to_process.append(products_to_scrape[item_id])
            logging.info(f'Found {len(items_to_process)} product for product scrape.')
            pool.map(self.product_pages_scrape, items_to_process)
        pool.close()
        return True

    def search_scrape_master(self, product):
        """Collect product links from CARiD.

        @:param self:
        @:param product: dict with ebay id and values for search
        @:type self: CaridScraper
        @:type product: dict

        Searches for each of the values and collecting product links for current value for search.

        @:returns: True
        @:rtype: bool
        """

        ebay_id = product.get('Product')
        
        initial_values_for_search = product.get('Product')
     
        info_msg = ''
        security = False
        product_links = None

        # search for links
        if initial_values_for_search:
            search_response = self.get_search_response(initial_values_for_search)
            if search_response:
                search_data = self.scrape_search_page(search_response)
                if not search_data:
                    info_msg = 'No products found.'
                    #continue
                info_msg = f'Found {len(search_data)} products.'
                logging.info(f'Found {len(search_data)} products for {initial_values_for_search}')
            else:
                security = True
                logging.info(f'Security Check in final response from search: {initial_values_for_search}.')
                info_msg = 'Security Check in final response from search.'
        else:
            logging.info(f'No values for search {ebay_id}')
            info_msg = 'No values for search.'

        # If dont have security check in response
        if not security:
            self.collection.update_one({"Product": ebay_id}, {"$set": {
                    "info_msg": info_msg,
                        "search_result": search_data,
                    }
                }, upsert=True)
        return True

    def product_scrape_master(self, product):
        """Collect product data.

        @:param self:
        @:param product: dict with id and link of product
        @:type self: CaridScraper
        @:type product: dict

        Collecting data from product page.

        @:returns: True
        @:rtype: bool
        """

        # get respons
        data_to_write = []
        links_to_scrape = []
        for item in product["search_result"]:
            links_to_scrape.append(item["url"])
        downloader = self.manager.get_download()
        downloader.load_product_header()
        for link in links_to_scrape:
            product_page = downloader.get_page(link)
            self.manager.push_download(downloader)

        # scrape product fitments
            security = False
            if product_page:
                try:
                    page = BeautifulSoup(product_page.text, 'html.parser')
                except:
                    page = BeautifulSoup(product_page, 'html.parser')
                try:
                    images = page.find("div", class_= "prod-gallery-thumbs prod-col-narrow js-product-thumbs").find_all("a")
                except:
                    images = None
            else:
                security = True
                logging.info('Security Check in final response for product link.')

        # If dont have security check in response
        if not security:
            pass
        return True

    def product_pages_scrape(self, res):
        prod_id = res.get("Product")
        data_to_write = []
        links_to_scrape = []
        page_to_scrape = []
        for item in res["search_result"]:
            brand= item["abstr"].split(" ")[0]
            brand = brand.replace("\xae","")
            checked_id = item["abstr"].split(" ")[1]
            if brand == "Replace" and checked_id == prod_id:
                links_to_scrape.append(item["url"])
                
        if len(links_to_scrape) == 0:
            try:   
                links_to_scrape.append(res["search_result"][0]["url"])
            except:
                logging.info("no url for product by id: %s" % prod_id)
                return None
        downloader = self.manager.get_download()
        downloader.load_product_header()
        for link in links_to_scrape:
            product_page = downloader.get_page(link)
            page_to_scrape.append(product_page)
            self.manager.push_download(downloader)
        for prod_page in page_to_scrape:
            #logging.info("scrape product url: %s" % prod_page)
            try:
                page = BeautifulSoup(prod_page.content, 'html.parser')
            except:
                try:
                    page = BeautifulSoup(prod_page.text, "html.parser")
                except:
                    page = BeautifulSoup(prod_page, "html.parser")
            try:
                product= page.find(class_="name").get_text()
            except:
                logging.info("no scrape product date to url: {}".format(prod_page))
                return None
            
            try:
                images = page.find("div", class_="main-content").find("main").find("div", class_="wrap main_wide").find_all("script")[-1]
                images = images.string.split("= ")[1].split(";")[0].split("'path': ")[1].split(",'alt': ")[0].replace("'","")
            except:
                images = ""

            try:
                replaces_oe = page.find(class_="prod-offer-content js-point-price-match js-prod-part-numbers").get_text().split(" ")
            except:
                replaces_oe = ""

            try:
                description = page.find_all(itemprop="description")[-1].get_text()
            except:
                description = ""
            try:
                full_description = page.find(class_="prod-full-descr").get_text()
            except:
                full_description = ""
            try:
                price = page.find(class_="js-product-price-hide prod-price").get_text()
            except:
                price = ""
            try:
                features = page.find(id="product-details").find("ul").find_all("li")
                features = [elem.get_text() for elem in features]
            except:
                features = ""
            try:
                replaces = page.find_all(class_="prod-offer-content js-point-price-match")
            except:
                replaces = ""
            try:
                part_number = replaces[0].get_text()
            except:
                part_number = ""
            try:
                upc = replaces[1].get_text()
            except:
                upc = ""
            try:
                fitment = {}
                fitment_data = page.find_all(class_="mmy-row")
                for data in fitment_data:
                    prime_key = data.find(class_="mmy-row-title pointer js-mmy-row-expand").text
                    fitment[prime_key] = []
                    subs = data.find_all(class_="mmy-subrow")
                    for sub in subs:
                        sub_title = sub.find(class_="mmy-subrow-title").get_text()
                        try:
                            sub_date = sub.find("ul").find_all("li")
                            sub_date =[elem.get_text() for elem in sub_date]
                            fitment[prime_key].append({sub_title:sub_date})
                        except:
                            fitment[prime_key].append(sub_title)
            except:
                fitment = ""

            data_to_write.append({"product":product, "images":images, "replaces_oe":replaces_oe, "description":description, "full_description":full_description, "features":features, "part_number":part_number, "upc":upc, "fitment":fitment, "price":price})
        self.collection.update_one({"Product": prod_id}, {"$set": {
                        "details_search_result":data_to_write,
                    }
                }, upsert=True)


    def get_search_response(self, product_number):
        """Get search response from CARiD.

        @:param self:
        @:param product_number: any value for search
        @:type self: CaridScraper
        @:type product_number: str

        Make search request with product number and unique search key from downloader.
        Get search response.

        @:returns: search info about product
        @:rtype: text
        """

        # get search CarID response
        downloader = self.manager.get_download()
        downloader.load_search_header()
        url = f"https://www.carid.com/search_aj.php?keep_https=1&term={product_number}&ajaxid=1&code={downloader.search_key}"
        search_response = downloader.get_page(url)
        self.manager.push_download(downloader)
        
        return search_response

    def scrape_search_page(self, search_response):
        """Collect product data.

        @:param self:
        @:param search_response: str with info about search result
        @:type self: CaridScraper
        @:type search_response: str

        Search response parsing. Get all links for current product from search result.

        @:returns: list with product links
        @:rtype: list
        """
        # get search info as json
        try:
            html = json.loads(search_response.content)['html']
        except:
            html = json.loads(search_response)['html']

        products_list = []
        if '0 results found' in html:
            return products_list

        # get all links for product
        page = BeautifulSoup(html, 'html.parser')
        items = page.find("div", class_="autoc-prod").find("ul").find_all("li")
        for prod in items:
            if prod.find("a"):
                url = "https://www.carid.com" + str(prod.find("a")["href"])

                try:
                    name_str = prod.find("a").find("span", class_="text").find("span", class_="name1").text
                except AttributeError:
                    name_str = prod.find("a").find("span", class_="text").find("span", class_="sub-title").text
                
                try:
                    oe_part = prod.find("a").find("span", class_="text").find("span", class_="item-number").text
                except AttributeError:
                    oe_part = ""
                
                try:
                    absctrect_cat = prod.find("a").find("span", class_="text").find("span", class_="name0").text
                except AttributeError:
                    absctrect_cat = prod.find("a").find("span", class_="text").find("span", class_="title").text

                try:
                    price = prod.find("a").find("span", class_="text").find("span", class_="price-holder").find("span", class_="autoc-today-price").text
                except AttributeError:
                    price ="-1"

                products_list.append({"url":url, "name":name_str, "(OE)Part":oe_part, "abstr":absctrect_cat, "price":price})
        return products_list

    def write_to_csv_processed(self, output_list, method, delimiter, filename):
        output_file = f'/home/slava/dataforest/GroupDealStore/Files/{filename}.csv'
        with open(output_file, method) as f:
            writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
            writer.writerows([output_list])


def main():
    cs = CaridScraper()
    #cs.scrape_carid(search_scrape_mode=True, product_scrape_mode=True)
    #cs.scrape_carid(search_scrape_mode=True)
    cs.scrape_carid(product_scrape_mode=True)


if __name__ == '__main__':
    main()
