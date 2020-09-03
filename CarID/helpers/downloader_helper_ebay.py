import re
import os
import csv
import time
import logging
import requests
import requests.utils


def create_dump_directories(dirs):
    for directory in dirs:
        logging.debug('creating {} directory'.format(directory))
        if not os.path.exists(directory):
            os.makedirs(directory)


class DownloaderHelper():
    def __init__(self):
        self.bad_proxies = []
        self.good_proxies = []
        self.current_proxy = None
        self.user_agent = None
        self.custom_headers = None
        self.search_key = None

        self.load_proxy()
        self.load_user_agents()
        self.session_initial()

    def session_initial(self):
        self.requests = requests.Session()
        self.rotate_user_agent()
        self.rotate_proxy()

    def load_proxy(self):
        self.good_proxies = requests.get(f'http://64.140.158.34:5000').json()
        self.good_proxies = list(set(self.good_proxies) - set(self.bad_proxies))

    def make_dump(self, response, url):
        t1 = time.time()
        for i in range(5):
            try:
                logging.debug('making dump for {}'.format(url))
                create_dump_directories(['ebay_dumps'])
                accepted_chars = ['/']
                url_to_directory = re.sub(r'[^\x00-\x7F]+', '', ''.join(e for e in url if e.isalnum() or e in accepted_chars).replace('/', '_'))
                directory = 'ebay_dumps/{}.html'.format(url_to_directory)
                file = open(directory, 'wb')
                file.write(response.encode('ascii', 'ignore'))
                file.close()
                return time.time() - t1
            except OSError:
                logging.info('Can`t create dump for {}, {} try'.format(url, i))
                # print('Can`t create dump for {}, {} try'.format(url, i))
        return time.time() - t1

    def get_page(self, url, data={}, cookies={}, timeout=30):
        try:
            accepted_chars = ['/']
            url_to_directory = re.sub(r'[^\x00-\x7F]+', '', ''.join(e for e in url if e.isalnum() or e in accepted_chars).replace('/', '_'))
            try:
                file = open(os.path.join('ebay_dumps/{}.html'.format(url_to_directory)), 'r')
            except:
                raise FileNotFoundError
            file_response = file.read()
            file.close()
            return file_response
        except FileNotFoundError:
            for i in range(10):
                try:
                    headers = {'User-Agent': self.user_agent}
                    response = self.requests.get(url, cookies=cookies, params=data, headers=headers, proxies=self.current_proxy_dict, timeout=timeout)
                    if response.status_code == 200:
                        break
                except Exception:
                    self.push_pudproxy()
            if '&_ipg=200' not in url:
                self.make_dump(response.text, url)
            return response

    def rotate_proxy(self):
        if not self.good_proxies:
            proxy_list = requests.get('http://64.140.158.34:5000/proxy?service=all').json()
            self.good_proxies = list(set(proxy_list) - set(self.bad_proxies))
        if self.current_proxy:
            self.good_proxies.append(self.current_proxy)
        self.current_proxy = self.good_proxies.pop(0)
        self.current_proxy_dict = {"http": self.current_proxy, "https": self.current_proxy}

    def rotate_user_agent(self):
        if self.user_agent:
            self.user_agents_list.append(self.user_agent)
        self.user_agent = self.user_agents_list.pop(0)

    def load_user_agents(self):
        cd = os.path.dirname(os.path.abspath(__file__))
        csvFile = os.path.join(cd, 'valid_user_agents.csv')
        with open(csvFile, 'r') as f:
            reader = csv.reader(f)
            user_agents_list = list(reader)
            self.user_agents_list = [x[0] for x in user_agents_list]

    def push_pudproxy(self):
        self.bad_proxies.append(self.current_proxy)
        self.current_proxy = None
        self.rotate_proxy()
        self.rotate_user_agent()


class ThreadDownloader(DownloaderHelper):
    def __init__(self, manager):
        self.manager = manager
        self.search_key = None
        self.current_proxy_dict = None
        self.user_agent = None
        self.session_initial()

    def rotate_proxy(self):
        self.current_proxy_dict = self.manager.get_proxy()

    def rotate_user_agent(self):
        self.user_agent = self.manager.get_useragent()

    def push_pudproxy(self):
        self.manager.push_budproxy(self.current_proxy_dict)
        self.current_proxy_dict = self.manager.get_proxy()


class PoolDownload():
    def __init__(self, numbers_downloaders):
        self.start_timer = time.perf_counter()
        self.user_agents_list = None
        self.good_proxies = []
        self.bad_proxies = []
        self.load_proxy()
        self.load_user_agents()
        self.downloads_list = [ThreadDownloader(self) for _ in range(numbers_downloaders)]

    def get_useragent(self):
        user_agent = self.user_agents_list.pop(0)
        self.user_agents_list.append(user_agent)
        return user_agent

    def get_proxy(self):
        if not self.good_proxies or time.perf_counter() - self.start_timer >= 1800:
            self.load_proxy()
            self.start_timer = time.perf_counter()
        proxy = self.good_proxies.pop(0)
        self.good_proxies.append(proxy)
        current_proxy_dict = {"http": proxy, "https": proxy}
        return current_proxy_dict

    def push_budproxy(self, proxy_dict):
        proxy = proxy_dict['http']
        try:
            self.bad_proxies.append(proxy)
            self.good_proxies.remove(proxy)
        except:
            pass

    def load_proxy(self):
        self.good_proxies = requests.get(f'http://64.140.158.34:5000').json()# + self.good_proxies
        self.good_proxies = list(set(self.good_proxies) - set(self.bad_proxies))

    def load_user_agents(self):
        cd = os.path.dirname(os.path.abspath(__file__))
        csvFile = os.path.join(cd, 'valid_user_agents.csv')
        with open(csvFile, 'r') as f:
            reader = csv.reader(f)
            user_agents_list = list(reader)
            self.user_agents_list = [x[0] for x in user_agents_list]

    def get_download(self):
        if self.downloads_list:
            return self.downloads_list.pop(0)
        else:
            print('Error no free downloader available')

    def push_download(self, downloader):
        self.downloads_list.append(downloader)
