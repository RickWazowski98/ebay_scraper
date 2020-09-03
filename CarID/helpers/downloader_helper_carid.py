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
        self.load_product_header()
        self.session_initial()

    def load_product_header(self):
        self.custom_headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'referer': 'https://www.carid.com/sw.js',
            'user-agent': self.user_agent
        }

    def load_search_header(self):
        self.custom_headers = {
            'accept': "application/json, text/javascript, */*; q=0.01",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "en-US;q=0.8,en;q=0.7,uk;q=0.6",
            'referer': "https://www.carid.com/",
            'cookie': "",
            'User-Agent': self.user_agent,
            'x-requested-with': "XMLHttpRequest",
        }

    def session_initial(self):
        self.requests = requests.Session()
        self.rotate_user_agent()
        self.rotate_proxy()
        self.get_page('https://www.carid.com')

    def load_proxy(self):
        self.good_proxies = requests.get(f'http://64.140.158.34:5000').json()
        self.good_proxies = list(set(self.good_proxies) - set(self.bad_proxies))

    def make_dump(self, response, url):
        if '&ajaxid' in url:
            url = url.split('&ajaxid')[0]
        t1 = time.time()
        for i in range(5):
            try:
                logging.debug('making dump for {}'.format(url))
                create_dump_directories(['carid_dumps'])
                accepted_chars = ['/']
                url_to_directory = re.sub(r'[^\x00-\x7F]+', '', ''.join(e for e in url if e.isalnum() or e in accepted_chars).replace('/', '_'))
                directory = 'carid_dumps/{}.html'.format(url_to_directory)
                file = open(directory, 'wb')
                file.write(response.encode('ascii', 'ignore'))
                file.close()
                return time.time() - t1
            except OSError:
                logging.info('Can`t create dump for {}, {} try'.format(url, i))
        return time.time() - t1

    def get_page(self, url, data={}, timeout=30):
        try:
            if url == 'https://www.carid.com':
                raise FileNotFoundError
            mod_url = url
            if '&ajaxid' in mod_url:
                mod_url = mod_url.split('&ajaxid')[0]
            accepted_chars = ['/']
            url_to_directory = re.sub(r'[^\x00-\x7F]+', '', ''.join(e for e in mod_url if e.isalnum() or e in accepted_chars).replace('/', '_'))
            file = open(os.path.join('carid_dumps/{}.html'.format(url_to_directory)), 'r')
            file_response = file.read()
            file.close()
            return file_response
        except FileNotFoundError:
            for i in range(10):
                try:
                    self.custom_headers['User-Agent'] = self.user_agent
                    response = self.requests.get(url, params=data, headers=self.custom_headers, proxies=self.current_proxy_dict, timeout=timeout)
                    if response.status_code == 200:
                        if 'Security Check' in response.text:
                            self.requests = requests.session()
                            self.rotate_user_agent()
                            self.rotate_proxy()
                            if url != 'https://www.carid.com':
                                self.requests.get('https://www.carid.com', params=data, headers=self.custom_headers, proxies=self.current_proxy_dict, timeout=timeout)
                        else:
                            break
                except Exception:
                    self.push_pudproxy()
            if 'search_aj.php' not in response.url:
                self.get_search_key(response)
            if (url != 'https://www.carid.com') and ('Security Check' not in response.text):
                self.make_dump(response.text, url)
            if 'Security Check' in response.text:
                return ''
            return response

    def get_search_key(self, response):
        try:
            self.search_key = response.text.split('"js-head-search-form-holder head-sform" data-code="')[1].split('"')[0].strip()
        except:
            self.search_key = self.search_key

    def rotate_proxy(self):
        if not self.good_proxies:
            proxy_list = requests.get('http://64.140.158.34:5000').json()
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


class ThreadDownloader(DownloaderHelper):
    def __init__(self, manager):
        self.manager = manager
        self.search_key = None
        self.current_proxy_dict = None
        self.user_agent = None
        self.load_product_header()
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
        try:
            proxy = self.good_proxies.pop(0)
        except IndexError:
            self.good_proxies = []
            self.bad_proxies = []
            self.load_proxy()
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
        self.good_proxies = requests.get(f'http://64.140.158.34:5000').json() + self.good_proxies
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
            logging.info('Error no free downloader available')

    def push_download(self, downloader):
        self.downloads_list.append(downloader)
