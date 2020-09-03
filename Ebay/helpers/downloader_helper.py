import random
import json
import logging
import requests
import requests.utils
from requests_html import HTML


class DownloaderHelper:
    # settings
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    attempts = 2
    # others
    http_requests = None
    proxies = {}
    # state
    use_proxy = False
    use_manual_proxy = False
    use_session = False
    proxies_list = []
    failed_proxies_list = []

    def __init__(self, use_session=True, use_proxy=True, client_name='', use_manual_proxy=False):
        self.load_requests(use_session)
        if use_proxy or use_manual_proxy:
            self.use_manual_proxy = use_manual_proxy
            self.use_proxy = use_proxy
            self.attempts = 20
            self.load_proxies(client_name)

    def create_request(self, type, url, cookies={}, data={}, cookies_text='', headers={}, timeout=10, response_all=False, render_page=False):
        '''Create request.'''
        # create headers
        if headers == {}:
            headers = {'User-Agent': self.user_agent}
            if cookies_text != '':
                # this cookie_text overwrites other cookies
                headers['Cookie'] = cookies_text
        #
        attempts = self.attempts
        while attempts > 0:
            # get new proxies if necesary
            if self.use_proxy:
                if self.use_manual_proxy:
                    if not self.proxies:
                        self.proxies = self.get_random_proxies()
                else:
                    self.proxies = self.get_random_proxies()
                # try without proxies last time
                if attempts == 1:
                    self.proxies = {}
            # create request
            try:
                if type == 'post':
                    r = self.http_requests.post(url, cookies=cookies, data=data, headers=headers, proxies=self.proxies, timeout=timeout)
                else:
                    r = self.http_requests.get(url, cookies=cookies, params=data, headers=headers, proxies=self.proxies, timeout=timeout)
                if response_all:
                    return r
                else:
                    if render_page:
                        return self.render_html_page(r.text)
                    return r.text
            except:
                if self.use_proxy:
                    if self.use_manual_proxy is False:
                        self.mark_proxies_as_failed()
                attempts = attempts - 1
        # cannot create requests
        logging.error(f'Cannot create {type} request to {url}')
        if response_all == False:
            return ''

    def render_html_page(self, page_content):
        '''Render html page'''
        try:
            html = HTML(html=page_content)
            html.render(reload=False)
            return html.text
        except:
            logging.error('Cannot render the page', exc_info=True)
            return page_content

    def get_page(self, url, cookies={}, data={}, cookies_text='', headers={}, timeout=10, response_all=False, render_page=False):
        '''Get get data.'''
        return self.create_request('get', url, cookies, data, cookies_text, headers, timeout, response_all, render_page)

    def post_page(self, url, cookies={}, data={}, cookies_text='', headers={}, timeout=60, response_all=False):
        '''Get post data.'''
        return self.create_request('post', url, cookies, data, cookies_text, headers, timeout, response_all)

    # others
    def load_requests(self, use_session):
        '''Load requests'''
        self.use_session = use_session
        if use_session:
            self.http_requests = requests.Session()
        else:
            self.http_requests = requests

    def reload_requests(self):
        '''Reload requests'''
        self.load_requests(self.use_session)

    # proxies
    def load_proxies(self, client_name):
        '''Load proxies'''
        from helpers.proxy_helper import ProxyHelper
        proxy_helper = ProxyHelper()
        self.proxies_list = proxy_helper.get_proxies_list(client_name)
        self.failed_proxies_list = []

    def change_proxies(self, proxies=None):
        '''Change proxies manually'''
        self.reload_requests()
        if not proxies:
            self.proxies = self.get_random_proxies()
        else:
            self.proxies = proxies

    def get_random_proxies(self):
        '''Get random proxy'''
        if len(self.proxies_list) > 0:
            try:
                proxy = random.choice(self.proxies_list)
                return {
                    'http': 'http://{0}'.format(proxy),
                    'https': 'https://{0}'.format(proxy)
                }
            except:
                logging.error('Cannot get random proxies', exc_info=True)
                return {}
        elif len(self.proxies_list) == 0:
            self.load_proxies("all")
            try:
                proxy = random.choice(self.proxies_list)
                return {
                    'http': 'http://{0}'.format(proxy),
                    'https': 'https://{0}'.format(proxy)
                }
            except:
                logging.error('Cannot get random proxies', exc_info=True)
                return {}

        logging.warning(f'No proxies left. Failed proxies count: {self.failed_proxies_list}')
        return {}

    def mark_proxies_as_failed(self):
        '''Add failed proxies to failed list'''
        proxy = self.proxies['http'].split('http://')[-1]
        if self.proxies_list and proxy in self.proxies_list:
            self.failed_proxies_list.append(proxy)
            self.proxies_list.remove(proxy)
        else:
            logging.warning(f'No proxies left. Failed proxies count: {self.failed_proxies_list}')

    # cookies
    def get_session_cookies(self):
        '''Get cookies from the current session'''
        try:
            return requests.utils.dict_from_cookiejar(self.http_requests.cookies)
        except:
            logging.error('Cannot get cookies from session', exc_info=True)
            return {}

    def save_cookies_to_file(self, cookies={}, name='cookies'):
        '''Save cookies dict to the file'''
        try:
            cookies = json.dumps(cookies)
            with open(f'{name}.txt', 'w') as the_file:
                the_file.write(cookies)
        except:
            logging.error('Cannot save cookies to file', exc_info=True)

    def get_cookies_from_file(self, name='cookies'):
        '''Return cookies dict from the file'''
        try:
            with open('{name}.txt') as the_file:
                cookies_text = the_file.read()
            try:
                cookies = json.loads(cookies_text)
            except:
                cookies = self.get_dict_cookies(cookies_text)
            return cookies
        except:
            logging.error('Cannot get cookies from file', exc_info=True)
            return {}

    def get_dict_cookies(self, rawdata):
        '''Returnt dict from cookies raw text'''
        try:
            from Cookie import SimpleCookie
            cookie = SimpleCookie()
            cookie.load(rawdata)
            cookies = {}
            for key, morsel in cookie.items():
                cookies[key] = morsel.value
        except:
            logging.error('Cannot get cookies from raw text', exc_info=True)
            cookies = {}
        return cookies
