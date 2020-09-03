import os
import sys
import logging
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class BrowserHelper:
    proxies = {}
    driver = None
    not_headless_mode = False
    perfomance_log = False

    def __init__(self, not_headless_mode=True, proxies={}, perfomance_log=False):
        self.not_headless_mode = not_headless_mode
        self.proxies = proxies
        self.perfomance_log = perfomance_log

    def __del__(self):
        self.stop()

    # main
    def start(self):
        '''Start the browser.'''
        if self.driver is None:
            if 'darwin' in sys.platform:
                chrome_drive_path = 'drivers/mac/chromedriver'
            elif 'win' in sys.platform:
                chrome_drive_path = 'drivers/windows/chromedriver.exe'
            else:
                chrome_drive_path = 'drivers/linux/chromedriver'
            try:
                cd = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
                chrome_drive_path = '{0}/{1}'.format(cd, chrome_drive_path)
                # settings
                options = webdriver.ChromeOptions()
                options.add_experimental_option('prefs', {'profile.managed_default_content_settings.images':2})
                if self.not_headless_mode == False:
                    options.add_argument('--headless')
                if self.proxies != {}:
                    options.add_argument('--proxy-server=%s' % self.proxies)
                options.add_argument('disable-infobars')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-extensions')
                if self.perfomance_log:
                    caps = DesiredCapabilities.CHROME
                    caps['loggingPrefs'] = {'performance': 'ALL'}
                    self.driver = webdriver.Chrome(chrome_drive_path, chrome_options=options, desired_capabilities=caps)
                else:
                    self.driver = webdriver.Chrome(chrome_drive_path, chrome_options=options)
            except:
                logging.error('Cannot start the browser', exc_info=True)
    
    def stop(self):
        '''Stop the browser.'''
        if self.driver is not None:
            try:
                self.driver.quit()
            except:
                logging.error('Cannot stop the browser', exc_info=True)
            self.driver = None

    def restart(self):
        '''Restart the browser.'''
        self.stop()
        self.start()

    def get_page(self, url):
        '''Get page.'''
        try:
            self.driver.get(url)
            return self.get_page_source()
        except:
            logging.error('Cannot get page', exc_info=True)
            return ''

    def get_page_source(self):
        '''Get the current page source'''
        try:
            return self.driver.page_source
        except:
            logging.error('Cannot get page source', exc_info=True)
            return ''

    def element_exist(self, xpath):
        '''Check if element exist.'''
        try:
            self.driver.find_element_by_xpath(xpath)
            return True
        except:
            return False
    
    def wait_element(self, xpath, sec=30):
        '''Wait and element.'''
        i = 0
        while i < sec:
            sleep(1)
            if self.element_exist(xpath):
                return True
    
    def click(self, xpath):
        '''Click.'''
        try:
            self.driver.find_element_by_xpath(xpath).click()
        except:
            logging.error('Cannot click', exc_info=True)

    def send_keys(self, xpath, keys):
        '''Send keys.'''
        try:
            self.driver.find_element_by_xpath(xpath).send_keys(keys)
        except:
            logging.error('Cannot send keys', exc_info=True)
    
    def switch_to_frame(self, name):
        '''Switch to a frame or to iframe'''
        try:
            element = self.driver.find_element_by_xpath("//frame[@name='{0}']".format(name))
            self.driver.switch_to_frame(element)
        except:
            try:
                element = self.driver.find_element_by_xpath("//iframe[@name='{0}']".format(name))
                self.driver.switch_to_frame(element)
            except:
                logging.error('Cannot switch to frame/iframe', exc_info=True)

    def scroll_down(self):
        '''Scroll to the end of the page'''
        try:
            script = "window.scrollTo(0, document.body.scrollHeight);"
            self.driver.execute_script(script)
        except:
            logging.error('Cannot scroll down', exc_info=True)

    def get_cookies(self):
        '''Return dict and test cookies'''
        cookies_text = ''
        cookies_dict = {}
        try:
            for cookie in self.driver.get_cookies():
                name = cookie["name"]
                value = cookie["value"]
                if value == '""': value = ''
                cookies_dict[name] = value
                cookies_text += '{0}="{1}"; '.format(name, value)
            return cookies_dict, cookies_text[:-2]
        except:
            logging.error('Cannot get cookies', exc_info=True)
            return {}, ''
        
    def save_screenshot_of_element(self, xpath, path):
        '''Save screenshot of the element.'''
        try:
            from PIL import Image
            element = self.driver.find_element_by_xpath(xpath)
            location = element.location
            size = element.size
            self.driver.save_screenshot(path)
            im = Image.open(path)
            left = location['x']
            top = location['y']
            right = location['x'] + size['width']
            bottom = location['y'] + size['height']
            im = im.crop((left, top, right, bottom))
            im.save(path)
        except:
            logging.error('Cannot save screenshot', exc_info=True)
