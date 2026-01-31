from selenium.webdriver.chrome.service import Service
from selenium import webdriver
import traceback
#from selenium.webdriver.support.ui import WebDriverWait

class selenium_wrapper:
    __slots__= ['options','service']
    def __init__(self,service_params:list,page_load_strategy:str='normal',log_output:str=None,executable_path:str="/usr/bin/chromedriver"):
        self.service=Service(executable_path=executable_path,log_output=log_output)

        self.options = webdriver.ChromeOptions()
        self.options.page_load_strategy = page_load_strategy
        for _param in service_params:
            self.options.add_argument(_param)
            
    def get_page_source(self,url:str)->str:
        driver = webdriver.Chrome(service = self.service,options=self.options)
        try:
            driver.get(url)
            res=(True,driver.page_source)
        except Exception as e:
            res=(False,traceback.format_exc())
        finally:
            driver.close()
        return res
