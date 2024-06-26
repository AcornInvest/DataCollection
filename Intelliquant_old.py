# 크롬을 실행시켜서 제어
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementNotInteractableException
import chromedriver_autoinstaller
import subprocess
import time
import os
import configparser
import json
import pyperclip

class Intelliquant:
    def __init__(self):
        self.load_config() # 설정 로드

    def __del__(self):
        ''' # 나중에 주석 해제하기
        if hasattr(self, 'driver'):
            self.driver.close()
        '''

    def load_config(self): # 설정 불러오기
        self.path = os.getcwd() + '\\' + 'config_Intelliquant.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(self.path, encoding='utf-8')

        # 설정값 읽기
        self.path_chrome = config['path']['path_chrome']
        self.port = config['path']['port']
        self.argument = config['path']['argument']

    def chrome_on(self, logger, page, name):
        subprocess.Popen(self.path_chrome)  # 디버거 크롬 구동

        option = Options()
        option.add_experimental_option("debuggerAddress", self.port)
        option.add_argument(self.argument)

        chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]
        driver_path = f'./{chrome_ver}/chromedriver.exe'
        if os.path.exists(driver_path):
            print(f"chrom driver is installed: {driver_path}")

        else:
            print(f"install the chrome driver(ver: {chrome_ver})")
            chromedriver_autoinstaller.install(True)

        service = Service(executable_path=f'./{chrome_ver}/chromedriver.exe')
        self.driver = webdriver.Chrome(service=service, options=option)  # selenium 4.10 버전으로 오면서 형식 변경

        ## 인텔리퀀트 홈페이지 들어갈 때 가끔 광고 팝업이 떠서 스튜디오를 누를 수 없는 경우 대비하여 수정함
        # 페이지 최상단으로 이동한 후 특정 상대좌표로 이동
        top_element = self.driver.find_element(By.TAG_NAME, 'body')
        self.driver.execute_script("arguments[0].scrollIntoView();", top_element)

        # 상대 좌표로 이동
        ActionChains(self.driver).move_to_element(top_element).move_by_offset(200, 200).click().perform()
        self.driver.implicitly_wait(1)

        self.driver.find_element(By.PARTIAL_LINK_TEXT, "스튜디오").click() # 2023.10.16 인텔리퀀트 사이트 변경. XPATH로 읽어오는 게 안되네
        self.driver.implicitly_wait(5)
      #  / html / body / div[1] / div[2] / div / div[2] / div[7] / ul / li[3] / a
      #  / html / body / div[1] / div[2] / div / div[2] / div[7] / ul / li[3] / a
       # body > div.algorithms > div.container - custom.content - sm > div > div.col - md - 10 > div.text - center > ul > li.active > a
        try:
            #self.driver.implicitly_wait(5)
            #self.driver.find_element(By.LINK_TEXT, "1").click()
            self.driver.find_element(By.XPATH, page).click()  # 페이지 누르기
            self.driver.implicitly_wait(5)
        except ElementNotInteractableException:
            print("Element is not interactable, moving on.") # 페이지가 1 이면 click()이 안된다.

        print(name)
        self.driver.find_element(By.LINK_TEXT, name).click() # 알고리즘으로 이동. selenium 4.10 버전으로 오면서 형식 변경
        logger.info("Chrome 켜기 완료") #이거 저장 설정 해야한다.

    def update_code(self, js_code):
        pyperclip.copy(js_code)
        self.driver.implicitly_wait(3)
        element = self.driver.find_element(By.CLASS_NAME, 'cm-comment')
        actions = ActionChains(self.driver)
        self.driver.implicitly_wait(3)
        actions.click(element).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).send_keys(Keys.DELETE).key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL)
        self.driver.implicitly_wait(3)
        actions.perform()
        self.driver.find_element(By.XPATH, "//*[@id='editor']/div[1]/span/button[2]").click()  # 저장 버튼
        self.driver.implicitly_wait(3)
        self.driver.find_element(By.XPATH, "/html/body/div[12]/div/div[1]/div/div/div[2]/button").click()  # 저장 후 팝업에서 OK 버튼

    def backtest(self, start_date_str, end_date_str, simul_money_str, logger): # 시작일, 종료일 따로 입력하도록 수정 필요
        # 날짜, 금액 입력, 백테스트 시작

        # selenium 4.10 버전으로 오면서 형식 변경
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(start_date_str)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(Keys.ENTER)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(end_date_str)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(Keys.ENTER)
        self.driver.find_element(By.XPATH, "//*[@id='aum_input']").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element(By.XPATH, "//*[@id='aum_input']").send_keys(simul_money_str)
        self.driver.find_element(By.XPATH, "//*[@id='aum_input']").send_keys(Keys.ENTER)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[2]/button").click()  # backtest 시작

        time.sleep(3)

        try:
            element = WebDriverWait(self.driver, 1200).until(
                #EC.text_to_be_present_in_element((By.XPATH, "//*[@id='board']/div[1]/span[2]/button"), "백테스트 시작")
                EC.text_to_be_present_in_element((By.XPATH, "//*[@id='board']/div[3]/div[2]"), "simulation complete")
            )
        finally:
            list = self.get_backtest_result()

        logger.info("Backtest 시뮬레이션 완료")
        return list

    '''
    def get_backtest_result(self): # 이건 그냥 다 긁어오는 것으로 하고, GetCompensationData에서 처리한다
        # simul_list = self.driver.find_elements_by_class_name('LogItem.info')
        simul_list = self.driver.find_elements(By.CLASS_NAME, 'LogItem.info') # selenium 4.10 버전으로 오면서 형식 변경
        backtest_list = []
        for item in simul_list:
            string = item.text.strip()
            string2 = string.split()  # ['[2022-03-29]', '형지I&C:1373']
            string3 = string2[1]  # 형지I&C:1373
            if string3 != 'compile' and string3 != 'initialize' and string3 != 'initOrlandoSimulator' and string3 != 'init' and string3 != 'simulation':
                backtest_list.append(string + '\n')
        return backtest_list
    '''

    def get_backtest_result(self): # 이건 그냥 다 긁어오는 것으로 하고, GetCompensationData에서 처리한다
        simul_list = self.driver.find_elements(By.CLASS_NAME, 'LogItem.info') # selenium 4.10 버전으로 오면서 형식 변경
        backtest_list = [element.text for element in simul_list]
        return backtest_list