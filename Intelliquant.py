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

class Intelliquant:
    def __init__(self):
        self.LoadConfig() # 설정 로드

    def __del__(self):
        ''' # 나중에 주석 해제하기
        if hasattr(self, 'driver'):
            self.driver.close()
        '''

    def LoadConfig(self): # 설정 불러오기
        self.path = os.getcwd() + '\\' + 'config_Intelliquant.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(self.path, encoding='utf-8')

        # 설정값 읽기
        self.path_chrome = config['path']['path_chrome']
        self.port = config['path']['port']
        self.argument = config['path']['argument']
        #self.page = config['intelliquant']['page']
        #self.name = config['intelliquant']['name']
        self.page = json.loads(config['intelliquant']['page'])
        self.name = json.loads(config['intelliquant']['name'])

    def ChromeOn(self, logger, key):
        subprocess.Popen(self.path_chrome)  # 디버거 크롬 구동

        option = Options()
        #option.add_experimental_option("debuggerAddress", "127.0.0.1:9221")
        option.add_experimental_option("debuggerAddress", self.port)
        #option.add_argument('--user-data-dir="C:\chrometemp_sub"')
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

        #print(self.page[key])
        try:
            self.driver.find_element(By.XPATH, self.page[key]).click()  # 페이지 누르기
            self.driver.implicitly_wait(5)
        except ElementNotInteractableException:
            print("Element is not interactable, moving on.") # 페이지가 1 이면 click()이 안된다.

        self.driver.find_element(By.LINK_TEXT, self.name[key]).click() # 알고리즘으로 이동. selenium 4.10 버전으로 오면서 형식 변경
        logger.info("Chrome 켜기 완료") #이거 저장 설정 해야한다.

    def backtest(self, simul_date_string, simul_money_string, logger):
        # 날짜, 금액 입력, 백테스트 시작
        '''
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(simul_date_string)
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(Keys.ENTER)
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(simul_date_string)
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(Keys.ENTER)
        self.driver.find_element_by_xpath("//*[@id='aum_input']").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element_by_xpath("//*[@id='aum_input']").send_keys(simul_money_string)
        self.driver.find_element_by_xpath("//*[@id='aum_input']").send_keys(Keys.ENTER)
        self.driver.find_element_by_xpath("//*[@id='board']/div[1]/span[2]/button").click() # backtest 시작
        '''

        # selenium 4.10 버전으로 오면서 형식 변경
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(simul_date_string)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[1]/input").send_keys(Keys.ENTER)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(simul_date_string)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[1]/div[3]/input").send_keys(Keys.ENTER)
        self.driver.find_element(By.XPATH, "//*[@id='aum_input']").send_keys(Keys.CONTROL + 'a')
        self.driver.find_element(By.XPATH, "//*[@id='aum_input']").send_keys(simul_money_string)
        self.driver.find_element(By.XPATH, "//*[@id='aum_input']").send_keys(Keys.ENTER)
        self.driver.find_element(By.XPATH, "//*[@id='board']/div[1]/span[2]/button").click()  # backtest 시작

        time.sleep(3)

        try:
            element = WebDriverWait(self.driver, 20).until(
                #EC.text_to_be_present_in_element((By.XPATH, "//*[@id='board']/div[1]/span[2]/button"), "백테스트 시작")
                EC.text_to_be_present_in_element((By.XPATH, "//*[@id='board']/div[3]/div[2]"), "simulation complete")
            )
        finally:
            list = self.get_backtest_result()

        logger.info("Backtest 시뮬레이션 완료")
        return list

    def get_backtest_result(self):
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