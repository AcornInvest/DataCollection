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
import pyperclip
import threading


class Intelliquant:
    def __init__(self, num_process):
        self.load_config() # 설정 로드
        self.num_process = num_process

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
        self.path_chrome = [config['path']['path_chrome_0'], config['path']['path_chrome_1'], config['path']['path_chrome_2'], config['path']['path_chrome_3']]
        self.port = [config['path']['port_0'], config['path']['port_1'], config['path']['port_2'], config['path']['port_3']]
        self.argument = [config['path']['argument_0'], config['path']['argument_0'], config['path']['argument_0'], config['path']['argument_0']]

    def chrome_on(self, logger, page, name):
        subprocess.Popen(self.path_chrome[self.num_process])  # 디버거 크롬 구동

        option = Options()
        option.add_experimental_option("debuggerAddress", self.port[self.num_process])
        option.add_argument(self.argument[self.num_process])

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
        logger.info("Backtest 시뮬레이션 시작")
        time.sleep(3)

        backtest_list = []
        complete = False
        '''
        # 스레드 종료를 위한 이벤트
        stop_event = threading.Event()
        last_log_id = 0  # 로그 수집을 시작할 초기 위치

        # 로그 수집 스레드 시작
        log_thread = threading.Thread(target=self.collect_logs, args=(self.driver, stop_event, last_log_id, backtest_list))
        log_thread.start()

        # 종료 조건 확인 스레드 시작
        wait_thread = threading.Thread(target=self.wait_for_completion, args=(self.driver, stop_event))
        wait_thread.start()

        # 스레드가 완료될 때까지 기다림
        log_thread.join()
        wait_thread.join()
        '''

        last_log_id = 0
        while complete is False:
            log_container = self.driver.find_element(By.CLASS_NAME, "ConsoleLog.show")  # 로그 데이터가 표시되는 컨테이너의 ID
            logs = log_container.find_elements(By.CLASS_NAME, "LogItem.info")  # 로그 라인을 식별할 셀렉터
            new_logs = logs[last_log_id:]  # 마지막 로그 이후로 새로운 로그만 처리

            for log in new_logs:
                backtest_list.append(log.text)
                if "simulation complete" in log.text:
                    complete = True

            # 마지막 로그 위치 업데이트
            last_log_id = len(logs)
            time.sleep(0.5)  # 너무 빈번한 확인을 방지하기 위해 적당한 휴식 시간을 설정

        logger.info("Backtest 시뮬레이션 완료")
        return backtest_list

    def get_backtest_result(self): # 이건 그냥 다 긁어오는 것으로 하고, GetCompensationData에서 처리한다
        simul_list = self.driver.find_elements(By.CLASS_NAME, 'LogItem.info') # selenium 4.10 버전으로 오면서 형식 변경
        backtest_list = [element.text for element in simul_list]
        return backtest_list

    def collect_logs(self, driver, stop_event, last_log_id, backtest_list):
        while not stop_event.is_set():
            log_container = driver.find_element(By.CLASS_NAME, "ConsoleLog.show")  # 로그 데이터가 표시되는 컨테이너의 ID
            logs = log_container.find_elements(By.CLASS_NAME, "LogItem.info")  # 로그 라인을 식별할 셀렉터
            new_logs = logs[last_log_id:]  # 마지막 로그 이후로 새로운 로그만 처리

            for log in new_logs:
                backtest_list.append(log.text)

            # 마지막 로그 위치 업데이트
            last_log_id = len(logs)
            time.sleep(0.5)  # 너무 빈번한 확인을 방지하기 위해 적당한 휴식 시간을 설정

        return last_log_id  # 종료 시 마지막 로그 위치 반환

    def wait_for_completion(self, driver, stop_event):
        # "simulation complete" 메시지가 나타날 때까지 기다립니다.
        WebDriverWait(driver, 1200).until(
            EC.text_to_be_present_in_element((By.XPATH, "//*[@id='board']/div[3]/div[2]"), "simulation complete")
        )
        stop_event.set()  # 로그 수집을 중단하기 위해 이벤트를 설정