import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv


"""Алгоритм скрипта:
   Удаление датасета прошлого дня и скачивание датасета за текущий день.
"""

# Импорт password и path для авторизации.
ODDO_URL = os.getenv('ODDO_URL')
ODDO_LOGIN = os.getenv('ODDO_LOGIN')
ODOO_PASSWORD = os.getenv('ODOO_PASSWORD')

PATH_TO_XLSX = os.getenv('PATH_TO_XLSX')
PATH_TO_CSV = os.getenv('PATH_TO_CSV') 


def del_files():
    """Удаление датасета прошлой недели."""
    files_XLSX = os.listdir(PATH_TO_XLSX)
    for file_XLSX in files_XLSX:
        if file_XLSX.endswith('.xlsx'):
            full_file_path = os.path.join(PATH_TO_XLSX, file_XLSX)
            try:
                os.remove(full_file_path)
                print(f'Файл успешно удалён {file_XLSX}')
            except Exception as e:
                print('Файл отсутсвует: {e}')
    
    # Удаление raw_data.csv.
    try:
        os.remove(PATH_TO_CSV)
        print(f'Файл успешно удалён: {PATH_TO_CSV}')
    except Exception as e:
        print('Файл отсутсвует: {e}')
    

def load_dataset():
    """Загрузка Датасета из Odoo .XLSX"""
    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        'download.default_directory': r'C:\Users\ПК48\Desktop\Dev\AsmblDataWarehouse\tmp_data', # Путь в папку с проектом.
        'download.prompt_for_download': False

    })
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    driver.get(ODDO_URL)
    actions = ActionChains(driver)
    time.sleep(2)

    find_login = driver.find_element(By.ID, 'login')
    find_password = driver.find_element(By.ID, 'password')
    find_login.send_keys(ODDO_LOGIN)
    find_password.send_keys(ODOO_PASSWORD)
    time.sleep(2)

    driver.find_element(By.XPATH, '//button[.="Войти"]').click()
    time.sleep(5)
    driver.find_element(By.CLASS_NAME, 'dropdown-toggle').click()
    time.sleep(5)
    driver.find_element(By.XPATH, '//a[.="Производство"]').click()
    time.sleep(5)
    driver.find_element(By.XPATH, '//span[.="Избранное"]').click()
    time.sleep(5)
    driver.find_element(By.XPATH, '//span[.="Выработка производства"]').click() 
    time.sleep(5)
    filter = driver.find_element(By.XPATH, '//span[.="Фильтры"]')
    actions.move_to_element(filter).perform()
    time.sleep(5)
    driver.find_element(By.XPATH, '//span[.="Произведено за сегодня"]').click()
    #driver.find_element(By.XPATH, '//span[.="Произведено за два дня"]').click()  
    time.sleep(5)
    driver.find_element(By.XPATH, '//button[@class="btn btn-light o_switch_view o_pivot oi oi-view-pivot"]').click()
    time.sleep(5)
    driver.find_element(By.XPATH, '//button[@class="btn btn-secondary fa fa-download o_pivot_download"]').click()
    time.sleep(20)


if __name__=="__main__":
    load_dotenv()
    del_files()
    load_dataset()


    
    