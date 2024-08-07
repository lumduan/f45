
import json
import time
from datetime import datetime
from pymonad.tools import curry
from pymonad.either import Left, Right
import sys
import certifi
import os
import re
from io import StringIO
import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from sel.sel import RemoteWebDriver
from mongo.mongo import MongoDBManager

class ScrapeSetF45:
    
    def __init__(self) -> None:
        pass
    
    def set_url(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Setting F45 URL')
            data['url'] = 'https://www.set.or.th/en/market/news-and-alert/news?source=company&securityType=S&keyword=F45'
            # data['url'] = 'https://www.set.or.th/en/market/news-and-alert/news?source=company&securityType=S&keyword=F45&fromDate=2024-07-01&toDate=2024-08-05'
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in setting F45 URL: {str(e)}')
    
    def set_xpath(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Setting F45 Xpath')
            data['xpath'] = {
                'headline_input_box' : '/html/body/div[1]/div/div/div[2]/div[2]/div[2]/div/div/div[1]/div[1]/div/div[3]/input',
                'search_button' : '//button[text()="         Search       "]',
            }
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in setting F45 Xpath: {str(e)}')
    
    def set_class_name(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Setting F45 Class Name')
            data['class_name'] = {
                'card_quote_news' : 'card-quote-news-contanier',
                'search_button' : 'btn-search-clear',
                'f45_text' : 'raw-html-new'
            }
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in setting F45 Class Name: {str(e)}')
        
        
    def open_web_browser(self, data:dict)->[Left, Right]: # type: ignore
        try:
            print('Opening Web Browser')
            webdriver = RemoteWebDriver()
            webdriver.InitializeWebDriver()
            
            data['webdriver'] = webdriver.driver
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in opening Web Browser: {str(e)}')
        
    def maximize_window(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Maximizing Window')
            webdriver = data['webdriver']
            webdriver.maximize_window()
            webdriver.set_window_size(1450, 3000)
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in maximizing Window: {str(e)}')
    
    def go_url(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Going to URL')
            webdriver = data['webdriver']   
            url = data['url']
            print(f'> Going to {data["url"]}')
            webdriver.implicitly_wait(5)
            webdriver.get(url)
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in going to URL: {str(e)}')
    
    def fill_headline_input_box(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Filling Headline Input Box')
            webdriver = data['webdriver']
            key_to_send = 'F45'
            
            xpath_headline_input_box = data['xpath']['headline_input_box']
            
            headline_input_box_element = WebDriverWait(webdriver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath_headline_input_box))
            )
            
            headline_input_box_element.send_keys(key_to_send)
            return Right(data)

        except Exception as e:
            return Left(f'Error in filling Headline Input Box: {str(e)}')
    
    def click_search_button(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Clicking Search Button')
            webdriver = data['webdriver']
            css_search_button = '.btn.fs-24px.px-4.btn-primary'
            
            search_button_element = WebDriverWait(webdriver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_search_button)))
            
            time.sleep(3)
            search_button_element.click()
            return Right(data)

        except Exception as e:
            return Left(f'Error in clicking Search Button: {str(e)}')
    
    def get_card_quote_news_elements(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Getting Card Quote News Element')
            webdriver = data['webdriver']
            data['elements'] = {}
            class_name_card_quote_news = data['class_name']['card_quote_news']
            
            WebDriverWait(webdriver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, class_name_card_quote_news))
            )
            
            card_quote_news_elements = webdriver.find_elements(By.CLASS_NAME, class_name_card_quote_news)
            
            data['elements']['card_quote_news'] = card_quote_news_elements
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in getting Card Quote News Element: {str(e)}')

    def extract_quote_news_elements(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Extracting Quote News Elements')
            elements = data['elements']['card_quote_news']
            data['f45s'] = []
            
            
            for element in elements:
                # Split the string by newline characters
                parts:str = element.text.split('\n')
                f45 = {}
                f45['date'] = parts[0]
                f45['time'] = parts[1]
                f45['symbol'] = parts[2]
                
                print(f'> Extracting Quote News Elements for {f45["symbol"]}')
                
                i:int = 0 
                if not re.match(r'^[A-Za-z0-9]+$', parts[3][0]):
                    i = 1
                
                f45['source'] = parts[3+i]    
                f45['headline'] = parts[4 + i]
                
                # Add the element to the list of extracted elements
                f45['element'] = element
                
                # print(f'Date: {f45["date"]} , Time: {f45["time"]} , Symbol: {f45["symbol"]} , Source:{f45["source"]} , Headline: {f45["headline"]}')
                data['f45s'].append(f45)
                
            return Right(data)
                
        except Exception as e:
            return Left(f'Error in extracting Quote News Elements: {str(e)}')
    
    def covert_date_time(self, data:dict)->[Left, Right]:    # type: ignore
        try:
            print('Converting Date Time')
            f45s = data['f45s']
            data['f45s'] = []
            
            for f45 in f45s:
                print(f'> Converting Date Time for {f45["symbol"]}')
                
                date = f45['date']
                time = f45['time']
                
                # Check if the date is 'Today' and replace it with the current date
                if 'Today' in date:
                    date = datetime.now().strftime("%d %b %Y")
                    
                # Define the format of the input date and time strings
                date_format = "%d %b %Y"
                time_format = "%H:%M"
                
                # Parse the date and time strings into datetime objects
                parsed_date = datetime.strptime(date, date_format)
                parsed_time = datetime.strptime(time, time_format).time()
                
                # Combine the date and time into a single datetime object
                combined_datetime = datetime.combine(parsed_date, parsed_time)
                
                f45['iso_date'] = combined_datetime.isoformat()
                
                data['f45s'].append(f45)
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in converting Date Time: {str(e)}')
    
    def connect_mongo_db(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Connecting to MongoDB')
            f45s = data['f45s']
            mongo = MongoDBManager('stockThai','f45')
            data['mongo'] = mongo
            
            return Right(data)

        except Exception as e:
            return Left(f'Error in connecting to MongoDB: {str(e)}')
    
    
    def compare_period_in_db(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Comparing Period')
            
            f45s = data['f45s']
            f45_to_update = []
            mongo = data['mongo']
            collection = mongo.collection
            
            for f45 in f45s:
                symbol = f45['symbol']
                print(f'> Comparing Period for {symbol}')
                
                iso_date = f45['iso_date']
                query = {'symbol': symbol}
                f45_in_db = collection.find_one(query)

                if f45_in_db is None or iso_date != f45_in_db['last_update']:
                    f45_to_update.append(f45)
                    # print(f'F45 to Update: {f45}')
            
            data['f45_to_update'] = f45_to_update
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in comparing Period: {str(e)}')
    
    def open_f45_page_get_text(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Opening F45 Page')
            
            webdriver = data['webdriver']
            f45_text_classname = data['class_name']['f45_text']
            f45_to_update = data['f45_to_update']
            data['f45_to_update'] = []
            
            for f45 in f45_to_update:
                print(f'> Opening F45 Page for {f45["symbol"]}')
                element = f45['element']
                element.click()
                
                webdriver.switch_to.window(webdriver.window_handles[1])
                f45['text'] = WebDriverWait(webdriver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, f45_text_classname))
                ).text

                data['f45_to_update'].append(f45)
                webdriver.close()
                webdriver.switch_to.window(webdriver.window_handles[0])
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in opening F45 Page: {str(e)}')
    
    def split_row_from_text(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Splitting Row from Text')
            f45_to_update:list = data['f45_to_update']
            data['f45_to_update'] = []
            
            for f45 in f45_to_update:
                print(f'> Splitting Row from Text for {f45["symbol"]}')
                
                f45_data = {}
                row_quarter = None
                row_year = None
                row_net_profit = None
                row_eps = None
                
                rows:list = f45['text'].split('\n')
                
                for row in rows:
                    if 'Quarter' in row or '12 Months' in row:
                        row_quarter = row.strip()
                    
                    elif '  Year  ' in row:
                        row_year = row.strip()
                        
                    elif 'Profit (loss)' in row or 'Increase (decrease)' in row:
                        row_net_profit = row.strip()
                    
                    elif 'EPS' in row:
                        row_eps = row.strip()
                
                # Check if the quarter is '12' (Year) and replace it with '4' (Quarter 4)
                if '12' in row_quarter:
                    row_quarter = 'Quarter 4'

                f45_data['symbol'] = f45['symbol']
                f45_data['last_update'] = f45['iso_date']
                f45_data['quarter'] = row_quarter
                f45_data['year'] = row_year
                f45_data['net_profit'] = row_net_profit
                f45_data['eps'] = row_eps
                
                data['f45_to_update'].append(f45_data)
                
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in splitting Row from Text: {str(e)}')
        
    def extract_numbers_from_rows(self, data:dict)->[Left, Right]:    # type: ignore
        try:
            def convert_spaces(s: str) -> str:
            # Replace sequences of spaces longer than two with double spaces
                return re.sub(r'\s{3,}', '  ', s)
    
            print('Extracting Numbers from Rows')
            f45_to_update = data['f45_to_update']
            data['f45_to_update'] = []
            
            for f45 in f45_to_update:
                f45_data = {}
                print(f'> Extracting Numbers from Rows for {f45["symbol"]}')
                
                f45_data['symbol'] = f45['symbol']
                f45_data['last_update'] = f45['last_update']
                
                #convert space to double
                f45['net_profit'] = convert_spaces(f45['net_profit'])
                f45['eps'] = convert_spaces(f45['eps'])
                
                # Split the string by newline characters
                f45_data['quarter'] = f45['quarter'].split()
                f45_data['year'] = f45['year'].split()
                f45_data['net_profit'] = f45['net_profit'].split('  ')
                f45_data['eps'] = f45['eps'].split('  ')
                
                f45_data['quarter'] = f45_data['quarter'][1]
                f45_data['year'] = f45_data['year'][1]
                f45_data['net_profit'] = f45_data['net_profit'][1]
                f45_data['eps'] = f45_data['eps'][1]
                
                data['f45_to_update'].append(f45_data)
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in extracting Numbers from Row: {str(e)}')
    
    def clean_f45_data(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Cleaning F45 Data')
            f45_to_update = data['f45_to_update']
            data['f45_cleaned_data'] = []
            
            for f45 in f45_to_update:
                print(f'> Cleaning F45 Data for {f45["symbol"]}')
                
                symbol = f45['symbol']
                last_update = f45['last_update']
                quarter = f45['quarter']
                year = f45['year']
                net_profit = f45['net_profit']
                eps = f45['eps']
                
                # Remove text from the numbers
                net_profit = net_profit.replace('(Update)', '')
                net_profit = net_profit.replace(',', '')
                net_profit = net_profit.replace('(', '-')
                net_profit = net_profit.replace(')', '')
                eps = eps.replace('(Update)', '')
                eps = eps.replace(',', '')
                eps = eps.replace('(', '-')
                eps = eps.replace(')', '')
                
                # Convert the str to  numbers
                quarter = int(quarter)
                year = int(year)
                net_profit = float(net_profit)
                eps = float(eps)
                
                cleaned_data = {
                    'symbol': symbol,
                    'last_update': last_update,
                    'quarter': quarter,
                    'year': year,
                    'net_profit': net_profit,
                    'eps': eps
                }
                
                data['f45_cleaned_data'].append(cleaned_data)
                
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in cleaning F45 Data: {str(e)}')
    
    def update_db(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Updating DB')
            f45_cleaned_data = data['f45_cleaned_data']
            mongo = data['mongo']
            collection = mongo.collection
            
            for f45 in f45_cleaned_data:
                symbol = f45['symbol']
                query = {'symbol': symbol}
                update = {
                    'symbol': symbol,
                    'last_update': f45['last_update'],
                    'quarter': f45['quarter'],
                    'year': f45['year'],
                    'net_profit': f45['net_profit'],
                    'eps': f45['eps']
                }
                collection.update_one(query, {'$set': update}, upsert=True)
                print(f'> Updated DB for {symbol}')
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in updating DB: {str(e)}')
    
    def close_mongo_db(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Closing MongoDB')
            mongo = data['mongo']
            mongo.client.close()
            print('MongoDB Closed')
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in closing MongoDB: {str(e)}')

    def close_web_browser(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Closing Web Browser')
            webdriver = data['webdriver']
            webdriver.quit()
            print('Web Browser Closed')
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in closing Web Browser: {str(e)}') 
    
    def export_data_to_json(self, data: dict)->[Left, Right]:    # type: ignore
        try:
            print('Exporting Data to JSON')
            json_filename:str = 'f45_data.json'
            f45_cleaned_data = data['f45_cleaned_data']
            
            folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), './report'))
            file_path = os.path.join(folder_path, json_filename)  # Create the file path
            
            
            if f45_cleaned_data == []:
                if file_path in os.listdir(folder_path):
                    os.remove(file_path) 
                return Left('No data to export')
            
            
            with open(file_path, 'w') as f:
                json.dump(f45_cleaned_data, f, ensure_ascii=False, indent=4)
            with open('f45_data.json', 'w') as file:
                file.write(f45_cleaned_data)
            
            return Right(data)
        
        except Exception as e:
            return Left(f'Error in exporting Data to JSON: {str(e)}')
    
    def main(self):
        data = {}
        data['webdriver'] = None

        result = (
            self.set_url(data)
            .then (self.set_xpath)
            .then (self.set_class_name)
            .then (self.open_web_browser)
            .then (self.maximize_window)
            .then (self.go_url)
            # .then (self.fill_headline_input_box)
            .then (self.click_search_button)
            .then (self.get_card_quote_news_elements)
            .then (self.extract_quote_news_elements)
            .then (self.covert_date_time)
            .then (self.connect_mongo_db)
            .then (self.compare_period_in_db)
            .then (self.open_f45_page_get_text)
            .then (self.close_web_browser)
            .then (self.split_row_from_text)
            .then (self.extract_numbers_from_rows)
            .then (self.clean_f45_data)
            .then (self.update_db)
            .then (self.close_mongo_db)
            .then (self.export_data_to_json)
        )

        if result.monoid[1] == True:
            # return result.value['webdriver']
            return True
        else:
            # print(result)
            return False


if __name__ == '__main__':
    ScrapeSetF45().main()
    
    