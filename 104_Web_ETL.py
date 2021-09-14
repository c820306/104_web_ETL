import requests 
from bs4 import BeautifulSoup
import json
import time
import os
import random
import pandas as pd

def crawl_104HR():
    t1 = time.time()
    df = pd.DataFrame()
    searchKeyword = input("請輸入查詢關鍵字:")
    # searchKeyword ='大數據分析'
    userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    headers = {'User-Agent':userAgent}  #偽裝成非爬蟲，將資料更改成字典回傳給網站
    ss = requests.session()

    pages = int(input("請輸入需要撈取104的頁數:"))
    index = 0

    for i in range (1, pages + 1):
        #F12 XHR內透過動態網頁裡的網址，並找到變數與網址間的關係
        url ='https://www.104.com.tw/jobs/search/?ro={}&kwop={}&keyword={}&expansionType=area%2Cspec%2Ccom%2Cjob%2Cwf%2Cwktm&area={}&order={}&asc={}&page={}&mode={}&jobsource={}'.format(0,7,searchKeyword,'6001001000%2C6001002000%2C6001005000',15,0,i,'s','2018indexpoc')
        urlres = ss.get(url, headers = headers)
        soup = BeautifulSoup(urlres.text, 'html.parser')
        jobContents = soup.select('h2 a') #指令104工作頁面的網址

        print("第{}頁".format(i))
        for jobtContent in jobContents:
            #點進工作介紹內容同樣無法定位，需至F12找ajax網頁是json格式，發現數字與工作瀏覽網址相關
            #網頁為js render，無法用一般標籤定位，須至F12 XHR內找到此網頁ID的網址
            jobUrlTag= 'https:' + jobtContent['href']  #查詢結果指定網頁連結的標籤位置 ->取工作頁面的ID號碼
            jobUrl_ID = jobUrlTag.split('?')[0].split('/')[4]
            url = 'https://www.104.com.tw/job/ajax/content/{}'.format(jobUrl_ID) #取得F12的ajax網址

            headers = {"Referer": "https://www.104.com.tw/job/ajax/content/{}'.format(jobUrlTmp)"}
            response = requests.get(url = url, headers = headers)
            jsonData = json.loads(response.text)   #取得JSON格式

            index += 1

            df.loc[index,'更新日期'] = jsonData['data']['header']['appearDate'][5:]
            df.loc[index,'職缺名稱'] = jsonData['data']['header']['jobName']
            df.loc[index,'公司名稱'] = jsonData['data']['header']['custName']
            df.loc[index,'工作內容'] = jsonData['data']['jobDetail']['jobDescription'].replace('\n', ' ').replace('\r',' ')
            df.loc[index,'職務類別'] = '/'.join(str(i['description']) for i in jsonData['data']['jobDetail']['jobCategory'])
            df.loc[index,'薪水'] = jsonData['data']['jobDetail']['salary']
            df.loc[index,'工作地點'] = str(jsonData['data']['jobDetail']['addressRegion'])+ jsonData['data']['jobDetail']['addressDetail']\
                                        + jsonData['data']['jobDetail']['industryArea']

            Job_Role_raw = jsonData['data']['condition']['acceptRole']['role'] 
            df.loc[index,'接受身份'] = "/".join(str(Job_Role_raw[i]['description']) for i in range(len(Job_Role_raw)))

            df.loc[index,'學歷要求'] = jsonData['data']['condition']['edu']

            Job_Major_raw = jsonData['data']['condition']['major'] 
            df.loc[index,'科系要求'] = "/".join(str(Job_Major_raw[i]) for i in range(len(Job_Major_raw)))
            df.loc[index,'工作經歷'] = jsonData['data']['condition']['workExp']
            df.loc[index,'公司福利'] = "/".join(str(i) for i in jsonData['data']['welfare']['tag'])
            df.loc[index,'聯絡對象'] = jsonData['data']['contact']['hrName']
            df.loc[index,'104連結'] = jobUrlTag
            df.loc[index,'公司網站'] = jsonData['data']['header']['custUrl']

            #以下處理工作技能資料
            SkillSet = {'Python': 0 ,'Linux':0, 'Data mining':0, 'Google Analytics': 0
                        ,'AI': 0 ,'Machine learning': 0, 'Deep learning': 0,'Tensorflow': 0
                        ,'MySQL': 0,'MS_SQL': 0,'PostgreSQL': 0,'hadoop': 0 , 'Hive': 0,'SparkMysql': 0
                        ,'Big data': 0 ,'JavaScript': 0, 'Cloud service': 0, 'Java': 0 
                        ,'Cloud service': 0,'NoSQL': 0 , 'AWS': 0 ,'AZURE': 0 , 'Oracle':0 
                        ,'ETL':0 , 'Git': 0, 'Tableau': 0, 'SAS':0  , 'SPSS':0 , 'R':0 
                       }

            Job_Specialty = []
            Job_Specialty_raw = jsonData['data']['condition']['specialty']  #擅長工具
            Job_Specialty += [Job_Specialty_raw[i]['description'] for i in range(len(Job_Specialty_raw))]
            for skill in Job_Specialty:
                if skill in SkillSet:
                    SkillSet[skill] += 1

            for key, value in SkillSet.items():
                df.loc[index, key] = str(value)

            df.loc[index,'其他條件'] = jsonData['data']['condition']['other'].replace('\r','').replace('\n','')

    df = df.sort_values(by = ['更新日期'],ascending=False)
    df.to_csv("./104人力爬蟲結果.csv", index = False, encoding = 'utf-8')

    spend = time.time() - t1
    print(f"Total spending {round(spend/60,2)} mins" )
    return df

if __name__ == '_main_':
    crawl_result = crawl_104HR()