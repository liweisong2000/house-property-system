#-*-codeing = utf-8-*-

#主程序

import time
import random
import requests
from  bs4 import BeautifulSoup
import re
# import urllib.request
# import urllib.error
# import xlwt
# import sqlite3
import threading
# from concurrent.futures import ThreadPoolExecutor
from multiprocessing.dummy import Pool
from lxml import etree
# from PIL import Image
# from io import BytesIO
import pymssql
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver import ChromeOptions
# import pytesseract
from PIL import Image
from hashlib import md5
import os
from selenium.webdriver.common.action_chains import ActionChains

#正则表达式匹配串
findHref=re.compile(r'href="/loupan(.+?)"') #用于匹配每个房产的链接的正则表达式
findName = re.compile(r'<a class="name".*target="_blank">(.+)?</a>')  #用于匹配房产的名字
findTownAddress=re.compile(r'<span>(.+?)</span>') #用于匹配房产处在的镇
findaAddress = re.compile(r'<a data-other-action.*target="_blank">(.+?)</a>') #用于匹配房产的地址
findArea=re.compile(r'<span>建面(.*)</span>') #用于匹配房产的建地面积
findHomeClass=re.compile(r'<span class="resblock-type".*;">(.+?)</span>') #用于匹配房产的类型
findHomeState=re.compile(r'<span class="sale-status".*;">(.+?)</span>') #用于匹配房产的状态
findHomePrice=re.compile(r'<span class="number">(.+?)</span>')  #用于匹配每个房产的平均每平方米的价格
findHomeTotalPrice=re.compile(r'<div class="second">(.+?)</div>')#用于匹配每个房产的的总价，！！！当平均价格为”价格待定“时是为空的
findHouseLabel=re.compile(r'<div class="resblock-tag">(.+?)</div>',re.DOTALL) #用于匹配每个房产的标签
findIp=re.compile(r'(.+?)@HTTP.?')  #用于匹配ip，ip池中使用
#楼房动态使用的正则表达式
findDynamicType=re.compile(r'<span class="a-tag">(.+?)</span>')  #用于匹配楼盘动态类型
findSonType=re.compile(r'<span class="a-title">(.+?)</span>')  #用于匹配子动态类型
findDynamicTime=re.compile(r'<span class="a-time">(.+?)</span>')  #用于匹配动态的日期时间
findDynamicContent=re.compile(r'<a href=".+?">(.+?)</a>',re.DOTALL)  #用于匹配动态的内容
#楼盘户型使用的正则表达式
findHuXingType=re.compile(r'<li>居室：(.+?)</li>')  #用于匹配居室类型
findHuXingArea=re.compile(r'<li>建面：(.+?)</li>')  #用于匹配居室建面
findHuXingPrice=re.compile(r'<span class="price">(.+?)</span>')  #用于匹配居室的均价
findHuXingState=re.compile(r'<span class="status.+?">(.+?)</span>')  #用于匹配居室的销售状态
findHuXingImgURl=re.compile(r'<img src="(.+?)".?>')  #用于匹配居室的图片路径
#楼盘相册使用的正则表达式
findImgName=re.compile(r'<h4><a href=".+?">(.+)</a></h4>')  #匹配图片名字，属于那一栏的图片
findImgUrl=re.compile(r'<img src="(.+?)"/>')  #匹配图片的路径
#楼盘周边使用的正则表达式
findTheme=re.compile(r'<div class="itemTitle"><span>(.+?)</span></div>')  #用于查找周边设施名字
findBusDetails=re.compile(r'<span class="bus-one bordered">(.+?)</span>')  #用于查找公交的详情
findDetails=re.compile(r'<div class="itemInfo">(.+?)</div>')   #用于查找除公交外周边设施详情

ipHtml=[]  #存储需爬的ip网址
ipList=[]  #存储IP列表
trueIp=[]  #存储真正可使用的IP
inndexHtml=[]  #存储每一页的网址
da=[] #存取每个房产详细信息的链接
homeAlbumHtml=[] #存取每个房产的相册的链接
houseTypeHtml=[] #存取每个房产的户型的链接
homeDynamicHtml=[]  #存取每个房产的楼盘动态的链接
homeNearbyHtml=[]  #存取每个房产的周边设施的链接
datr=[] #存放每个房产的详细信息，每个列表是一个房产；第一个是名字，第二个是具体地址
houseDongTa=[]   #存储每个房产的动态消息
houseType=[]  #存储户型信息
houseXiangce=[]  #存储每个房产的相册
periphteryList=[] #存储每个房产周边
coordinateData=[] #存储由超级鹰返回的坐标--解封网站对主机IP的封闭

# #无头浏览器多开
# chrome_options=Options()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--disable-gpu')
# option=ChromeOptions()
# option.add_experimental_option('excludeSwotches',['enable-automation'])
# driv1=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv2=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv3=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv4=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv5=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv6=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv7=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)
# driv8=webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options,options=option)

#获取代理IP
def ipx4():
    trueIp.clear()
    url = 'http://webapi.http.zhimacangku.com/getip?num=40&type=1&pro=&city=0&yys=0&port=1&pack=222264&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
    htmltext = requests.get(url=url).text
    ipAll = str(htmltext).split("\r\n")
    for i in ipAll[0:len(ipAll)-2]:
        trueIp.append(i)

#获取每一页的网址
def htmlNum(url):
    htmlText = Url(url)
    while(htmlText is None or re.search(r"(.*人机认证.*)", htmlText) is not None):
        htmlText = Url(url)
    etre = etree.HTML(htmlText)
    Num = etre.xpath('/html/body/div[3]/div[2]/div/span[2]/text()')[0]
    print(type(Num))
    print(Num)
    i = int(str(Num)) / 10
    print(i)
    print(int(i))
    for n in range(1, int(i) + 1):
        st = url + "pg" + str(n) + "/"
        print(st)
        inndexHtml.append(st)

#爬取每一页，获得每个房源的详情网址
def homeHTtml(url):
    htmlText=Url(url)
    while (htmlText is None or re.search(r"(.*人机认证.*)", htmlText) is not None):
        htmlText = Url(url)
    data(htmlText)

#获得每个房产的动态详情、
def houseDongTaData(url):
    try:
        htmlText = Url(url)
        while (htmlText is None or re.search(r"(.*人机认证.*)", htmlText) is not None):
            htmlText = Url(url)
        dongta=[]
        etr=etree.HTML(htmlText)
        dongHtml = etr.xpath('/html/body/div[2]/div/div/a[6]/@href')[0]
        dongHtml = "https://dg.fang.lianjia.com/" + dongHtml
        textNum = etr.xpath('//div[@class="page-box"]/@data-total-count')[0]
        dongta.append(url)
        num = 0
        bou = BeautifulSoup(htmlText, "html.parser")
        st = str(htmlText)
        while (num != int(textNum)):
            n = 1
            for intm in bou.select('div[class="dongtai-one for-dtpic"]'):
                sta = str(intm)
                num = num + 1
                dynamic = []
                SonType = re.findall(findSonType, sta)[0]
                dynamic.append(SonType)
                DynamicTime = re.findall(findDynamicTime, sta)[0]
                dynamic.append(DynamicTime)
                DynamicContent = re.findall(findDynamicContent, sta)[0]
                dynamic.append(DynamicContent)
                dongta.append(dynamic)
            while (num < int(textNum)):
                n = n + 1
                url = dongHtml + "pg" + str(n)
                htmlText = Url(url)
                while (htmlText is None or re.search(r"(.*人机认证.*)", htmlText) is not None):
                    htmlText = Url(url)
                bou = BeautifulSoup(htmlText, "html.parser")
                for intm in bou.select('div[class="dongtai-one for-dtpic"]'):
                    sta = str(intm)
                    num = num + 1
                    dynamic = []
                    SonType = re.findall(findSonType, sta)[0]
                    dynamic.append(SonType)
                    DynamicTime = re.findall(findDynamicTime, sta)[0]
                    dynamic.append(DynamicTime)
                    DynamicContent = re.findall(findDynamicContent, sta)[0]
                    dynamic.append(DynamicContent)
                    dongta.append(dynamic)
        houseDongTa.append(dongta)
    except:
       print("出错了")

#获得每个房产户型的详情
def dataHuXing(url):
    try:
        htmlText = Url(url)
        while (htmlText is None or re.search(r"(.*人机认证.*)", htmlText) is not None):
            htmlText = Url(url)
        typeall = []
        typeall.append(url)
        bou = BeautifulSoup(htmlText, "html.parser")
        st = str(htmlText)
        for inte in bou.select('li[class="huxing-item"]'):
            stra = str(inte)
            HuXing = []
            type = re.findall(findHuXingType, stra)[0]
            HuXing.append(type)
            area = re.findall(findHuXingArea, stra)[0]
            HuXing.append(area)
            price = re.findall(findHuXingPrice, stra)[0]
            price = str(price).replace(" <i>", "")
            price = price.replace("</i> ", "")
            HuXing.append(price)
            state = re.findall(findHuXingState, stra)[0]
            HuXing.append(state)
            imgurl = re.findall(findHuXingImgURl, stra)[0]
            HuXing.append(imgurl)
            typeall.append(HuXing)
        houseType.append(typeall)

    except:
        print("出错了")
        return 0

#解析数据，获取楼盘相册
def dataHouseXiangCe(url):
    try:
        htmlText = Url(url)
        # print(htmlText)
        while (htmlText is None or re.search(r"(.*人机认证.*)", htmlText) is not None):
            htmlText = Url(url)
        # etr=etree.HTML(htmlText)
        # houseName=etr.xpath('/html/body/div[2]/div/div/a[5]/text()')[0]
        # print(houseName)
        bou = BeautifulSoup(htmlText, "html.parser")
        st = str(htmlText)
        xianCe=[]
        xianCe.append(url)
        for inte in bou.select('div[class="tab-group"]'):
            stra = str(inte)
            xiangName=re.findall(findImgName,stra)[0]
            index = str(xiangName).find('（')
            xiangName = xiangName[0:index]
            # print(xiangName)
            xiangUrl1=re.findall(findImgUrl,stra)
            xiangUrl2=[]
            xiangUrl2.append(xiangName)
            for url in xiangUrl1:
                url=str(url).replace("w_235,h_178","w_1000")
                # print(url)
                xiangUrl2.append(url)
            xianCe.append(xiangUrl2)
        # print(xianCe)
        houseXiangce.append(xianCe)

    except:
        print("出错了")

#获取附近设施
def peripheryText(driver,url,num):
    driver.get(url)
    driver.implicitly_wait(4)
    htmlText=driver.page_source
    while(re.search(r'(.*人机认证.*)',htmlText) is not  None):
        if(num==1):
            crackCode(driver)
            driver.refresh()
            driver.implicitly_wait(4)
            htmlText = driver.page_source
        else:
            time.sleep(3)
            driver.refresh()
            driver.implicitly_wait(4)
            htmlText = driver.page_source
    # htmlText = driver.page_source
    etr=etree.HTML(htmlText)
    num=etr.xpath('//ul[@class="type "]/li/text()')
    for n in num:
        k=num.index(n)+1
        if(k!=1):
            xpath='//*[@id="around"]/div/ul/li['+str(k)+']'
            click=driver.find_element_by_xpath(xpath)
            click.click()
            time.sleep(2)
            htmlText= driver.page_source
            periphteryData(htmlText,n,url)
            time.sleep(2)
        else:
            periphteryData(htmlText,n,url)
            time.sleep(2)

#解析附近设施数据
def periphteryData(htmlText,string,url):
    mediumList=[]
    mediumList.append(url)
    mediumList.append(string)
    bou = BeautifulSoup(htmlText, "html.parser")
    text = bou.select('div[class="item"]')
    if (string == "公交" or string=="地铁"):
        find = findBusDetails
    else:
        find = findDetails
    for g in text:
        data = []
        g = str(g)
        theme = re.findall(findTheme, g)[0]
        # print(theme)
        data.append(theme)
        details = re.findall(find, g)
        for i in details:
                data.append(i)
        mediumList.append(data)
    periphteryList.append(mediumList)

# #selenium多线程爬取每个房产的周边
# def nearbyThread1():
#     for url in homeNearbyHtml[0::8]:
#         print(url)
#         peripheryText(driv1, url,1)
#     driv1.quit()
#
# def nearbyThread2():
#     for url in homeNearbyHtml[1::8]:
#         print(url)
#         peripheryText(driv2, url,2)
#     driv2.quit()
#
# def nearbyThread3():
#     for url in homeNearbyHtml[2::8]:
#         print(url)
#         peripheryText(driv3, url,3)
#     driv3.quit()
#
# def nearbyThread4():
#     for url in homeNearbyHtml[3::8]:
#         print(url)
#         peripheryText(driv4, url,4)
#     driv4.quit()
#
# def nearbyThread5():
#     for url in homeNearbyHtml[4::8]:
#         print(url)
#         peripheryText(driv5, url,5)
#     driv5.quit()
#
# def nearbyThread6():
#     for url in homeNearbyHtml[5::8]:
#         print(url)
#         peripheryText(driv6, url,6)
#     driv6.quit()
#
# def nearbyThread7():
#     for url in homeNearbyHtml[6::8]:
#         print(url)
#         peripheryText(driv7, url,9)
#     driv7.quit()
#
# def nearbyThread8():
#     for url in homeNearbyHtml[7::8]:
#         print(url)
#         peripheryText(driv8, url,8)
#     driv8.quit()

#超级鹰提供的开发文档
class Chaojiying_Client(object):

    def __init__(self, username, password, soft_id):
        self.username = username
        password =  password.encode('utf8')
        self.password = md5(password).hexdigest()
        self.soft_id = soft_id
        self.base_params = {
            'user': self.username,
            'pass2': self.password,
            'softid': self.soft_id,
        }
        self.headers = {
            'Connection': 'Keep-Alive',
            'User-Agent': 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)',
        }

    def PostPic(self, im, codetype):
        """
        im: 图片字节
        codetype: 题目类型 参考 http://www.chaojiying.com/price.html
        """
        params = {
            'codetype': codetype,
        }
        params.update(self.base_params)
        files = {'userfile': ('ccc.jpg', im)}
        r = requests.post('http://upload.chaojiying.net/Upload/Processing.php', data=params, files=files, headers=self.headers)
        return r.json()

    def ReportError(self, im_id):
        """
        im_id:报错题目的图片ID
        """
        params = {
            'id': im_id,
        }
        params.update(self.base_params)
        r = requests.post('http://upload.chaojiying.net/Upload/ReportError.php', data=params, headers=self.headers)
        return r.json()

def chaojiyingrukou():
    chaojiying = Chaojiying_Client('lwg0107', '123456', '928871')  # 用户中心>>软件ID 生成一个替换 96001
    im = open('验证码b.png', 'rb').read()  # 本地图片文件路径 来替换 a.jpg 有时WIN系统须要//
    code = chaojiying.PostPic(im,9004) # 1902 验证码类型  官方网站>>价格体系 3.4+版 print 后要加()
    pic_id = code['pic_id']
    if code['err_str'] == 'OK' and code['err_str'] != '无可用题分':
        print(code['pic_str'])
        return code['pic_str']
    else:
        print('超级鹰出现问题了，请查看')
        return -1

#模仿人工点击验证码实现解封
def crackCode(driver):
    driver.refresh()
    driver.implicitly_wait(4)
    htmlText = driver.page_source
    if re.search(r'(.*人机认证.*)',htmlText) is None:
        return
    codeClick=driver.find_elements_by_xpath('//*[@id="captcha"]/div/div[1]/div/div[2]/div[2]/div[1]/div[3]')[0]
    codeClick.click()
    time.sleep(2)
    codeImg=driver.find_elements_by_xpath('//div[@class="geetest_popup_box"]')[0]
    screenshot_as_bytes=driver.find_elements_by_xpath('//div[@class="geetest_popup_box"]')[0].screenshot_as_png
    determine=driver.find_elements_by_xpath('//div[@class="geetest_commit_tip"]')[0]
    with open('验证码a.png', 'wb') as f:
        f.write(screenshot_as_bytes)
    im=Image.open('验证码a.png')
    region=im.crop((0,0,348,390))
    region.save('验证码b.png')
    result=chaojiyingrukou()
    if result == -1:
        return
    if '|' in result:
        list1=result.split('|')
        count=len(list1)
        for i in range(count):
            xy=[]
            x=int(list1[i].split(',')[0])
            y=int(list1[i].split(',')[1])
            xy.append(x)
            xy.append(y)
            coordinateData.append(xy)
    else:
        xy = []
        x = int(result.split(',')[0])
        y = int(result.split(',')[1])
        xy.append(x)
        xy.append(y)
        coordinateData.append(xy)
    print(coordinateData)
    for j in coordinateData:
        x=j[0]
        y=j[1]
        ActionChains(driver).move_to_element_with_offset(codeImg,x,y).click().perform()
        time.sleep(0.5)
    determine.click()
    time.sleep(0.5)
    text=driver.page_source
    etr=etree.HTML(text)
    lastText=etr.xpath('//div[@class="geetest_result_tip geetest_up geetest_fail"]/text()')
    if(len(lastText)!=0):
        lastText=lastText[0]
    else:
        lastText="验证成功"
    time.sleep(2)
    if os.path.exists('验证码a.png'):
        os.remove('验证码a.png')
    if os.path.exists('验证码b.png'):
        os.remove('验证码b.png')
    failNum=0
    while(re.search(r'(.*验证失败.*)',lastText) is not None):
        failNum=failNum+1
        if(failNum%6==0):
           driver.refresh()
           driver.implicitly_wait(4)
           codeClick = driver.find_elements_by_xpath('//*[@id="captcha"]/div/div[1]/div/div[2]/div[2]/div[1]/div[3]')[0]
           codeClick.click()
           time.sleep(2)
        codeImg = driver.find_elements_by_xpath('//div[@class="geetest_popup_box"]')[0]
        screenshot_as_bytes = driver.find_elements_by_xpath('//div[@class="geetest_popup_box"]')[0].screenshot_as_png
        determine = driver.find_elements_by_xpath('//div[@class="geetest_commit_tip"]')[0]
        with open('验证码a.png', 'wb') as f:
            f.write(screenshot_as_bytes)
        im = Image.open('验证码a.png')
        region = im.crop((0, 0, 348, 390))
        region.save('验证码b.png')
        coordinateData.clear()
        result = chaojiyingrukou()
        if result==-1:
            return
        if '|' in result:
            list1 = result.split('|')
            count = len(list1)
            for i in range(count):
                xy = []
                x = int(list1[i].split(',')[0])
                y = int(list1[i].split(',')[1])
                xy.append(x)
                xy.append(y)
                coordinateData.append(xy)
        else:
            xy = []
            x = int(result.split(',')[0])
            y = int(result.split(',')[1])
            xy.append(x)
            xy.append(y)
            coordinateData.append(xy)
        print(coordinateData)
        for j in coordinateData:
            x = j[0]
            y = j[1]
            ActionChains(driver).move_to_element_with_offset(codeImg, x, y).click().perform()
            time.sleep(0.5)
        determine.click()
        time.sleep(0.5)
        text = driver.page_source
        etr = etree.HTML(text)
        lastText = etr.xpath('//div[@class="geetest_result_tip geetest_up geetest_fail"]/text()')
        if (len(lastText) != 0):
            lastText = lastText[0]
        else:
            lastText = "验证成功"
        time.sleep(2)
        if os.path.exists('验证码a.png'):
            os.remove('验证码a.png')
        if os.path.exists('验证码b.png'):
            os.remove('验证码b.png')
    print("验证码通过")

# #开启多线程爬取每个房产的周边
# def periphery():
#     threa = []
#     t1 = threading.Thread(target=nearbyThread1)
#     t2 = threading.Thread(target=nearbyThread2)
#     t3 = threading.Thread(target=nearbyThread3)
#     t4 = threading.Thread(target=nearbyThread4)
#     t5 = threading.Thread(target=nearbyThread5)
#     t6 = threading.Thread(target=nearbyThread6)
#     t7 = threading.Thread(target=nearbyThread7)
#     t8 = threading.Thread(target=nearbyThread8)
#     threa.append(t1)
#     threa.append(t2)
#     threa.append(t3)
#     threa.append(t4)
#     threa.append(t5)
#     threa.append(t6)
#     threa.append(t7)
#     threa.append(t8)
#     for i in threa:
#         i.setDaemon(True)
#         i.start()
#     for i in threa:
#         i.join()

#获取用户代理
def get_user_agent():
    user_agents = [

        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",

        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",

        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",

        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",

        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",

        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",

        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",

        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",

        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",

        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",

        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",

        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",

        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",

        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",

        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",

        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",

        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",

        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",

        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",

        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",

        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",

        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",

        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",

        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",

        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",

        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",

        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",

        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",

        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",

        "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",

        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",

        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",

        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",

        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"

    ]
    # random.choice返回列表的随机项
    user_agent = random.choice(user_agents)
    return user_agent

#获取代理IP
def ipx():
    n=random.randint(0,len(trueIp)-1)
    # print(n)
    ip=trueIp[n]
    # print(ip)
    proxiex={
        'http': ip,
        'https': ip,
    }
    # print("代理IP："+str(proxiex))
    return proxiex

#爬取网页，获得网页源码
def Url(url):
    heads={
        "User - Agent": get_user_agent(),
        }
    try:
        print("开始爬取："+url)
        html = requests.get(url=url, headers=heads, proxies=ipx(),timeout=10)
        html.encoding = 'utf-8'
        htmltext = html.text
        print('爬取完毕')
        # print(requests)
        return htmltext
    except Exception as e:
       print(e)
       return None

#解析数据
va1=0
def data(html):
    try:
        bou = BeautifulSoup(html, "html.parser")
        st = str(html)
        a = 0
        for intm in bou.select('li[class="resblock-list post_ulog_exposure_scroll has-results"]'):
            stra = str(intm)
            home=[]
            data = re.findall(findHref, stra)[0]
            name=re.findall(findName,stra)[0]
            print(name)
            home.append(name)
            town=re.findall(findTownAddress,stra)[0]
            print(town)
            addrss=re.findall(findaAddress,stra)[0]
            print(addrss)
            home.append(str(town)+str(addrss))
            areadate=re.findall(findArea,stra)
            if len(areadate)==0:
                print('面积暂时不确定')
                area="暂不确定"
                home.append(area)
            else:
                area=areadate[0]
                print(area)
                home.append(area)
            homeClass = re.findall(findHomeClass, stra)[0]
            print(homeClass)
            home.append(homeClass)
            homeState = re.findall(findHomeState, stra)[0]
            print(homeState)
            home.append(homeState)
            homePrice=re.findall(findHomePrice,stra)[0]
            if str(homePrice)=='价格待定':
                HomeTotalPrice="价格待定"
                home.append(HomeTotalPrice)
                print(HomeTotalPrice)
                home.append("价格待定")
            else:
                homePrice = homePrice + "元/㎡(均价)"
                home.append(homePrice)
                print(homePrice)
                HomeTotalPrice=re.findall(findHomeTotalPrice,stra)
                if(len(HomeTotalPrice)==0):
                    HomeTotalPrice.append("无")
                home.append(HomeTotalPrice[0])
                print(HomeTotalPrice[0])
            da.append("https://dg.fang.lianjia.com/loupan" + str(data))
            print("https://dg.fang.lianjia.com/loupan" + str(data))
            homeAlbumHtml.append("https://dg.fang.lianjia.com/loupan" + str(data)+"xiangce/")
            home.append("https://dg.fang.lianjia.com/loupan" + str(data)+"xiangce/")
            print("https://dg.fang.lianjia.com/loupan" + str(data)+"xiangce/")
            houseTypeHtml.append("https://dg.fang.lianjia.com/loupan" + str(data)+"huxingtu/")
            home.append("https://dg.fang.lianjia.com/loupan" + str(data)+"huxingtu/")
            print("https://dg.fang.lianjia.com/loupan" + str(data)+"huxingtu/")
            homeDynamicHtml.append("https://dg.fang.lianjia.com/loupan" + str(data)+"dongtai/")
            home.append("https://dg.fang.lianjia.com/loupan" + str(data)+"dongtai/")
            print("https://dg.fang.lianjia.com/loupan" + str(data)+"dongtai/")
            homeNearbyHtml.append("https://dg.fang.lianjia.com/loupan" + str(data)+"peitao/")
            home.append("https://dg.fang.lianjia.com/loupan" + str(data)+"peitao/")
            print("https://dg.fang.lianjia.com/loupan" + str(data)+"peitao/")
            lable = re.findall(findHouseLabel, stra)
            homeLable = ""
            if (len(lable) > 0):
                lable = lable[0]
                lable = str(lable).replace('\n<span>', "")
                lableList = lable.split('</span>')
                # print(lable)
                # print(lableList)
                for i in lableList[0:len(lableList) - 1]:
                    homeLable = homeLable + str(i)
                    homeLable = homeLable + "、"
                homeLable = homeLable[:-1]
            else:
                homeLable = ""
            print(homeLable)
            home.append(homeLable)
            crawlingDate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            print(crawlingDate)
            home.append(crawlingDate)
            datr.append(home)
            a = 1
    except EOFError as e:
        print(e)
        return 0

#存储房产基本信息
def saveInformation():
    con = pymssql.connect("10_0_8_11", "sa", "1ovemifengwang.", "houeData", 'utf-8')
    # con = pymssql.connect("LAPTOP-8MTU5T87", "sa", "20010107", "houeData", 'utf-8')
    cursor=con.cursor()
    #创建房产基础信息表的sql语句
    createProperty_baseSql='''
create table property_base(
Room_ID int identity(1,1) primary key,
Name varchar(40) not null,
Location varchar(100) not null,
House_Type varchar(10) not null,
IsSell varchar(4) not null,
Average_Price varchar(30) ,
Total_Price varchar(30) , 
Architecture_Areae varchar(30),
House_Type_Html text not null,
Images_Html text not null,
Dynamic_Html text not null,
Nearby_Html text not null,
Views int null default (0),
Lng_lat varchar(20) null,
House_Label varchar(50) null,
Bad_Label varchar(50) null,
Well_Label varchar(50) null,
Find_Date varchar(10) null 
)'''
    cursor.execute(createProperty_baseSql)
    con.commit()
    for i in datr:
        # print(i)
        insertHouseSql = "insert into property_base(Name,Location,House_Type,IsSell,Average_Price,Total_Price,Architecture_Areae,House_Type_Html,Images_Html,Dynamic_Html,Nearby_Html,House_Label,Find_Date) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        cursor.execute(insertHouseSql%(i[0],i[1],i[3],i[4],i[5],i[6],i[2],i[8],i[7],i[9],i[10],i[11],i[12]))
        con.commit()
    cursor.close()
    con.close()

#存储房产户型信息
def saveType():
    con = pymssql.connect("10_0_8_11", "sa", "1ovemifengwang.", "houeData", 'utf-8')
    # con = pymssql.connect("LAPTOP-8MTU5T87", "sa", "20010107", "houeData", 'utf-8')
    cursor = con.cursor()
    #创建户型信息表sql语句
    createTypeSql='''
    create table House_Type(
ID int identity(1,1) primary key ,
Room_ID int Not null,
House_Type varchar(10) Not null,
Area varchar(20) Not null,
Price varchar(20) Not null,
Sales_Status varchar(5) Not null,
Images_Link varchar(200) Not null,
foreign key(Room_ID) references property_base(Room_ID)
)
    '''
    cursor.execute(createTypeSql)
    con.commit()
    for i in houseType:
        houseIdSql="select Room_ID from property_base where House_Type_Html like '%s'"
        cursor.execute(houseIdSql%(i[0]))
        results=cursor.fetchall()
        houseId=results[0][0]
        insertTypeSql="insert into House_Type(Room_ID,House_Type,Area,Price,Sales_status,Images_Link) values('%s','%s','%s','%s','%s','%s')"
        for j in i[1:]:
            cursor.execute(insertTypeSql%(houseId,j[0],j[1],j[2],j[3],j[4]))
            con.commit()
    # sql="select Room_ID from property_base where Name='华讯大宅'"
    # cursor.execute(sql)
    # results=cursor.fetchall()
    # print(results[0][0])
    # cursor.execute('')
    # res=cursor.fetchall()
    # print(res)
    cursor.close()
    con.close()

#存储房产相册信息
def saveImages():
    coun=0
    con = pymssql.connect("10_0_8_11", "sa", "1ovemifengwang.", "houeData", 'utf-8')
    # con = pymssql.connect("LAPTOP-8MTU5T87", "sa", "20010107", "houeData", 'utf-8')
    cursor = con.cursor()
    createHouseImgSql='''
    create table images_list(
ID int identity(1,1) primary key ,
Room_ID int Not null,
Images_Type varchar(40) Not null,
Images_Lingk text Not null,
foreign key(Room_ID) references property_base(Room_ID)
 )'''
    cursor.execute(createHouseImgSql)
    con.commit()
    for i in houseXiangce:
        houseIdSql = "select Room_ID from property_base where Images_Html like '%s'"
        coun=coun+1
        cursor.execute(houseIdSql % (i[0]))
        results = cursor.fetchall()
        houseId = results[0][0]
        insertXiangceSql="insert into images_list(Room_ID,Images_Type,Images_Lingk) values('%s','%s','%s')"
        for j in i[1:]:
            for k in j[1:]:
                # print(i)
                cursor.execute(insertXiangceSql%(houseId,j[0],k))
                con.commit()
    cursor.close()
    con.close()
    return coun

#存储房产变化信息
def saveChange():
    con = pymssql.connect("10_0_8_11", "sa", "1ovemifengwang.", "houeData", 'utf-8')
    # con = pymssql.connect("LAPTOP-8MTU5T87", "sa", "20010107", "houeData", 'utf-8')
    cursor = con.cursor()
    #创建房产变化表
    createHouseImgSql = '''
       create table property_change(
Change_ID int identity(1,1) primary key ,
Room_ID int Not null,
Change_Theme varchar(40) Not null,
Change_Date varchar(16) Not null,
Change_Content text Not null,
 foreign key(Room_ID) references property_base(Room_ID)
)'''
    cursor.execute(createHouseImgSql)
    con.commit()
    for i in houseDongTa:
        # print(i)
        houseIdSql = "select Room_ID from property_base where Dynamic_Html like'%s'"
        cursor.execute(houseIdSql % (i[0]))
        results = cursor.fetchall()
        houseId = results[0][0]
        insertXiangceSql = "insert into property_change(Room_ID,Change_Theme,Change_Date,Change_Content) values('%s','%s','%s','%s')"
        for j in i[1:]:
            cursor.execute(insertXiangceSql % (houseId, j[0],j[1],j[2]))
            con.commit()
    cursor.close()
    con.close()

#数据更新前将数据表删除
def dropData():
    con = pymssql.connect("10_0_8_11", "sa", "1ovemifengwang.", "houeData", 'utf-8')
    # con = pymssql.connect("LAPTOP-8MTU5T87", "sa", "20010107", "houeData", 'utf-8')
    cursor = con.cursor()
    # 删除images_list,property_change,property_base表的SQL语句
    dropImg = "drop table images_list"
    dropType = "drop table House_Type"
    dropProperty = "drop table property_change"
    dropBase = "drop table property_base"
    cursor.execute(dropImg)
    con.commit()
    cursor.execute(dropType)
    con.commit()
    cursor.execute(dropProperty)
    con.commit()
    cursor.execute(dropBase)
    con.commit()
    cursor.close()
    con.close()


#存储周边
# def saveNearby():
#     con = pymssql.connect("LAPTOP-8MTU5T87", "sa", "20010107", "houeData", 'utf-8')
#     cursor = con.cursor()
#     #创建房产周边表
#     createNearbySql='''
#     create table Room_Near(
# Near_ID int identity(1,1) primary key,
# Room_ID int Not null,
# Place_Name varchar(80),
# Place_Type varchar(12),
# Details_1 varchar(250),
# Details_2 varchar(100)
# foreign key(Room_ID) references property_base(Room_ID)
# )'''
#     cursor.execute(createNearbySql)
#     con.commit()
#     for i in periphteryList:
#         houseIdSql = "select Room_ID from property_base where Nearby_Html like '%s'"
#         cursor.execute(houseIdSql % (i[0]))
#         results = cursor.fetchall()
#         houseId = results[0][0]
#         insertNearby="insert into Room_Near(Room_ID,Place_Name,Place_Type,Details_1,Details_2) values('%s','%s','%s','%s','%s')"
#         if (str(i[1])=="公交" or str(i[1])=="地铁"):
#             if (len(i)==2):
#                 continue
#             for j in i[2::]:
#                 Route=""
#                 for k in j[1::]:
#                     Route=Route+str(k)+"、"
#                 Route=Route[:-1]
#                 cursor.execute(insertNearby % (houseId,j[0],i[1],Route,""))
#                 con.commit()
#         else:
#             for j in i[2::]:
#                 if (len(j)==2):
#                     cursor.execute(insertNearby % (houseId, j[0], i[1], j[1], ""))
#                     con.commit()
#                 else:
#                     cursor.execute(insertNearby % (houseId, j[0], i[1], j[1], j[2]))
#                     con.commit()
#     cursor.close()
#     con.close()


def ipLeep():
    print("开始取代理IP")
    ipx4()
    global timer
    timer=threading.Timer(30,ipLeep)
    timer.start()

if __name__ == '__main__':
    #开始取代理IP，并在程序执行的同时每隔一定时间内更新代理IP
    ipx4()
    timer=threading.Timer(30,ipLeep)
    timer.start()
    stratime=time.time()
    #开始爬取数据
    htmlNum('https://dg.fang.lianjia.com/loupan/')
    pool=Pool(10)
    pool.map(homeHTtml,inndexHtml)
    pool.close()
    pool.join()
    pool=Pool(20)
    pool.map(houseDongTaData,homeDynamicHtml)
    pool.close()
    pool.join()
    pool=Pool(20)
    pool.map(dataHuXing,houseTypeHtml)
    pool.close()
    pool.join()
    pool=Pool(20)
    pool.map(dataHouseXiangCe,homeAlbumHtml)
    pool.close()
    pool.join()
    timer.cancel()
    # periphery()
    #开始存储数据
    dropData()
    saveInformation()
    saveType()
    saveImages()
    saveChange()
    # saveNearby()
    # test()
    lasttime = time.time()
    print(lasttime - stratime)





