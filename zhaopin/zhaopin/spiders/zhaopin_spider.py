# -*- coding: utf-8 -*-
__author__ = 'igaozp'

from ..items import ZhaopinItem
from scrapy import Spider, Request
from urllib.parse import quote
from bs4 import BeautifulSoup
import datetime
import logging
import redis
import re

logger = logging.getLogger(__name__)


class ZhaopinSpider(Spider):
    """
    智联招聘爬虫
    """
    name = "zhaopin_spider"

    allowed_domains = ['www.zhaopin.com']
    start_urls = ['http://www.zhaopin.com/']

    custom_settings = {
        'Referer': 'https://sou.zhaopin.com'
    }

    def __init__(self, **kwargs):
        """
        初始化 Redis 连接以及 headers 参数
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.logger.info('初始化 Redis 连接')
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=11)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'adfbid2=0; JSSearchModel=0; sts_deviceid=163918e1e734a5-08a878273b41c3-737356c-1327104-'
                      '163918e1e74377; dywez=95841923.1527669355.18.4.dywecsr=google.com|dyweccn=(referral)|dywecmd='
                      'referral|dywectr=undefined|dywecct=/; LastJobTag=%e4%ba%94%e9%99%a9%e4%b8%80%e9%87%91%7c%e8%8a%'
                      '82%e6%97%a5%e7%a6%8f%e5%88%a9%7c%e5%b8%a6%e8%96%aa%e5%b9%b4%e5%81%87%7c%e7%bb%a9%e6%95%88%e5%a5%'
                      '96%e9%87%91%7c%e9%a4%90%e8%a1%a5%7c%e5%b9%b4%e5%ba%95%e5%8f%8c%e8%96%aa%7c%e5%ae%9a%e6%9c%9f%e4%'
                      'bd%93%e6%a3%80%7c%e5%91%98%e5%b7%a5%e6%97%85%e6%b8%b8%7c%e5%bc%b9%e6%80%a7%e5%b7%a5%e4%bd%9c%7c%'
                      'e5%85%a8%e5%8b%a4%e5%a5%96%7c%e5%91%a8%e6%9c%ab%e5%8f%8c%e4%bc%91%7c%e8%a1%a5%e5%85%85%e5%8c%bb%'
                      'e7%96%97%e4%bf%9d%e9%99%a9%7c%e4%ba%a4%e9%80%9a%e8%a1%a5%e5%8a%a9%7c%e5%8a%a0%e7%8f%ad%e8%a1%a5'
                      '%e5%8a%a9%7c%e5%b9%b4%e7%bb%88%e5%88%86%e7%ba%a2%7c%e6%af%8f%e5%b9%b4%e5%a4%9a%e6%ac%a1%e8%b0%83'
                      '%e8%96%aa%7c%e5%88%9b%e4%b8%9a%e5%85%ac%e5%8f%b8%7c%e5%8c%85%e4%bd%8f%7c%e8%82%a1%e7%a5%a8%e6%9c'
                      '%9f%e6%9d%83%7c%e9%80%9a%e8%ae%af%e8%a1%a5%e8%b4%b4%7c%e5%81%a5%e8%ba%ab%e4%bf%b1%e4%b9%90%e9%83'
                      '%a8%7c%e6%88%bf%e8%a1%a5%7c%e4%b8%8d%e5%8a%a0%e7%8f%ad%7c%e5%8c%85%e5%90%83%7c14%e8%96%aa%7c%e5'
                      '%85%8d%e8%b4%b9%e7%8f%ad%e8%bd%a6%7c%e4%bd%8f%e6%88%bf%e8%a1%a5%e8%b4%b4%7c%e6%97%a0%e8%af%95%'
                      'e7%94%a8%e6%9c%9f%7c%e9%ab%98%e6%b8%a9%e8%a1%a5%e8%b4%b4%7c%e9%87%87%e6%9a%96%e8%a1%a5%e8%b4%'
                      'b4%7c%e5%85%8d%e6%81%af%e6%88%bf%e8%b4%b7; LastSearchHistory=%7b%22Id%22%3a%2285f29195-d292-'
                      '4883-8628-7045cc256302%22%2c%22Name%22%3a%22%e5%8c%97%e4%ba%ac+%2b+%e7%bd%91%e7%bb%9c%e6%b8%b8'
                      '%e6%88%8f%22%2c%22SearchUrl%22%3a%22http%3a%2f%2fsou.zhaopin.com%2fjobs%2fsearchresult.ashx%'
                      '3fin%3d160600%26jl%3d%25e5%258c%2597%25e4%25ba%25ac%26p%3d1%26isadv%3d0%22%2c%22SaveTime%22%3a%'
                      '22%5c%2fDate(1527672678730%2b0800)%5c%2f%22%7d; ZP_OLD_FLAG=false; LastCity=%E7%83%9F%E5%8F%B0; '
                      'LastCity%5Fid=707; urlfrom2=121126445; adfcid2=none; __lnkrntdmcvrd=-1; campusOperateJobUserInfo'
                      '=f125b035-8a63-4dae-b978-db160fa7520b; zg_did=%7B%22did%22%3A%20%22163cad48de050b-0935d24a14ae55'
                      '-737356c-144000-163cad48de1135%22%7D; zg_08c5bcee6e9a4c0594a5d34b79b9622a=%7B%22sid%22%3A%201528'
                      '116317673%2C%22updated%22%3A%201528116908342%2C%22info%22%3A%201528116317677%2C%22superProperty%'
                      '22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22refer'
                      'rerDomain%22%3A%20%22sou.zhaopin.com%22%7D; dywea=95841923.1451030504579649300.1526544765.152811'
                      '5754.1528168623.23; ZP-ENV-FLAG=gray; sts_sg=1; sts_sid=163d46dd9f651d-03c75c744aff55-737356c-'
                      '1327104-163d46dd9f7477; __ads_session=VXgwn1f3GwlvLlkFFAA=; sts_evtseq=3',
            'DNT': '1',
            'Host': 'sou.zhaopin.com',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://sou.zhaopin.com/'
        }

    def start_requests(self):
        # URL 前缀包含计算机相关职位的参数信息
        url_prefix = 'http://sou.zhaopin.com/jobs/searchresult.ashx?bj='
        # URL 后缀
        url_suffix = '&p=1&isadv=0'
        # 全国主要城市列表
        city_list = [
            '北京', '上海', '广州', '深圳', '天津', '武汉', '西安',
            '成都', '大连', '长春', '沈阳', '南京', '济南', '青岛',
            '杭州', '苏州', '无锡', '宁波', '重庆', '郑州', '长沙',
            '福州', '厦门', '哈尔滨', '石家庄', '合肥', '烟台', '太原',
            '合肥', '温州', '烟台'
        ]
        # 行业类别
        industry_list = [
            '210500',
            '160400',
            '160000',
            '160500',
            '160200',
            '300100',
            '160100',
            '160600',
        ]
        # 职位类别
        background_list = [
            '160000',
            '160300',
            '160200',
            '160400',
            '200500',
            '200300',
            '5001000',
        ]

        # 拼接 URL
        urls = []
        for city in city_list:
            for industry in industry_list:
                for background in background_list:
                    url = url_prefix + background + '&in=' + industry + '&jl=' + quote(city) + url_suffix
                    urls.append(url)
                    logger.info('生成 URL: ' + url)

        for url in urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        解析招聘列表的信息
        :param response: 页面返回的相应数据
        :return: item 对象
        """
        soup = BeautifulSoup(response.text, 'lxml')

        # 当前搜索结果页面的列表
        job_name_list = soup.select("table.newlist > tr > td.zwmc > div > a:nth-of-type(1)")
        salary_list = soup.select("table.newlist > tr > td.zwyx")
        date_list = soup.select("table.newlist > tr > td.gxsj > span")
        city_list = soup.select("table.newlist > tr > td.gzdd")

        # job_name_list = soup.select("span.jobTitle")
        # urls = soup.select(".infoBox a")
        # salary_list = soup.select("div.salary")
        # date_list = soup.select("div.timeState")
        # city_list = soup.select("div.address")

        for job_name, salary, date, city in zip(job_name_list, salary_list, date_list, city_list):
            item = ZhaopinItem()
            url = job_name.get('href')
            if self.redis_db.hexists('zhaopin_url', url):
                logger.info("重复的数据: " + url)
                continue
            logger.info('URL: ' + url)
            item['url'] = url
            item['job_name'] = job_name.get_text()
            item['salary'] = salary.get_text()
            item['release_date'] = date.get_text()
            item['city'] = city.get_text()

            yield Request(url=url, meta={"item": item}, callback=self.parse_content, dont_filter=True)

        # 抓取下一页
        if soup.select("a.next-page") is not None:
            logger.info('Next page')
            next_page_url = soup.select("a.next-page")[0].get('href')
            yield Request(url=next_page_url, callback=self.parse, dont_filter=True)

    @staticmethod
    def parse_content(response):
        """
        解析招聘页面的详细数据
        :param response: 页面返回的响应数据
        :return: item 对象
        """

        # 招聘职位的要求信息
        require_data = response.xpath(
            '//body/div[@class="terminalpage clearfix"]/div'
            '[@class="terminalpage-left"]/div[@class="terminalpage-main '
            'clearfix"]/div[@class="tab-cont-box"]/div[1]/p'
        ).extract()
        require_data_middle = ''
        for line in require_data:
            temp = re.sub(r'<.*?>', r'', line, re.S)
            require_data_middle = require_data_middle + re.sub(r'\s*', r'', temp, re.S)

        job_soup = BeautifulSoup(response.body, 'lxml')
        item = response.meta['item']

        # 获取其他字段的信息
        address = job_soup.select('div.terminalpage-main.clearfix > div > div:nth-of-type(1) > h2')[0].text.strip()
        if address[-6:] == "查看职位地图":
            address = address[:-7]
        item['address'] = address
        item['job_require'] = require_data_middle
        item['experience'] = job_soup.select('div.terminalpage-left strong')[4].text.strip()
        item['company_name'] = job_soup.select('div.fixed-inner-box h2')[0].text
        item['company_size'] = job_soup.select('ul.terminal-ul.clearfix li strong')[8].text.strip()
        item['head_count'] = job_soup.select('div.terminalpage-left strong')[6].text.strip()
        item['education_require'] = job_soup.select('div.terminalpage-left strong')[5].text.strip()
        item['time'] = datetime.date.today()

        # item['job_require'] = job_soup.select('div.responsibility.pos-common > div.pos-ul')
        # item['experience'] = job_soup.select('div.info-three.l > span:nth-child(2)')
        # item['company_name'] = job_soup.select('div.promulgator-info.clearfix > h3 > a')
        # item['company_size'] = job_soup.select('div.promulgator-info.clearfix > ul > li:nth-child(3) > strong')
        # item['education_require'] = job_soup.select(' div.info-three.l > span:nth-child(3)')

        yield item
