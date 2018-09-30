# -*- coding: utf-8 -*-

__author__ = 'igaozp'

from ..items import ZhaopinItem
from scrapy import Spider, Request
from bs4 import BeautifulSoup
import datetime
import logging
import redis
import json

logger = logging.getLogger(__name__)


class ZhaopinSpider(Spider):
    """
    智联招聘爬虫
    """
    name = "zhaopin_spider"

    allowed_domains = ['www.zhaopin.com', 'sou.zhaopin.com', 'fe-api.zhaopin.com', 'jobs.zhaopin.com']
    start_urls = ['http://www.zhaopin.com/']

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
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': 'adfbid2=0; sts_deviceid=163918e1e734a5-08a878273b41c3-737356c-1327104-163918e1e74377; '
                      'zg_did=%7B%22did%22%3A%20%22163cad48de050b-0935d24a14ae55-737356c-144000-163cad48de1135'
                      '%22%7D; urlfrom2=121126445; adfcid2=none; JSSearchModel=0; LastSearchHistory=%7b%22Id%2'
                      '2%3a%221370e748-65e6-4768-899f-643c4def3c4d%22%2c%22Name%22%3a%22%e9%9d%92%e5%b2%9b%22%'
                      '2c%22SearchUrl%22%3a%22http%3a%2f%2fsou.zhaopin.com%2fjobs%2fsearchresult.ashx%3fsm%3d0'
                      '%26p%3d1%22%2c%22SaveTime%22%3a%22%5c%2fDate(1531192767261%2b0800)%5c%2f%22%7d; ZP_OLD_F'
                      'LAG=false; zg_08c5bcee6e9a4c0594a5d34b79b9622a=%7B%22sid%22%3A%201532654517726%2C%22upda'
                      'ted%22%3A%201532654517726%2C%22info%22%3A%201532654517730%2C%22superProperty%22%3A%20%22%'
                      '7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDom'
                      'ain%22%3A%20%22www.zhaopin.com%22%2C%22landHref%22%3A%20%22https%3A%2F%2Foverseas.zhaopin.'
                      'com%2F%22%7D; dywez=95841923.1532658528.37.10.dywecsr=sou.zhaopin.com|dyweccn=(referral)|dy'
                      'wecmd=referral|dywectr=undefined|dywecct=/; urlfrom=121126445; adfcid=none; adfbid=0; sts_'
                      'sg=1; dywec=95841923; zp_src_url=https%3A%2F%2Fwww.zhaopin.com%2F; GUID=754a375ce25b4a04ab4'
                      '3b0f4fa09d438; LastCity=%E6%83%A0%E5%B7%9E; LastCity%5Fid=773; ZL_REPORT_GLOBAL={%22sou%22:'
                      '{%22actionIdFromSou%22:%2208131a1e-7e6a-464f-bd61-a98c523977df-sou%22%2C%22funczone%22:%22s'
                      'mart_matching%22}}; dywea=95841923.1451030504579649300.1526544765.1532681572.1532694108.39;'
                      ' dyweb=95841923.2.10.1532694108; sts_sid=164dbb0164a28-062731b7180f95-47e1039-1327104-164'
                      'dbb0164b133; sts_evtseq=2',
            'DNT': '1',
            'Host': 'fe-api.zhaopin.com',
            'Origin': 'https://sou.zhaopin.com',
            'Referer': 'https://sou.zhaopin.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        }

    def start_requests(self):
        url_prefix = 'https://fe-api.zhaopin.com/c/i/sou?start=60&pageSize=60&cityId='
        url_suffix = '&industry=10100'

        city_id = [
            '489',  # 全国
            '530',  # 北京
            '538',  # 上海
            '765',  # 深圳
            '763',  # 广州
            '531',  # 天津
            '801',  # 成都
            '653',  # 杭州
            '736',  # 武汉
            '600',  # 大连
            '613',  # 长春
            '635',  # 南京
            '702',  # 济南
            '703',  # 青岛
            '639',  # 苏州
            '599',  # 沈阳
            '854',  # 西安
            '719',  # 郑州
            '749',  # 长沙
            '551',  # 重庆
            '622',  # 哈尔滨
            '636',  # 无锡
            '654',  # 宁波
            '681',  # 福州
            '682',  # 厦门
            '565',  # 石家庄
            '664',  # 合肥
            '773',  # 惠州
        ]

        urls = []
        for city in city_id:
            url = url_prefix + city + url_suffix
            urls.append(url)

        for url in urls:
            prefix = url[:41]
            suffix = url[44:]
            logger.info('请求 {}'.format(url))
            yield Request(url=url, meta={'suffix': suffix, 'prefix': prefix, 'url': url, 'page': 1},
                          callback=self.parse)

    def parse(self, response):
        """
        解析招聘列表的信息
        :param response: 页面返回的相应数据
        :return: item 对象
        """

        data = json.loads(response.body_as_unicode())
        total_num = data['data']['numFound']
        results = data['data']['results']
        prefix = response.meta['prefix']
        suffix = response.meta['suffix']
        url = response.meta['url']
        page = int(response.meta['page'])

        if page * 60 < int(total_num):
            for result in results:
                logger.info('当前 URL {}'.format(url))
                url = result['positionURL']
                if self.redis_db.hexists('zhaopin_url', url):
                    logger.info('重复的数据: ' + url)
                    continue

                logger.info('当前职位 URL: {}'.format(url))

                item = ZhaopinItem()
                item['job_name'] = result['jobName']
                item['release_date'] = result['timeState']
                item['city'] = result['city']['display']
                item['salary'] = result['salary']
                item['url'] = url
                item['experience'] = result['workingExp']['name']
                item['company_name'] = result['company']['name']
                item['company_size'] = result['company']['size']['name']
                item['education_require'] = result['eduLevel']['name']

                yield Request(url=url, meta={'item': item}, callback=self.parse_content, dont_filter=True)

        page = page + 1
        if page * 60 < int(total_num):
            url = prefix + str(page * 60) + suffix
            yield Request(url=url, meta={'page': page, 'prefix': prefix, 'suffix': suffix, 'url': url},
                          callback=self.parse, dont_filter=True)

    @staticmethod
    def parse_content(response):
        """
        解析招聘页面的详细数据
        :param response: 页面返回的响应数据
        :return: item 对象
        """
        soup = BeautifulSoup(response.text, 'lxml')
        item = response.meta['item']

        job_requires = soup.select('div.terminalpage-main.clearfix > div > div:nth-of-type(1) > p')
        if len(job_requires) is 0:
            job_requires = soup.select('div.responsibility.pos-common > div.pos-ul > div')
        job_require = ''
        for require in job_requires:
            job_require = job_require + require.get_text().strip()

        try:
            address = soup.select('div.company-box > ul > li:nth-of-type(4) > strong')[0].get_text().strip()
        except Exception as err:
            print(err)
            address = ''
        try:
            if address is '':
                address = soup.select('div.pos-common.work-add.cl > p.add-txt')[0].get_text().strip()
        except Exception as err:
            print(err)
            address = ''
        try:
            if address is '':
                address = soup.select('div.promulgator-info.clearfix > ul > li:'
                                      'nth-of-type(5) > strong')[0].get_text().strip()
        except Exception as err:
            print(err)
            address = ''

        item['job_require'] = job_require
        item['address'] = address
        item['head_count'] = ''
        item['time'] = datetime.date.today()

        yield item
