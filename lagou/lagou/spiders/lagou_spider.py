from scrapy import Spider, Request
from bs4 import BeautifulSoup
from lagou.items import LagouItem
import redis
import datetime
import logging
import requests

logger = logging.getLogger(__name__)


class LagouSpider(Spider):
    name = "lagou_spider"

    allowed_domains = ['www.lagou.com']
    start_urls = ['https://www.lagou.com/']
    agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' \
            '(KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'

    custom_settings = {
        'COOKIES_ENABLE': False,
        'DOWNLOAD_DELAY': 1,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': '_ga=GA1.2.1133493535.1527384205; user_trace_token=20180527092319-89d610a6-614c-11e8-8c87'
                      '-5254005c3644; LGUID=20180527092319-89d613f3-614c-11e8-8c87-5254005c3644; isCloseNotice=0; '
                      'index_location_city=%E5%85%A8%E5%9B%BD; X_HTTP_TOKEN=5c6831b96ea390fb9d4d4df9333db9d1; '
                      'ab_test_random_num=0; LGSID=20180621142800-3e8859e7-751c-11e8-9738-5254005c3644; PRE_UTM=; '
                      'PRE_HOST=; PRE_SITE=; PRE_LAND=; _putrc=CC997F765FD84A38123F89F2B170EADC; JSESSIONID=ABAAAB'
                      'AACBHABBI938C345637C76B4F1638392AE0215B94; login=true; unick=God+Is+An+Astronaut; showExprie'
                      'dIndex=1; showExpriedCompanyHome=1; showExpriedMyPublish=1; hasDeliver=0; _gat=1; gate_login_'
                      'token=612f7ef9c24bd8eaead74495dffe9f1d752c39c10103c2b61e6d33438569c4b5; TG-TRACK-CODE=index_n'
                      'avigation; SEARCH_ID=cf66f3191ff34535be1abc0ab26320ae; LGRID=20180621143034-9a81e2d6-751c-11e'
                      '8-9738-5254005c3644',
            'Host': 'www.lagou.com',
            'Origin': 'https://www.lagou.com',
            'Referer': 'https://www.lagou.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'
        }
    }

    def __init__(self):
        self.logger.info('初始化 Redis 连接')
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=11)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)

    def start_requests(self):
        """
        开始发送请求
        """

        # 获取所有职位列表的 URL
        response = requests.get('https://www.lagou.com/')
        soup = BeautifulSoup(response.text, 'lxml')
        board_list = soup.select('#sidebar > div > div')

        urls = []
        for board in board_list[:4]:
            board_content = board.select('div.menu_sub.dn > dl > dd > a')
            url_list = map(lambda x: x.get('href'), board_content)
            for url in url_list:
                urls.append(url)
        logger.info('获取到职位 URL %s 个', len(urls))

        # 对所有的职位 URL 发送请求
        for url in urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        解析职位列表的信息

        :param response: 页面的请求数据
        """

        soup = BeautifulSoup(response.text, 'lxml')

        job_name_list = soup.select(
            '#s_position_list > ul > li > div.list_item_top > div.position > div.p_top > a > h3')
        salary_list = soup.select(
            '#s_position_list > ul > li > div.list_item_top > div.position > div.p_bot > div > span')
        company_list = soup.select(
            '#s_position_list > ul > li > div.list_item_top > div.company > div.company_name > a')
        city_list = soup.select(
            '#s_position_list > ul > li > div.list_item_top > div.position > div.p_top > a > span > em')
        experience_education_list = soup.select(
            '#s_position_list > ul > li > div.list_item_top > div.position > div.p_bot > div')
        public_date_list = soup.select(
            '#s_position_list > ul > li > div.list_item_top > div.position > div.p_top > span')
        urls = soup.select('#s_position_list > ul > li > div.list_item_top > div.position > div.p_top > a')

        experience_list = []
        education_list = []
        for item in experience_education_list:
            string = item.get_text().strip()
            pos = string.find('经验')
            string = string[pos:]
            experience = string.split('/')[0][2:].strip()
            education = string.split('/')[1].strip()

            experience_list.append(experience)
            education_list.append(education)

        for job_name, salary, company_name, city, experience, education, public_date, url in zip(job_name_list,
                                                                                                 salary_list,
                                                                                                 company_list,
                                                                                                 city_list,
                                                                                                 experience_list,
                                                                                                 education_list,
                                                                                                 public_date_list,
                                                                                                 urls):
            url = url.get('href')
            if self.redis_db.hexists('lagou_url', url):
                self.logger.info('重复的数据: ' + url)
                continue
            item = LagouItem()
            item['job_name'] = job_name.get_text().strip()
            item['salary'] = salary.get_text().strip()
            item['company_name'] = company_name.get_text().strip()
            item['city'] = city.get_text().strip()
            item['experience'] = experience
            item['education'] = education
            item['public_date'] = public_date.get_text()
            item['url'] = url
            item['crawled_date'] = datetime.date.today()

            # 获取职位的详细信息
            yield Request(url=url, meta={"item": item}, callback=self.parse_content, dont_filter=True)

        # 获取下一页的 URL 和数据
        next_url = soup.select('#s_position_list > div.item_con_pager > div > a')[-1].get('href')
        if next_url.find('http') is not None:
            self.logger.info('Next url: ' + next_url)
            yield Request(url=next_url, callback=self.parse)

    @staticmethod
    def parse_content(response):
        """
        解析职位页面的详细信息

        :param response: 页面的请求数据
        :return: item 实例
        """

        soup = BeautifulSoup(response.body, 'lxml')
        item = response.meta['item']

        address = soup.select('#job_detail > dd.job-address.clearfix > div.work_addr')[0].get_text().strip()
        pos = address.find('查看地图')
        address = address[:pos]
        address = address.replace('\n', '').replace(' ', '')
        item['address'] = address
        item['job_advantage'] = soup.select('#job_detail > dd.job-advantage > p')[0].get_text()
        logger.info(item['job_advantage'])
        item['description'] = soup.select('#job_detail > dd.job_bt > div')[0].get_text()
        item['company_size'] = soup.select('#job_company > dd > ul > li:nth-of-type(3)')[0].get_text()

        yield item
