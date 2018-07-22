# -*- coding: utf-8 -*-
__author__ = 'igaozp'

import scrapy


class LiepinItem(scrapy.Item):
    # 工作名称
    job_name = scrapy.Field()
    # 薪水
    salary = scrapy.Field()
    # 公司名称
    company_name = scrapy.Field()
    # 页面 URL
    url = scrapy.Field()
    # 城市
    city = scrapy.Field()
    # 学历要求
    education = scrapy.Field()
    # 经验要求
    experience = scrapy.Field()
    # 职位描述
    description = scrapy.Field()
    # 公司地址
    address = scrapy.Field()
    # 发布时间
    public_date = scrapy.Field()
    # 抓取时间
    crawled_date = scrapy.Field()
