# -*- coding: utf-8 -*-
__author__ = 'igaozp'

import scrapy


class WuejobItem(scrapy.Item):
    # 工作名称
    job_name = scrapy.Field()
    # 所在城市
    city = scrapy.Field()
    # 招聘页面的 URL
    url = scrapy.Field()
    # 公司地址
    address = scrapy.Field()
    # 发布日期
    public_date = scrapy.Field()
    # 招聘人数
    head_count = scrapy.Field()
    # 职位信息
    description = scrapy.Field()
    # 学历要求
    education = scrapy.Field()
    # 工作经验
    experience = scrapy.Field()
    # 公司名称
    company_name = scrapy.Field()
    # 薪水
    salary = scrapy.Field()
    # 页面抓取时间
    crawled_date = scrapy.Field()
