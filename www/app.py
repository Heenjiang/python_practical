#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''
async web application.
'''

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web
from jinja2 import Environment, FileSystemLoader

import orm
from coroweb import add_routes, add_static
#加载jinjia 模板模块
def init_jinja2(app, **kw):
    logging.info('init jinja2...')
    #模板的一些参数设定，比如模块结束与开始的标识符，变量的标识符，是否自动加载模块等等
    options = dict(
        autoescape = kw.get('autoescape', True),
        block_start_string = kw.get('block_start_string', '{%'),
        block_end_string = kw.get('block_end_string', '%}'),
        variable_start_string = kw.get('variable_start_string', '{{'),
        variable_end_string = kw.get('variable_end_string', '}}'),
        auto_reload = kw.get('auto_reload', True)
    )
    #模板的文件夹路径
    path = kw.get('path', None)
    #如果在创建app的时候没有指定模板的文件夹，就默认为app文件目录中的templates目录
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    logging.info('set jinja2 template path: %s' % path)
    #实例化jinjia的核心模块Environment
    env = Environment(loader=FileSystemLoader(path), **options)
    #添加过滤器
    filters = kw.get('filters', None)
    if filters is not None:
        for name, f in filters.items():
            env.filters[name] = f
    app['__templating__'] = env
#中间件声明
async def logger_factory(app, handler):
    async def logger(request):
        logging.info('Request: %s %s' % (request.method, request.path))
        # await asyncio.sleep(0.3)
        return (await handler(request))
    return logger

async def data_factory(app, handler):
    #解析request请求的数据
    async def parse_data(request):
        if request.method == 'POST':
            if request.content_type.startswith('application/json'):
                request.__data__ = await request.json()
                logging.info('request json: %s' % str(request.__data__))
            elif request.content_type.startswith('application/x-www-form-urlencoded'):
                request.__data__ = await request.post()
                logging.info('request form: %s' % str(request.__data__))
        return (await handler(request))
    return parse_data

async def response_factory(app, handler):
    async def response(request):
        logging.info('Response handler...')
        #调用url处理函数，获取返回的数据
        r = await handler(request)
        #如果处理函数返回的是一个response类型
        if isinstance(r, web.StreamResponse):
            return r
        #如果处理函数返回的是一个bytes类型的结果。比如：图片，视频， 声音等
        if isinstance(r, bytes):
            resp = web.Response(body=r)
            resp.content_type = 'application/octet-stream'
            return resp
        #如果处理函数返回的是一个字符串
        if isinstance(r, str):
            #字符串中包含重定向
            if r.startswith('redirect:'):
                return web.HTTPFound(r[9:])
            resp = web.Response(body=r.encode('utf-8'))
            resp.content_type = 'text/html;charset=utf-8'
            return resp
        #如果处理函数返回的是一个dict
        if isinstance(r, dict):
            template = r.get('__template__')
            #如果返回的dict没有指定渲染模板，就是Api函数返回的json数据
            if template is None:
                resp = web.Response(body=json.dumps(r, ensure_ascii=False, default=lambda o: o.__dict__).encode('utf-8'))
                resp.content_type = 'application/json;charset=utf-8'
                return resp
            #利用返回的dict数据渲染指定的模板
            else:
                resp = web.Response(body=app['__templating__'].get_template(template).render(**r).encode('utf-8'))
                resp.content_type = 'text/html;charset=utf-8'
                return resp
        if isinstance(r, int) and r >= 100 and r < 600:
            return web.Response(r)
        if isinstance(r, tuple) and len(r) == 2:
            t, m = r
            if isinstance(t, int) and t >= 100 and t < 600:
                return web.Response(t, str(m))
        # default:
        resp = web.Response(body=str(r).encode('utf-8'))
        resp.content_type = 'text/plain;charset=utf-8'
        return resp
    return response

def datetime_filter(t):
    delta = int(time.time() - t)
    if delta < 60:
        return u'1分钟前'
    if delta < 3600:
        return u'%s分钟前' % (delta // 60)
    if delta < 86400:
        return u'%s小时前' % (delta // 3600)
    if delta < 604800:
        return u'%s天前' % (delta // 86400)
    dt = datetime.fromtimestamp(t)
    return u'%s年%s月%s日' % (dt.year, dt.month, dt.day)

async def init(loop):
    #异步利用orm创建数据库连接池
    await orm.create_pool(loop=loop, host='127.0.0.1', port=3306, user='root', password='120788', db='awesome')
    #利用aiohttp模块的web创建应用，并注册中间 件函数
    app = web.Application(loop=loop, middlewares=[
        logger_factory, response_factory
    ])
    #初始化jinjia2模板，注册模板的filter
    init_jinja2(app, filters=dict(datetime=datetime_filter))
    #将handleers.py中的url函数注册到app中
    add_routes(app, 'handlers')
    #添加静态路由
    add_static(app)
    #创建server，并监听ip和指定的端口
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9001)
    logging.info('server started at http://127.0.0.1:9001...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()