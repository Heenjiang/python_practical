#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, logging
import aiomysql

#python 的全局变量定义的时候直接在函数外定义，不用global关键字。global关键子用于在函数内部访问或者修改全局变量时使用
__pool = None

#在控制台打印出sql的信息
def log(sql, args=()):
    logging.info('SQL: %s' % sql)
#创建数据库连接池，这样可以提升效率，最大效率的利用已有的链接。比如：如果不用连接池，每一个请求都建立一个数据库连接，io操作后
#再关闭，但是用了连接池之后，一旦有需要获得数据库连接时候，直接从连接池里取，连接池里如果没有连接才会创建。
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    #读取全局变量的申明
    global __pool 
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

#执行select语句的函数，返回查询的结果
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    #获取数据库连接
    async with __pool.get() as conn:
        #获取数据库游标
        #A cursor which returns results as a dictionary
        async with conn.cursor(aiomysql.DictCursor) as cur:
            #因为mysql的占位符是'?'，所以压要将'%s'替换成'?'。之后再利用cur执行sql语句
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs
#执行处select之外的其他语句，因为update, delete, insert这些操作期待的返回值都是影响的行数(或者成功与否)
#所以可以用同一个执行函数来执行这三种操作，返回值：如果操作成功返回数据表中受影响的行数，如果操作失败raise error
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected
#根据参数的个数，创建预定义的参数列表:(?, ?, ?, ?)
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

#model类属性的基本类型
class Field(object):
    #初始化方法，
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    #重写__str__方法
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
#继承自Field基本类型的string
class StringField(Field):
    #初始化，并且调用父类的__init__方法， varchar: 不定长字符串，注意不能超过定义的长度，否则超过的部分被自动截断
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        #real:是sql的一种数据类型，一个real类型的数据占4个字节，它可以描述7个精度
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

#python用来创建并实现ORM的魔术类
class ModelMetaclass(type):
    #这个方法用于具体创建
    def __new__(cls, name, bases, attrs):
        #如果调用这个方法的实例是model的直接类型，就返回
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        #获取Model子类型实列的所对应的数据库表的名称，如果没有就默认为该类型的类名
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        #Model子类类属性的对应关系
        mappings = dict()
        #除开主键外的其他类属性对应关系
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 找到主键:
                if v.primary_key:
                    # 如果找到主键之后，又出现主键
                    if primaryKey:
                        raise Exception('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise Exception('Primary key not found.')
        #将已经添加到mapping的属性从attrs中删除
        for k in mappings.keys():
            attrs.pop(k)
        #map(function(), Iterable)是python的全局函数，用于对Iterable对象中的每个元素应用function()，返回一个Iterator 
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
#所有model子类的父类，继承了dict和ModelMetaclass.
#这样的好处是所有的model的子类隐形继承了metaclass，当创建实例时，python解释器会检查当前类的定义和父类的定义有没有metacalss，
#如果有就会直接调用metaclass类的__new()__方法创建对象
#又由于Model也继承了dict，所以堆属性的访问，可以使用a.b或者a[b]
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value
    #获取实例属性的值
    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        #如果实例属性值为None，就获取对应的类属性的默认值
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                #如果类属性的是一个函数就调用
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                #将类属性的默认值赋给实例
                setattr(self, key, value)
        return value
    # @classmethod是用来修饰类方法的，用该标识符修饰的方法不需要传入self对象，所以可以直接类名.方法名调用
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        #这个方法一般是由类名直接调用的，例如：User.findAll(args...)
        sql = [cls.__select__]
        #根据参数添加sql的子句
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        #由于定义所有__***sql__的时候，都是list，所以最后拼接的时候直接每个子句空格隔开就行了
        rs = await select(' '.join(sql), args)
        #cla(**r)创建实例，最后返回的是一个实例列表
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)