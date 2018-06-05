# coding:utf-8 
'''
Created on 2015年8月13日

@author: zhangtao
'''
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
import os

class classManager(object):
    '''
    插件类的装饰器
    '''
    PLUGINS = []
    def getClass(self, text):
        for plugin in self.PLUGINS:
            if plugin().check(text):
                return plugin
        return None

    def getInstance(self, text):
        for plugin in self.PLUGINS:
            inst = plugin()
            if inst.check(text):
                return inst
        return None
    
    @classmethod
    def plugin(cls, plugin):
        cls.PLUGINS.append(plugin)

class dataset(object):
    ''' hdf base class '''
    def __init__(self, name, product, ary, dtype):
        self.name = name
        self.product = product
        self.ary = ary
        self.dtype = dtype
             
class satDataBase(object):  # metaclass=ABCMeta
    ''' hdf base class '''
    __metaclass__ = ABCMeta
    def __init__(self, sat, sensor, pat, resM, region):
        self.sat = sat
        self.sensor = sensor
        self.pat = pat
        self.resM = resM
        self.region = region
        self.level = ''  # 数据等级
        self.proj = 'GLL'
        self.dtype = 'f4'
        self.ymd = None
        self.hms = None
        self.totalSec = None
        self.secondaryFiles = []
        self.Bands = {}

    @abstractmethod
    def check(self, fileName):
        pass

    @abstractmethod
    def load(self, filePath):
        pass
        
    def open(self, filePath):
        fileName = os.path.split(filePath)[1]
        if not self.check(fileName):
            print("Check Failed!\n%s" % fileName)
        self.load(filePath)
            
    def close(self):
        if hasattr(self, 'lons'):
            del self.lons
        if hasattr(self, 'lats'):
            del self.lats
        if hasattr(self, 'bands'):
            del self.bands
        if hasattr(self, 'dset'):
            del self.dset
    
    def add2Bands(self, bandName, product, ary, dtype='f4'):
        self.Bands[bandName] = dataset(bandName, product, ary, dtype)
    
    def valid_condition(self):
        return None
    
    @property
    def resDg(self):  # 分辨率转成度
        return self.resM / 100000.
    
    @property
    def dt_s(self):
        return datetime.strptime('%s %s' % (self.ymd, self.hms), "%Y%m%d %H%M%S")

    @property
    def dt_e(self):
        return datetime.strptime('%s %s' % (self.ymd, self.hms), "%Y%m%d %H%M%S") + timedelta(seconds=(self.totalSec))
    
class L1Class(satDataBase):  # metaclass=ABCMeta
    ''' 
    L1 base class 
    '''
    __metaclass__ = ABCMeta
    def __init__(self, sat, sensor, pat, res, region):
        super(L1Class, self).__init__(sat, sensor, pat, res, region)
        self.level = 'L1'
    
class L2Class(satDataBase):  # metaclass=ABCMeta
    ''' 
    L2 base class 
    '''
    __metaclass__ = ABCMeta
    def __init__(self, sat, sensor, pat, res, region):
        super(L2Class, self).__init__(sat, sensor, pat, res, region)
        self.level = 'L2'

class L3Class(satDataBase):  # metaclass=ABCMeta
    ''' 
    L3 base class 
    '''
    __metaclass__ = ABCMeta
    def __init__(self, sat, sensor, pat, res, region):
        super(L3Class, self).__init__(sat, sensor, pat, res, region)
        self.level = 'L3'
             
if __name__ == '__main__':
    pass
