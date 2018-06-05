# coding: utf-8
import os
import re
import errno
import yaml
from datetime import datetime

__description__ = u'IO处理的函数'
__author__ = 'wangpeng'
__date__ = '2018-01-25'
__version__ = '1.0.0_beat'


def makeYamlCfg(yaml_dict, oFile):
    # 投影程序需要的配置文件名称
    cfgPath = os.path.dirname(oFile)
    if not os.path.isdir(cfgPath):
        os.makedirs(cfgPath)
    with open(oFile, 'w') as stream:
            yaml.dump(yaml_dict, stream, default_flow_style=False)


def loadYamlCfg(iFile):

    if not os.path.isfile(iFile):
        print("No yaml: %s" % (iFile))
        return None
    try:
        with open(iFile, 'r') as stream:
            plt_cfg = yaml.load(stream)

        # check yaml
        if 'chan' in plt_cfg and 'plot' in plt_cfg and 'days' in plt_cfg:
            pass
        else:
            print ('plt yaml lack some keys')
            plt_cfg = None
    except:
        print('plt yaml not valid: %s' % iFile)
        plt_cfg = None

    return plt_cfg


def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def FindFile(ipath, pat):
    '''
    # 要查找符合文件正则的文件
    '''
    FileLst = []

    if not os.path.isdir(ipath):
        return FileLst
    Lst = sorted(os.listdir(ipath), reverse=False)
    for Line in Lst:
        FullPath = os.path.join(ipath, Line)
        if os.path.isdir(FullPath):
            continue
        # 如果是文件则进行正则判断，并把符合规则的所有文件保存到List中
        elif os.path.isfile(FullPath):
            FileName = Line
            m = re.match(pat, FileName)
            if m:
                FileLst.append(FullPath)
    if len(FileLst) == 0:
        return FileLst
    else:
        return FileLst


def find_file(path, reg):
    '''
    path: 要遍历的目录
    reg: 符合条件的文件
    '''
    FileLst = []
    try:
        lst = os.walk(path)
        for root, dirs, files in lst:
            for name in files:
                try:
                    m = re.match(reg, name)
                except Exception as e:
                    continue
                if m:
                    FileLst.append(os.path.join(root, name))
    except Exception as e:
        print str(e)

    return sorted(FileLst)


def path_replace_ymd(path, ymd):
    '''
    path:替换路径中的日期 ,path中%YYYY%MM%DD%JJJ 等关键字会被ymd日期实例
    ymd: yyyymmdd  (20180101)
    '''
    # 转成datetime类型
    ymd = datetime.strptime(ymd, '%Y%m%d')
    yy = ymd.strftime('%Y')
    mm = ymd.strftime('%m')
    dd = ymd.strftime('%d')
    jj = ymd.strftime('%j')
    path = path.replace('%YYYY', yy)
    path = path.replace('%MM', mm)
    path = path.replace('%DD', dd)
    path = path.replace('%JJJ', jj)

    return  path


def load_yaml_config(in_file):
    """
    加载 Yaml 文件
    :param in_file:
    :return: Yaml 类
    """
    if not os.path.isfile(in_file):
        print "File is not exist: {}".format(in_file)
        return None
    try:
        with open(in_file, 'r') as stream:
            yaml_data = yaml.load(stream)
    except IOError as why:
        print why
        print "Load yaml file error."
        yaml_data = None

    return yaml_data


def is_none(*args):
    """
    判断传入的变量中是否有 None
    :param args:
    :return:
    """
    has_none = False
    for arg in args:
        if arg is None:
            has_none = True
    return has_none


def copy_attrs_h5py(pre_object, out_object):
    """
    复制 file、dataset 或者 group 的属性
    :param pre_object: 被复制属性的 dataset 或者 group
    :param out_object: 复制属性的 dataset 或者 group
    :return:
    """
    for akey in pre_object.attrs.keys():
        out_object.attrs[akey] = pre_object.attrs[akey]


if __name__ == '__main__':
    pass


#     path_replace_ymd('/abc/%YYYY/%MM%DD/%JJJ', '20180101')
