# coding: utf-8

'''
Created on 2017年9月7日

@author: wangpeng
'''

import os
import re
import shutil

import h5py

from DV import dv_plt
import numpy as np


# 配置文件信息，设置为全局
MainPath, MainFile = os.path.split(os.path.realpath(__file__))


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

if __name__ == '__main__':

    inpath = r'D:\download\FY3B_MERSI_SRF\FY3B_MERSI\FY3B_mersi_srf_B1-4_B8-20.txt'

    data = np.loadtxt(inpath)
    ofile = r'D:\download\FY3B_MERSI_SRF\FY3B_MERSI_SRF_CH21_Pub.txt'
    print data.shape

    newdata0 = data[:, 0]
    newdata1 = data[:, 18]
    newdata0 = newdata0.reshape(len(newdata0), 1)
    newdata1 = newdata1.reshape(len(newdata1), 1)

    newdata = np.concatenate((newdata0, newdata1), axis=1)

    np.savetxt(ofile, newdata, fmt='%.7e')
