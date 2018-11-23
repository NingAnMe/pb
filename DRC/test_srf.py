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

    #     inpath = r'D:\download\FY3B_MERSI_SRF\FY3B_MERSI\FY3B_mersi_srf_B1-4_B8-20.txt'
    #
    #     data = np.loadtxt(inpath)
    #     ofile = r'D:\download\FY3B_MERSI_SRF\FY3B_MERSI_SRF_CH21_Pub.txt'
    #     print data.shape
    #
    #     newdata0 = data[:, 0]
    #     newdata1 = data[:, 18]
    #     newdata0 = newdata0.reshape(len(newdata0), 1)
    #     newdata1 = newdata1.reshape(len(newdata1), 1)
    #
    #     newdata = np.concatenate((newdata0, newdata1), axis=1)
    #
    #     np.savetxt(ofile, newdata, fmt='%.7e')
    #     inpath = r'D:\download\FY3A_VIRR\FY3A_VIRR_SRF_CH05_Pub.txt'
    #     ofile = r'D:\download\FY3A_VIRR\FY3A_VIRR_SRF_CH05_Pub1.txt'
    #     dtype = {'names': ('wave_length', 'response'), 'formats': ('f4', 'f4')}
    #     datas = np.loadtxt(inpath, dtype=dtype)
    #     print datas.shape
    #     wave_length = datas['wave_length'][::-1]
    #     wave_number_channel = 10 ** 7 / wave_length
    # #     wave_number_dict[channel_name] = wave_number_channel
    #     response_channel = datas['response'][::-1]
    #     a = wave_number_channel.reshape(len(wave_number_channel), 1)
    #     b = response_channel.reshape(len(response_channel), 1)
    #     c = np.concatenate((a, b), axis=1)
    #     print wave_number_channel.shape
    #     print response_channel
    #     print c
    #     print c.shape
    #     np.savetxt(ofile, c, fmt='%.7e')

    reg1 = '.*_(\d{8})_(\d{4})_lon(.{8})_lat(.{8})_(\w+).H5$'
    reg = '.*_(\d{8})_(\d{4})_lon(−?\d∗\.\d∗)_lat(−?\d∗\.\d∗).*'
    filename = 'FY3C+VIRR_20180101_0005_lon+123.000_lat-074.500_Dome_C.H5'
    regf = '-?\d+\.\d+'
    lon = re.findall(regf, filename)
    print lon, type(lon[0])
    RR = re.match(reg1, filename)
    print RR
    print RR.group(1)
    print RR.group(2)
    print float(RR.group(3))
    print float(RR.group(4))
    print RR.group(5)
