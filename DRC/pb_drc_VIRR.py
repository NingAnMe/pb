#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/8/16 14:12
@Author  : AnNing
"""
import os
import re

import h5py
from PB.pb_time import get_ymd, get_hm
from PB.pb_io import attrs2dict


class ReadL1(object):
    def __init__(self, in_file):

        self.in_file = in_file
        self.error = False

        # 1数据外部描述信息
        self.satellite = None  # 卫星名
        self.sensor = 'VIRR'  # 传感器名
        self.ymd = None  # L1 文件年月日 YYYYMMDD
        self.hms = None  # L1 文件时分秒 HHMMSS
        self.resolution = None  # 分辨率
        self.channels = None  # 通道数量
        self.file_attr = None  # L1 文件属性
        self.data_shape = None

        # 2数据内部物理量,按通道存放
        # 通道相关
        # 使用字典储存每个通道的数据，np.ndarray 二维矩阵
        # ch_01_dn_data = np.array(self.shape, np.nan, dtype=float32)
        # self.dn = {'CH_01': ch_01_dn_data, 'CH_02': ch_02_dn_data}
        self.dn = None
        self.ref = None
        self.rad = None
        self.tbb = None
        self.sv = None
        self.bb = None
        self.k0 = None
        self.k1 = None
        self.k2 = None

        # 3数据内部物理量,非通道相关
        # 非通道相关
        # 使用 np.ndarray 二维矩阵存储
        # self.height = np.array(self.shape, np.nan, dtype=float32)
        self.height = None
        self.latitude = None
        self.longitude = None
        self.land_sea_mask = None
        self.land_cover = None
        self.sensor_azimuth = None
        self.sensor_zenith = None
        self.sun_azimuth = None
        self.sun_zenith = None
        self.relative_azimuth = None
        self.UTC_timestamp = None

        # 执行初始化相关方法
        # 初始化卫星名
        self.set_satellite()
        # 初始化与 L1 对应的 GEO 和 OBC 文件，如果有
        self.set_file_geo_obc()
        # 初始化数据的分辨率
        self.set_resolution()
        # L1 文件名中获取并初始化 ymd hms
        self.set_ymd_hms()
        # 初始化文件属性
        self.set_file_attr()
        # 初始化获取数据处理完成以后的大小。
        self.set_dataset_shape()
        # 初始化通道数
        self.set_channels()

    def set_satellite(self):
        """
        根据文件名 set self.satellite
        :return:
        """
        pass

    def set_file_geo_obc(self):
        """
        根据 self.file_level_1 set self.file_geo 和 self.file_obc
        :return:
        """
        pass

    def set_ymd_hms(self):
        """
        根据 self.file_level_1 set self.level_1_ymd 和 self.level_1_ymd
        :return:
        """
        pass

    def set_file_attr(self):
        """
        根据 self.file_level_1 获取 L1 文件的属性
        set self.level_1_attr
        储存格式是字典
        :return:
        """
        pass

    def set_dataset_shape(self):
        """
        根据 self.satellite set self.dataset_shape
        :return:
        """
        pass

    def set_resolution(self):
        """
        根据 self.satellite set self.sensor_resolution_ratio
        :return:
        """
        pass

    def set_channels(self):
        """
        根据 self.satellite set self.sensor_channel_amount
        :return:
        """
        pass

    def set_dn(self):
        """
        从数据文件中获取 DN 值, set self.dn
        :return:
        """
        pass

    def get_dn(self):
        """
        如果self.dn已经获取，返回 self.dn
        否则执行 self.set_dn 以后，再返回 self.dn
        :return:
        """
        pass


class ReadVirrL1(ReadL1):
    """
    读取VIRR传感器的L1数据
    目前支持 FY3A FY3B FY3C
    """
    def __init__(self, in_file):
        super(ReadVirrL1, self).__init__(in_file)

        # 固定值
        # 红外通道的中心波数，固定值，MERSI_Equiv Mid_wn (cm-1)
        self.WN = {'CH_03': 2673.796, 'CH_04': 925.925, 'CH_05': 833.333}
        # 红外转tbb的修正系数，固定值
        self.TeA = {'CH_03': 1, 'CH_04': 1, 'CH_05': 1}
        self.TeB = {'CH_03': 0, 'CH_04': 0, 'CH_05': 0}

    def set_satellite(self):
        """
        根据文件名 set self.satellite
        :return:
        """
        file_name = os.path.basename(self.in_file)
        pattern = r'([A-Z0-9]+)_%s.*' % self.sensor
        m = re.match(pattern, file_name)
        if m:
            self.satellite = m.groups()[0]
        else:
            raise ValueError('Cant get the satellite name from file name.')

    def set_file_geo_obc(self):
        """
        根据 self.file_level_1 set self.file_geo 和 self.file_obc
        :return:
        """
        pass

    def set_ymd_hms(self):
        """
        根据 self.file_level_1 set self.level_1_ymd 和 self.level_1_ymd
        :return:
        """
        file_name = os.path.basename(self.in_file)
        self.ymd = get_ymd(file_name)
        self.hms = get_hm(file_name) + '00'

    def set_file_attr(self):
        """
        根据 self.file_level_1 获取 L1 文件的属性
        set self.level_1_attr
        储存格式是字典
        :return:
        """
        with h5py.File(self.in_file, 'r') as hdf5_file:
            return attrs2dict(hdf5_file.attrs)

    def set_dataset_shape(self):
        """
        根据 self.satellite set self.dataset_shape
        :return:
        """
        # 如果分辨率是 1000 米
        if self.resolution == 1000:
            self.data_shape = (1800, 2048)
        # 如果分辨率是 250 米
        elif self.resolution == 250:
            self.data_shape = ()

    def set_resolution(self):
        """
        根据 self.satellite set self.sensor_resolution_ratio
        :return:
        """
        pass

    def set_channels(self):
        """
        根据 self.satellite set self.sensor_channel_amount
        :return:
        """
        pass

    def set_dn(self):
        """
        从数据文件中获取 DN 值, set self.dn
        :return:
        """
        pass

    def get_dn(self):
        """
        如果self.dn已经获取，返回 self.dn
        否则执行 self.set_dn 以后，再返回 self.dn
        :return:
        """
        if self.dn is not None:
            return self.dn
        else:
            self.set_dn()
            return self.dn


if __name__ == '__main__':
    t_in_file = r'D:\nsmc\fix_data\FY3ABC\FY3A_VIRRX_GBAL_L1_20081106_2055_1000M_MS.HDF'
    t_read_l1 = ReadVirrL1(t_in_file)
    print t_read_l1.satellite  # 卫星名
    print t_read_l1.sensor  # 传感器名
    print t_read_l1.ymd  # L1 文件年月日 YYYYMMDD
    print t_read_l1.hms  # L1 文件时分秒 HHMMSS
    print t_read_l1.resolution  # 分辨率
    print t_read_l1.channels  # 通道数量
    print t_read_l1.file_attr  # L1 文件属性
    print t_read_l1.data_shape
