#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/8/21 17:51
@Author  : AnNing
"""
import os


class ReadL1(object):
    """
    读取处理 L1 数据
    传感器：
    目前支持卫星：
    通道数量：
    可见光通道：
    红外通道：
    """
    def __init__(self, in_file, sensor):

        if not os.path.isfile(in_file):
            raise ValueError('{} is not exist.'.format(in_file))
        self.in_file = in_file

        # 1数据外部描述信息
        self.satellite = None  # 卫星名
        self.sensor = sensor  # 传感器名
        self.ymd = None  # L1 文件年月日 YYYYMMDD
        self.hms = None  # L1 文件时分秒 HHMMSS
        self.resolution = None  # 分辨率
        self.channels = None  # 通道数量
        self.file_attr = None  # L1 文件属性
        self.data_shape = None  # 处理以后的数据 np.shape

        # 2数据内部物理量,按通道存放
        # 通道相关
        # 使用字典储存每个通道的数据，np.ndarray 二维矩阵
        # ch_01_dn_data = np.array(self.shape, np.nan, dtype=float32)
        # self.dn = {'CH_01': ch_01_dn_data, 'CH_02': ch_02_dn_data}
        self.dn = {}  # 仪器响应值
        self.ref = {}  # 反射率
        self.rad = {}  # 辐亮度
        self.tbb = {}  # 亮温
        self.sv = {}  # 空间温度
        self.bb = {}  # 黑体
        self.k0 = {}  # 定标系数
        self.k1 = {}  # 定标系数
        self.k2 = {}  # 定标系数

        # 3数据内部物理量,非通道相关
        # 非通道相关
        # 使用 np.ndarray 二维矩阵存储
        # self.height = np.array(self.shape, np.nan, dtype=float32)
        self.height = None  # 高度
        self.latitude = None  # 纬度
        self.longitude = None  # 经度
        self.land_sea_mask = None  # 海陆掩码
        self.land_cover = None  # 地表类型
        self.sensor_azimuth = None  # 卫星方位角
        self.sensor_zenith = None  # 卫星天顶角
        self.sun_azimuth = None  # 太阳方位角
        self.sun_zenith = None  # 太阳天顶角
        self.relative_azimuth = None  # 相对方位角
        self.timestamp = None  # 距离 1970 年 1 月 1 日的时间戳
        self.wave_number = None  # 传感器的中心波

        # 执行初始化相关方法
        # 初始化数据的分辨率
        self.set_resolution()
        # 初始化卫星名
        self.set_satellite()
        # 初始化与 L1 对应的 GEO 和 OBC 文件，如果有
        self.set_file_geo_obc()
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

    def set_resolution(self):
        """
        根据 self.satellite set self.sensor_resolution_ratio
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

    def set_channels(self):
        """
        根据 self.satellite set self.sensor_channel_amount
        :return:
        """
        pass

    def get_dn(self):
        return

    def get_ref(self):
        return

    def get_rad(self):
        return

    def get_tbb(self):
        return

    def get_sv(self):
        return

    def get_bb(self):
        return

    def get_k0(self):
        return

    def get_k1(self):
        return

    def get_k2(self):
        return

    def get_height(self):
        return

    def get_latitude(self):
        return

    def get_longitude(self):
        return

    def get_land_sea_mask(self):
        return

    def get_land_cover(self):
        return

    def get_sensor_azimuth(self):
        return

    def get_sensor_zenith(self):
        return

    def get_sun_azimuth(self):
        return

    def get_sun_zenith(self):
        return

    def get_relative_azimuth(self):
        return

    def get_timestamp(self):
        return

    def get_wave_number(self):
        return

    def get_wave_length(self):
        return

    def get_response_value(self):
        return
