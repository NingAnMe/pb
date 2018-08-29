# -*- coding: utf-8 -*-

import os
import re
import sys
import time

import h5py

from PB import pb_sat
from PB.pb_io import attrs2dict
from PB.pb_time import fy3_ymd2seconds
from PB.pb_time import get_ymd, get_hm
from pb_drc_base import ReadL1
import numpy as np


__description__ = u'MERSI传感器读取类'
__author__ = 'wangpeng'
__date__ = '2018-08-28'
__version__ = '1.0.0_beat'
# __updated__ = '2018-08-28'


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class ReadMersiL1(ReadL1):

    """
    mersi数据解析类,进行解耦合设计
        分辨率：1000
        卫星： FY3A FY3B FY3C
        通道数量：10
        可见光通道：1 2 6 7 8 9 10
        红外通道：3 4 5
    """

    def __init__(self, in_file):
        sensor = 'MERSI'
        super(ReadMersiL1, self).__init__(in_file, sensor)

        # 中心波数: wn(cm-1) = 10 ^ 7 / wave_length(nm)
        self.central_wave_number = {'CH_05': 869.565}

    def set_resolution(self):
        """
        根据L1文件名 set self.resolution 分辨率
        :return:
        """
        file_name = os.path.basename(self.in_file)
        if '1000M' in file_name:
            self.resolution = 1000
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def set_satellite(self):
        """
        根据L1文件名 set self.satellite 卫星名
        :return:
        """
        file_name = os.path.basename(self.in_file)
        pattern = r'([A-Z0-9]+)_%s.*' % self.sensor
        m = re.match(pattern, file_name)
        if m:
            self.satellite = m.groups()[0]
        else:
            raise ValueError('Cant get the satellite name from file name.')

    def set_ymd_hms(self):
        """
        根据根据L1文件名 set self.level_1_ymd 和 self.level_1_ymd
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
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    self.file_attr = attrs2dict(hdf5_file.attrs)
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))

    def set_dataset_shape(self):
        """
        根据 self.satellite set self.dataset_shape
        :return:
        """
        # 如果分辨率是 1000 米
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                self.data_shape = (2000, 2048)
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        # elif self.resolution == 250:
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))

    def set_channels(self):
        """
        根据 self.satellite set self.sensor_channel_amount
        :return:
        """
        if self.resolution == 1000:
            self.channels = 20
        # elif self.resolution == 250:
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def get_dn(self):
        """
        从数据文件中获取 DN 值, set self.dn
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                data_file = self.in_file
                if not os.path.isfile(data_file):
                    raise ValueError(
                        'Data file is not exist. {}'.format(data_file))
#                 s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(data_file, 'r') as hdf5_file:
                    #                     emissive = hdf5_file.get('/EV_Emissive')[:, :s[0], :s[1]]
                    #                     ref_sb = hdf5_file.get('/EV_RefSB')[:, :s[0], :s[1]]
                    ary_ch1 = hdf5_file.get('/EV_250_Aggr.1KM_RefSB').value
                    ary_ch5 = hdf5_file.get('/EV_250_Aggr.1KM_Emissive').value
                    ary_ch6 = hdf5_file.get('/EV_1KM_RefSB').value
            elif self.satellite in satellite_type2:
                data_file = self.in_file
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch1 = hdf5_file.get(
                        '/Data/EV_250_Aggr.1KM_RefSB').value
                    ary_ch5 = hdf5_file.get(
                        '/Data/EV_250_Aggr.1KM_Emissive').value
                    ary_ch6 = hdf5_file.get('/Data/EV_1KM_RefSB').value
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 1 2 6 7 8 9 10 为可见光通道，dn 值为 ref_sb
            # 3 4 5 为红外通道，dn 值为 emissive
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                if i < 4:
                    k = i
                    data_pre = ary_ch1[k]
                    # 开始处理
                    data_pre = data_pre.astype(np.float32)
                    invalid_index = np.logical_or(
                        data_pre <= 0, data_pre > 10000)
                    data_pre[invalid_index] = np.nan
                    channel_data = data_pre
                elif i == 4:
                    data_pre = ary_ch5
                    # 开始处理
                    data_pre = data_pre.astype(np.float32)
                    invalid_index = np.logical_or(
                        data_pre <= 0, data_pre > 10000)
                    data_pre[invalid_index] = np.nan
                    channel_data = data_pre
                else:
                    k = i - 5
                    data_pre = ary_ch6[k]
                    # 开始处理
                    data_pre = data_pre.astype(np.float32)
                    invalid_index = np.logical_or(
                        data_pre <= 0, data_pre > 10000)
                    data_pre[invalid_index] = np.nan
                    channel_data = data_pre
                data[channel_name] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_coefficient(self):

        data0 = dict()
        data1 = dict()
        data2 = dict()

        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # vis_k, 54 = 19*3  （19通道 3个系数）
                tmp_k = self.file_attr['VIR_Cal_Coeff']
                K = np.full((19, 3), 0.)
                for i in range(19):
                    for j in range(3):
                        K[i, j] = tmp_k[i * 3 + j]
                # 变成20*3  k0,k1,k2
                values = np.array([0, 1, 0])
                K = np.insert(K, 4, values, 0)

            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    K = hdf5_file.get('/Calibration/VIS_Cal_Coeff').value

                values = np.array([0, 1, 0])
                k = np.insert(K, 4, values, 0)
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                # k0
                channel_data = np.full(
                    self.data_shape, k[i, 0], dtype=np.float32)
                data0[channel_name] = channel_data
                # k1
                channel_data = np.full(
                    self.data_shape, k[i, 1], dtype=np.float32)
                data1[channel_name] = channel_data
                # k2
                channel_data = np.full(
                    self.data_shape, k[i, 2], dtype=np.float32)
                data2[channel_name] = channel_data

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data0, data1, data2

    def get_ref(self):
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            ref_channels = ['CH_{:02d}'.format(i) for i in xrange(1, 21, 1)]
            ref_channels.remove('CH_05')
            if self.satellite in satellite_type1:
                dn = self.get_dn()
                k0, k1, k2 = self.get_k0()

                for channel_name in dn:
                    if channel_name not in ref_channels:
                        continue
                    channel_data = dn[channel_name]**2 * k2[channel_name] + dn[channel_name] * \
                        k1[channel_name] + k0[channel_name]
                    data[channel_name] = channel_data / 100.
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_rad(self):

        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                rad_pres = self.get_rad_pre()
                b0_b1_b2_nonlinear = self.file_attr.get(
                    'Prelaunch_Nonlinear_Coefficients')

                # 通道 3 4 5
                for i in xrange(3):
                    channel_name = 'CH_{:02d}'.format(i + 3)
                    b0 = b0_b1_b2_nonlinear[i]
                    b1 = b0_b1_b2_nonlinear[3 + i]
                    b2 = b0_b1_b2_nonlinear[6 + i]
                    rad_pre = rad_pres[channel_name]
                    rad_nonlinear = rad_pre ** 2 * b2 + rad_pre * b1 + b0
                    rad = rad_pre + rad_nonlinear
                    data[channel_name] = rad
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

if __name__ == '__main__':
    L1File = 'D:/data/FY3C_MERSI/FY3C_MERSI_GBAL_L1_20150223_2340_1000M_MS.HDF'
    mersi = ReadMersiL1(L1File)
    print mersi.satellite  # 卫星名
    print mersi.sensor  # 传感器名
    print mersi.ymd  # L1 文件年月日 YYYYMMDD
    print mersi.hms  # L1 文件时分秒 HHMMSS
    print mersi.resolution  # 分辨率
    print mersi.channels  # 通道数量
    print mersi.data_shape

    def print_data_status(datas, name=None):
        data_shape = datas.shape
        data_min = np.nanmin(datas)
        data_max = np.nanmax(datas)
        data_mean = np.nanmean(datas)
        data_median = np.nanmedian(datas)
        print "{}: shape: {}, min: {}, max: {}, mean: {}, median: {}".format(
            name, data_shape, data_min, data_max, data_mean, data_median)

    def print_channel_data(datas):
        if not isinstance(datas, dict):
            return
        keys = list(datas.viewkeys())
        keys.sort()
        for t_channel_name in keys:
            channel_data = datas[t_channel_name]
            print_data_status(channel_data, name=t_channel_name)
    print 'dn:'
    t_data = mersi.get_dn()
    print_channel_data(t_data)
    print 'k0:'
    k0, k1, k2 = mersi.get_coefficient()
    print_channel_data(k0)
    print 'k1:'
    print_channel_data(k1)
    print 'k2:'
    print_channel_data(k2)
    print 'ref:'
    t_data = mersi.get_ref()
    print_channel_data(t_data)

#     print mersi.file_attr  # L1 文件属性
