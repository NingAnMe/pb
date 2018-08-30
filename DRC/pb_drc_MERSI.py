# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re
import sys
import time

import h5py

from PB import pb_sat
from PB.pb_io import attrs2dict
from PB.pb_sat import planck_r2t
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

    def get_central_wave_number(self):
        '''
        central_wave_number
        wn(cm-1) = 10 ^ 7 / wave_length(nm)
        '''
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            satellite_type2 = ['FY3D']
            if self.satellite in satellite_type1:
                data = {'CH_05': 869.565}
            elif self.satellite in satellite_type2:
                data = {'CH_20': 2634.359, 'CH_21': 2471.654, 'CH_22':
                        1382.621, 'CH_23': 1168.182, 'CH_24': 933.364, 'CH_25': 836.941}
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def __get_geo_file(self):
        """
        返回 GEO 文件
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3C']
            if self.satellite in satellite_type1:
                geo_file = self.in_file[:-12] + 'GEO1K_MS.HDF'
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))
        return geo_file

    def get_dn(self):
        """
        DN
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
                band = 'CH_{:02d}'.format(i + 1)
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
                data[band] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_coefficient(self):
        """
        K0,K1,K2
        :return:
        """

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

            # 逐个通道处理
            for i in xrange(self.channels):
                band = 'CH_{:02d}'.format(i + 1)
                # k0
                channel_data = np.full(
                    self.data_shape, k[i, 0], dtype=np.float32)
                data0[band] = channel_data
                # k1
                channel_data = np.full(
                    self.data_shape, k[i, 1], dtype=np.float32)
                data1[band] = channel_data
                # k2
                channel_data = np.full(
                    self.data_shape, k[i, 2], dtype=np.float32)
                data2[band] = channel_data

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data0, data1, data2

    def get_ref(self):
        """
        Ref
        :return:
        """

        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']

            if self.satellite in satellite_type1:
                dn = self.get_dn()
                k0, k1, k2 = self.get_coefficient()

                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if 'CH_05' in band:
                        continue

                    channel_data = dn[band]**2 * k2[band] + dn[band] * \
                        k1[band] + k0[band]
                    data[band] = channel_data / 100.
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_rad(self):
        """
        TBB
        :return:
        """

        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                dn = self.get_dn()
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if 'CH_05' in band:
                        data[band] = dn[band] / 100.
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb(self):
        """
        TBB
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            # rad转tbb的修正系数，所有时次都是固定值
            rad2tbb_k0 = {'CH_05': 1}
            rad2tbb_k1 = {'CH_05': 0}
            if self.satellite in satellite_type1:
                rads = self.get_rad()
                central_wave_numbers = self.get_central_wave_number()
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if 'CH_05' in band:
                        k0 = rad2tbb_k0[band]
                        k1 = rad2tbb_k1[band]
                        central_wave_number = central_wave_numbers[band]
                        rad = rads[band]
                        tbb = planck_r2t(rad, central_wave_number)
                        data[band] = tbb * k0 + k1
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sv(self):
        """
        TBB
        :return:
        """
        data = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/SV_DN_average').value
            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get(
                        '/Calibration/SV_DN_average').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre <= 0, data_pre > 4095)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan

            # 逐个通道处理
            for i in xrange(self.channels):
                band = 'CH_{:02d}'.format(i + 1)
                channel_data = np.full(
                    self.data_shape, np.nan, dtype=np.float32)
                if self.satellite in satellite_type1:
                    channel_data[:] = data_pre.reshape(-1, 1)
                elif self.satellite in satellite_type2:
                    data_pre_new = np.repeat(data_pre[i, :], 10)
                    channel_data[:] = data_pre_new.reshape(-1, 1)
                data[band] = channel_data

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_bb(self):
        """
        TBB
        :return:
        """
        data = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/BB_DN_average').value
            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get(
                        '/Calibration/BB_DN_average').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre <= 0, data_pre > 4095)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan

            # 逐个通道处理
            for i in xrange(self.channels):
                band = 'CH_{:02d}'.format(i + 1)
                channel_data = np.full(
                    self.data_shape, np.nan, dtype=np.float32)
                if self.satellite in satellite_type1:
                    channel_data[:] = data_pre.reshape(-1, 1)
                elif self.satellite in satellite_type2:
                    data_pre_new = np.repeat(data_pre[i, :], 10)
                    channel_data[:] = data_pre_new.reshape(-1, 1)
                data[band] = channel_data

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_longitude(self):
        """
        longitude
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Longitude').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/Longitude').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < -180, data_pre > 180)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_latitude(self):
        """
        longitude
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Latitude').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/Latitude').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < -180, data_pre > 180)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_land_sea_mask(self):
        """
        land_sea_mask
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/LandSeaMask').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/LandSeaMask').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < 0, data_pre > 7)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_land_cover(self):
        """
        land_cover
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/LandCover').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/LandCover').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < 0, data_pre > 17)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_sensor_azimuth(self):
        """
        sensor_azimuth
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/SensorAzimuth').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get(
                        '/Geolocation/SensorAzimuth').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            if self.satellite in satellite_type1:
                invalid_index = np.logical_or(
                    data_pre < -18000, data_pre > 18000)
            elif self.satellite in satellite_type2:
                invalid_index = np.logical_or(data_pre < 0, data_pre > 36000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_sensor_zenith(self):
        """
        sensor_zenith
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/SensorZenith').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get(
                        '/Geolocation/SensorZenith').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < 0, data_pre > 18000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_solar_azimuth(self):
        """
        solar_azimuth
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/SolarAzimuth').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get(
                        '/Geolocation/SolarAzimuth').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            if self.satellite in satellite_type1:
                invalid_index = np.logical_or(
                    data_pre < -18000, data_pre > 18000)
            elif self.satellite in satellite_type2:
                invalid_index = np.logical_or(data_pre < 0, data_pre > 36000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_solar_zenith(self):
        """
        solar_zenith
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/SolarZenith').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get(
                        '/Geolocation/SolarZenith').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < 0, data_pre > 18000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_timestamp(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                seconds_of_file = 300  # 一个时次持续 300 秒
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            file_date = datetime.strptime(self.ymd + self.hms, '%Y%m%d%H%M%S')
            timestamp = (
                file_date - datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
            row_length = self.data_shape[0]
            delta = np.linspace(0, seconds_of_file - 1, row_length)
            data = np.full(self.data_shape, np.nan, dtype=np.float64)
            data[:] = (delta + timestamp).reshape(-1, 1)
            data = data.astype(np.int32)
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_spectral_response(self):
        data1 = dict()
        data2 = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                dtype = {
                    'names': ('wave_length', 'response'), 'formats': ('f4', 'f4')}
                for i in xrange(self.channels):
                    k = i + 1
                    band = "CH_{:02d}".format(k)
                    file_name = '{}_{}_SRF_CH{:02d}_Pub.txt'.format(
                        self.satellite, self.sensor, k)
                    data_file = os.path.join(g_main_path, 'SRF', file_name)
                    if not os.path.isfile(data_file):
                        continue
                    datas = np.loadtxt(data_file, dtype=dtype)
                    wave_length = datas['wave_length'][::-1]
                    response = datas['response'][::-1]
                    channel_data = 10 ** 7 / wave_length
                    data1[band] = channel_data
                    data2[band] = response
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data1, data2

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

#     print 'dn:'
#     t_data = mersi.get_dn()
#     print_channel_data(t_data)
# #     print 'k0:'
# #     k0, k1, k2 = mersi.get_coefficient()
# #     print_channel_data(k0)
# #     print 'k1:'
# #     print_channel_data(k1)
# #     print 'k2:'
# #     print_channel_data(k2)
#     print 'ref:'
#     t_data = mersi.get_ref()
#     print_channel_data(t_data)
#
#     print 'rad:'
#     t_data = mersi.get_rad()
#     print_channel_data(t_data)
#
#     print 'tbb:'
#     t_data = mersi.get_tbb()
#     print_channel_data(t_data)

    print 'longitude:'
    t_data = mersi.get_sensor_zenith()
#     print t_data.keys()
    print t_data.shape
    print_data_status(t_data)


#     print mersi.file_attr  # L1 文件属性
