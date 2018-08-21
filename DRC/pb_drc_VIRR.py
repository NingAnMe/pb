#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/8/16 14:12
@Author  : AnNing
"""
import os
import re
from datetime import datetime

import h5py
import numpy as np

from PB.pb_time import get_ymd, get_hm
from PB.pb_io import attrs2dict
from PB.pb_sat import planck_r2t

from pb_drc_read import ReadL1

"""
读取处理 L1 数据，1000m 和 250m
处理原则
1 过滤原数据无效值和填充值，过滤后无效数据统一使用 NaN 填充
2 统一 shape
3 统一数据 dtype
4 统一通道相关和通道无关数据的存放格式
"""


class ReadVirrL1(ReadL1):
    """
    读取VIRR传感器的L1数据
    目前支持卫星： FY3A FY3B FY3C
    通道数量：10
    可见光通道：1 2 6 7 8 9 10
    红外通道：3 4 5
    """
    def __init__(self, in_file):
        sensor = 'VIRR'
        super(ReadVirrL1, self).__init__(in_file, sensor)

        # 固定值
        # 红外通道的中心波数，固定值，MERSI_Equiv Mid_wn (cm-1)
        self.wave_number = {'CH_03': 2673.796, 'CH_04': 925.925, 'CH_05': 833.333}

    def set_resolution(self):
        """
        根据L1文件名 set self.resolution 分辨率
        :return:
        """
        file_name = os.path.basename(self.in_file)
        if '1000M' in file_name:
            self.resolution = 1000
        # elif '0250M' in file_name:
        #     self.resolution = 250
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

    def set_file_geo_obc(self):
        """
        根据 L1 文件名 set self.file_geo 和 self.file_obc
        :return:
        """
        dir_path = os.path.dirname(self.in_file)
        file_name = os.path.basename(self.in_file)
        geo_file = os.path.join(dir_path, file_name[0:-12] + 'GEOXX_MS.HDF')
        obc_file = os.path.join(dir_path, file_name[0:-12] + 'OBCXX_MS.HDF')
        setattr(self, 'geo_file', geo_file)
        setattr(self, 'obc_file', obc_file)
        print 'Read L1 <<< {}'.format(self.in_file)
        print 'Read L1 <<< {}'.format(geo_file)
        print 'Read L1 <<< {}'.format(obc_file)

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
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError("Cant handle this resolution: ".format(self.resolution))

    def set_dataset_shape(self):
        """
        根据 self.satellite set self.dataset_shape
        :return:
        """
        # 如果分辨率是 1000 米
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                self.data_shape = (1800, 2048)
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
        # elif self.resolution == 250:
        else:
            raise ValueError("Cant handle this resolution: ".format(self.resolution))

    def set_channels(self):
        """
        根据 self.satellite set self.sensor_channel_amount
        :return:
        """
        if self.resolution == 1000:
            self.channels = 10
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
                # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
                s = self.data_shape
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    emissive = hdf5_file.get('/EV_Emissive')[:, :s[0], :s[1]]
                    ref_sb = hdf5_file.get('/EV_RefSB')[:, :s[0], :s[1]]
            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    emissive = hdf5_file.get('/Data/EV_Emissive').value
                    ref_sb = hdf5_file.get('/Data/EV_RefSB').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 1 2 6 7 8 9 10 为可见光通道，dn 值为 ref_sb
            # 3 4 5 为红外通道，dn 值为 emissive
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                if i < 2:
                    k = i
                    data_pre = ref_sb
                elif 2 <= i <= 4:
                    data_pre = emissive
                    k = i - 2
                else:
                    data_pre = ref_sb
                    k = i - 3
                # 将无效值使用NaN填充
                channel_data = np.full(self.data_shape, np.nan, dtype=np.float32)
                valid_index = np.logical_and(data_pre[k] < 32767, data_pre[k] > 0)
                channel_data[valid_index] = data_pre[k][valid_index]
                data[channel_name] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_k0(self):
        """
        vis: 可见光缩写
        ir: 红外缩写
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                s = self.data_shape
                k0_k1_vis = self.file_attr['RefSB_Cal_Coefficients']
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    k0_ir = hdf5_file.get('Emissive_Radiance_Scales')[0: s[0]]
                    # k1_ir = hdf5_file.get('Emissive_Radiance_Offsets')[0: s[0]]
            elif self.satellite in satellite_type2:
                k0_k1_vis = self.file_attr['RefSB_Cal_Coefficients']
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    k0_ir = hdf5_file.get('/Data/Emissive_Radiance_Scales').value
                    # k1_ir = hdf5_file.get('/Data/Emissive_Radiance_Offsets').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                if i < 2:
                    k = i * 2
                    k0 = k0_k1_vis[k]
                    # k1 = k0_k1_vis[k + 1]
                    channel_data = np.full(self.data_shape, k0, dtype=np.float32)
                    data[channel_name] = channel_data
                elif 2 <= i <= 4:
                    k = i - 2
                    data_pre = k0_ir[:, k].reshape(-1, 1)
                    # data_pre = k1_ir[:, k].reshape(-1, 1)
                    invalid_index = np.logical_and(data_pre < 0, data_pre > 50000)
                    data_pre[invalid_index] = np.nan
                    channel_data = np.full(self.data_shape, np.nan, dtype=np.float32)
                    channel_data[:] = data_pre
                    data[channel_name] = channel_data
                else:
                    k = (i - 3) * 2
                    k0 = k0_k1_vis[k]
                    # k1 = k0_k1_vis[k + 1]
                    channel_data = np.full(self.data_shape, k0, dtype=np.float32)
                    data[channel_name] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_k1(self):
        """
        vis: 可见光缩写
        ir: 红外缩写
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                s = self.data_shape
                k0_k1_vis = self.file_attr['RefSB_Cal_Coefficients']
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    # k0_ir = hdf5_file.get('Emissive_Radiance_Scales')[0: s[0]]
                    k1_ir = hdf5_file.get('Emissive_Radiance_Offsets')[0: s[0]]
            elif self.satellite in satellite_type2:
                k0_k1_vis = self.file_attr['RefSB_Cal_Coefficients']
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    # k0_ir = hdf5_file.get('/Data/Emissive_Radiance_Scales').value
                    k1_ir = hdf5_file.get('/Data/Emissive_Radiance_Offsets').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                if i < 2:
                    k = i * 2
                    # k0 = k0_k1_vis[k]
                    k1 = k0_k1_vis[k + 1]
                    channel_data = np.full(self.data_shape, k1, dtype=np.float32)
                    data[channel_name] = channel_data
                elif 2 <= i <= 4:
                    k = i - 2
                    # data_pre = k0_ir[:, k].reshape(-1, 1)
                    data_pre = k1_ir[:, k].reshape(-1, 1)
                    invalid_index = np.logical_and(data_pre < 0, data_pre > 50000)
                    data_pre[invalid_index] = np.nan
                    channel_data = np.full(self.data_shape, np.nan, dtype=np.float32)
                    channel_data[:] = data_pre
                    data[channel_name] = channel_data
                else:
                    k = (i - 3) * 2
                    # k0 = k0_k1_vis[k]
                    k1 = k0_k1_vis[k + 1]
                    channel_data = np.full(self.data_shape, k1, dtype=np.float32)
                    data[channel_name] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_ref(self):
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            ref_channels = ['CH_{:02d}'.format(i) for i in [1, 2, 6, 7, 8, 9, 10]]
            if self.satellite in satellite_type1:
                dn = self.get_dn()
                k0 = self.get_k0()
                k1 = self.get_k1()
                for channel_name in dn:
                    if channel_name not in ref_channels:
                        continue
                    channel_data = dn[channel_name] * k0[channel_name] + k1[channel_name]
                    data[channel_name] = channel_data
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_rad_pre(self):
        """
        RAD预处理
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            ref_channels = ['CH_{:02d}'.format(i) for i in [3, 4, 5]]
            if self.satellite in satellite_type1:
                dn = self.get_dn()
                k0 = self.get_k0()
                k1 = self.get_k1()
                for channel_name in dn:
                    if channel_name not in ref_channels:
                        continue
                    channel_data = dn[channel_name] * k0[channel_name] + k1[channel_name]
                    data[channel_name] = channel_data
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_rad(self):
        """
        经非线性校正后的RAD
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                rad_pres = self.get_rad_pre()
                b0_b1_b2_nonlinear = self.file_attr.get('Prelaunch_Nonlinear_Coefficients')

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
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
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
            # 红外转tbb的修正系数，所有时次都是固定值
            rad2tbb_k0 = {'CH_03': 1, 'CH_04': 1, 'CH_05': 1}
            rad2tbb_k1 = {'CH_03': 0, 'CH_04': 0, 'CH_05': 0}
            if self.satellite in satellite_type1:
                rads = self.get_rad()
                for channel_name in rads:
                    rad = rads[channel_name]
                    center_wave_number = self.wave_number[channel_name]
                    k0 = rad2tbb_k0[channel_name]
                    k1 = rad2tbb_k1[channel_name]
                    tbb = planck_r2t(rad, center_wave_number, k0, k1)
                    data[channel_name] = tbb
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb_coeff(self):
        """
        校正后的TBB
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                tbbs = self.get_tbb()
                if 'Emmisive_BT_Coefficients' in self.file_attr:
                    coeffs_name = 'Emmisive_BT_Coefficients'
                else:
                    coeffs_name = 'Emissive_BT_Coefficients'
                coeffs = self.file_attr.get(coeffs_name)
                for i in xrange(3):
                    channel_name = 'CH_{:02d}'.format(i + 3)
                    tbb = tbbs[channel_name]
                    k0 = coeffs[i * 2 + 1]
                    k1 = coeffs[i * 2]
                    tbb_coeff = tbb * k0 + k1
                    data[channel_name] = tbb_coeff
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sv(self):
        data = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            obc_file = getattr(self, 'obc_file')
            if not os.path.isfile(obc_file):
                raise ValueError('OBC file is not exist. {}'.format(obc_file))
            if self.satellite in satellite_type1:
                with h5py.File(obc_file, 'r') as hdf5_file:
                    # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
                    s = self.data_shape
                    data_pre = hdf5_file.get('Space_View')[:, :s[0], :s[1]]
            elif self.satellite in satellite_type2:
                with h5py.File(obc_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Calibration/Space_View').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < 1, data_pre > 1023)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan

            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                channel_data = np.full(self.data_shape, np.nan, dtype=np.float32)
                channel_data[:] = data_pre[i, :, 0].reshape(-1, 1)
                data[channel_name] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_bb(self):
        data = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            obc_file = getattr(self, 'obc_file')
            if not os.path.isfile(obc_file):
                raise ValueError('OBC file is not exist. {}'.format(obc_file))
            if self.satellite in satellite_type1:
                with h5py.File(obc_file, 'r') as hdf5_file:
                    # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
                    s = self.data_shape
                    data_pre = hdf5_file.get('Blackbody_View')[:, :s[0], :s[1]]
            elif self.satellite in satellite_type2:
                with h5py.File(obc_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Calibration/Blackbody_View').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < 1, data_pre > 1023)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan

            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                channel_data = np.full(self.data_shape, np.nan, dtype=np.float32)
                channel_data[:] = data_pre[i, :, 0].reshape(-1, 1)
                data[channel_name] = channel_data
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_height(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('Height')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/DEM').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_latitude(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('Latitude')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/Latitude').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_longitude(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('Longitude')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/Longitude').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_land_sea_mask(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('LandSeaMask')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/LandSeaMask').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_land_cover(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('LandCover')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/LandCover').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sensor_azimuth(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('SensorAzimuth')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/SensorAzimuth').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sensor_zenith(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('SensorZenith')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/SensorZenith').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sun_azimuth(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('SolarAzimuth')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/SolarAzimuth').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sun_zenith(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            geo_file = getattr(self, 'geo_file')

            # 因为FY3A数据不规整，存在 1810,2048 的数据，所以，取数据的时候，取 1800,2048
            s = self.data_shape
            if self.satellite in satellite_type1:
                if not os.path.isfile(self.in_file):
                    raise ValueError('L1 file is not exist. {}'.format(self.in_file))
                with h5py.File(self.in_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('SolarZenith')[:s[0], :s[1]]
            elif self.satellite in satellite_type2:
                if not os.path.isfile(geo_file):
                    raise ValueError('L1 file is not exist. {}'.format(geo_file))
                with h5py.File(geo_file, 'r') as hdf5_file:
                    data_pre = hdf5_file.get('/Geolocation/SolarZenith').value
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_and(data_pre < -1000, data_pre > 10000)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_timestamp(self):
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                seconds_of_file = 300  # 一个时次持续 300 秒
            else:
                raise ValueError('Cant read this satellite`s data.: {}'.format(self.satellite))
            file_date = datetime.strptime(self.ymd + self.hms, '%Y%m%d%H%M%S')
            timestamp = (file_date - datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
            row_length = self.data_shape[0]
            delta = np.linspace(0, seconds_of_file - 1, row_length)
            data = np.full(self.data_shape, np.nan, dtype=np.float64)
            data[:] = (delta + timestamp).reshape(-1, 1)
            data = data.astype(np.int32)
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data


if __name__ == '__main__':
    t_in_file = r'D:\nsmc\L1\FY3A\VIRR\FY3A_VIRRX_GBAL_L1_20150101_0000_1000M_MS.HDF'
    t_read_l1 = ReadVirrL1(t_in_file)
    print 'attribute', '-' * 50
    print t_read_l1.satellite  # 卫星名
    print t_read_l1.sensor  # 传感器名
    print t_read_l1.ymd  # L1 文件年月日 YYYYMMDD
    print t_read_l1.hms  # L1 文件时分秒 HHMMSS
    print t_read_l1.resolution  # 分辨率
    print t_read_l1.channels  # 通道数量
    print t_read_l1.data_shape
    print t_read_l1.file_attr  # L1 文件属性

    print 'Channel', '-' * 50

    def print_channel_data(datas):
        if not isinstance(datas, dict):
            return
        keys = list(datas.viewkeys())
        keys.sort()
        for t_channel_name in keys:
            channel_data = datas[t_channel_name]
            data_shape = channel_data.shape
            data_min = np.nanmin(channel_data)
            data_max = np.nanmax(channel_data)
            print t_channel_name, data_shape, data_min, data_max

    print 'dn:'
    t_data = t_read_l1.get_dn()
    print_channel_data(t_data)

    print 'k0:'
    t_data = t_read_l1.get_k0()
    print_channel_data(t_data)

    print 'k1:'
    t_data = t_read_l1.get_k1()
    print_channel_data(t_data)

    print 'ref:'
    t_data = t_read_l1.get_ref()
    print_channel_data(t_data)

    print 'rad_pre:'
    t_data = t_read_l1.get_ref()
    print_channel_data(t_data)

    print 'rad'
    t_data = t_read_l1.get_rad()
    print_channel_data(t_data)

    print 'tbb:'
    t_data = t_read_l1.get_tbb()
    print_channel_data(t_data)

    print 'tbb_coeff:'
    t_data = t_read_l1.get_tbb_coeff()
    print_channel_data(t_data)

    print 'sv:'
    t_data = t_read_l1.get_sv()
    print_channel_data(t_data)

    print 'bb:'
    t_data = t_read_l1.get_bb()
    print_channel_data(t_data)

    print 'No channel', '-' * 50

    def print_data_status(datas):
        data_shape = datas.shape
        data_min = np.nanmin(datas)
        data_max = np.nanmax(datas)
        print data_shape, data_min, data_max

    t_data = t_read_l1.get_height()
    print 'height'
    print_data_status(t_data)

    t_data = t_read_l1.get_latitude()
    print 'latitude:'
    print_data_status(t_data)

    t_data = t_read_l1.get_longitude()
    print 'longitude:'
    print_data_status(t_data)

    t_data = t_read_l1.get_land_cover()
    print 'land_cover:'
    print_data_status(t_data)

    t_data = t_read_l1.get_land_sea_mask()
    print 'land_sea_mask:'
    print_data_status(t_data)

    t_data = t_read_l1.get_sensor_azimuth()
    print 'sensor_azimuth:'
    print_data_status(t_data)

    t_data = t_read_l1.get_sensor_zenith()
    print 'sensor_zenith:'
    print_data_status(t_data)

    t_data = t_read_l1.get_sun_azimuth()
    print 'sun_azimuth:'
    print_data_status(t_data)

    t_data = t_read_l1.get_sun_zenith()
    print 'sun_zenith:'
    print_data_status(t_data)

    t_data = t_read_l1.get_timestamp()
    print 'Timestamp'
    print_data_status(t_data)
    datetime_timestamp = datetime.utcfromtimestamp(t_data[0][0])
    datetime_file = datetime.strptime(t_read_l1.ymd + t_read_l1.hms, '%Y%m%d%H%M%S')
    if datetime_timestamp != datetime_file:
        print 'Error', '-' * 100
        print t_data[0][0], datetime_timestamp
        print t_read_l1.ymd + t_read_l1.hms, datetime_file
        raise ValueError('Please check the get_timestamp')
