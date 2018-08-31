# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re

import h5py

from PB.pb_io import attrs2dict
from PB.pb_sat import planck_r2t
from pb_drc_base import ReadL1
import numpy as np


__description__ = u'MERSI传感器读取类'
__author__ = 'wangpeng'
__date__ = '2018-08-28'
__version__ = '1.0.0_beat'


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class ReadMersiL1(ReadL1):

    """
    读取 MERSI 传感器的 L1 数据
    分辨率：1000m
    卫星： [FY3A FY3B FY3C]
    通道数量：20
    可见光通道：1,2,3,4,6~20
    红外通道：5

    卫星： [FY3D]
    通道数量：25
    可见光通道：1~20
    红外通道：20~25

    分辨率：250
    卫星：
    通道数量：
    可见光通道：
    红外通道：
    """

    def __init__(self, in_file):
        sensor = 'MERSI'
        super(ReadMersiL1, self).__init__(in_file, sensor)

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
        use filename  set self.ymd self.hms
        """
        file_name = os.path.basename(self.in_file)
        pat = u'\w{4}_\w{5}_\w{4}_L1_(\d{8})_(\d{4})_\w{5}_MS.HDF$'
        g = re.match(pat, file_name)
        if g:
            self.ymd = g.group(1)
            self.hms = g.group(2) + '00'
        else:
            raise ValueError('Cant get the ymdhms from file name.')

    def set_file_attr(self):
        """
        根据 self.file_level_1 获取 L1 文件的属性
        set self.level_1_attr
        储存格式是字典
        :return:
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                with h5py.File(self.in_file, 'r') as h5r:
                    self.file_attr = attrs2dict(h5r.attrs)
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))

    def set_data_shape(self):
        """
        根据 self.satellite set self.data_shape
        :return:
        """
        # 如果分辨率是 1000 米
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C', 'FY3D']
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
        return sensor channels
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            satellite_type2 = ['FY3D']
            if self.satellite in satellite_type1:
                self.channels = 20
            elif self.satellite in satellite_type2:
                self.channels = 25
        # elif self.resolution == 250:
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def get_central_wave_number(self):
        '''
        return 中心波数
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
        return 定位文件
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3C', 'FY3D']
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
        return DN
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            satellite_type3 = ['FY3D']

            if self.satellite in satellite_type1:
                data_file = self.in_file
                with h5py.File(data_file, 'r') as h5r:
                    ary_ch1 = h5r.get('/EV_250_Aggr.1KM_RefSB').value
                    ary_ch5 = h5r.get('/EV_250_Aggr.1KM_Emissive').value
                    ary_ch6 = h5r.get('/EV_1KM_RefSB').value
                    vmin = 0
                    vmax = 10000

                    # 逐个通道处理
                    for i in xrange(self.channels):
                        band = 'CH_{:02d}'.format(i + 1)

                        if i < 4:
                            k = i
                            data_pre = ary_ch1[k]
                            # 开始处理
                        elif i == 4:
                            data_pre = ary_ch5
                        else:
                            k = i - 5
                            data_pre = ary_ch6[k]

                    data_pre = data_pre.astype(np.float32)
                    invalid_index = np.logical_or(
                        data_pre <= vmin, data_pre > vmax)
                    data_pre[invalid_index] = np.nan
                    data[band] = data_pre

            elif self.satellite in satellite_type2:
                data_file = self.in_file
                with h5py.File(data_file, 'r') as h5r:
                    ary_ch1 = h5r.get('/Data/EV_250_Aggr.1KM_RefSB').value
                    ary_ch5 = h5r.get('/Data/EV_250_Aggr.1KM_Emissive').value
                    ary_ch6 = h5r.get('/Data/EV_1KM_RefSB').value
                    vmin = 0
                    vmax = 10000

                    # 逐个通道处理
                    for i in xrange(self.channels):
                        band = 'CH_{:02d}'.format(i + 1)

                        if i < 4:
                            k = i
                            data_pre = ary_ch1[k]
                            # 开始处理
                        elif i == 4:
                            data_pre = ary_ch5
                        else:
                            k = i - 5
                            data_pre = ary_ch6[k]

                    data_pre = data_pre.astype(np.float32)
                    invalid_index = np.logical_or(
                        data_pre <= vmin, data_pre > vmax)
                    data_pre[invalid_index] = np.nan
                    data[band] = data_pre

            elif self.satellite in satellite_type3:
                data_file = self.in_file
                with h5py.File(data_file, 'r') as h5r:
                    ary_ch1_4 = h5r.get('/Data/EV_250_Aggr.1KM_RefSB')[:]
                    ary_ch5_19 = h5r.get('/Data/EV_1KM_RefSB')[:]
                    ary_ch20_23 = h5r.get('/Data/EV_1KM_Emissive')[:]
                    ary_ch24_25 = h5r.get('/Data/EV_250_Aggr.1KM_Emissive')[:]
                    vmin = 0
                    vmax = 4095
                    # 逐个通道处理
                    for i in xrange(self.channels):
                        band = 'CH_{:02d}'.format(i + 1)
                        if i < 4:
                            k = i
                            data_pre = ary_ch1_4[k]
                            # 开始处理
                        elif i >= 4 and i < 19:
                            k = i - 4
                            data_pre = ary_ch5_19[k]
                        elif i >= 19 and i < 23:
                            k = i - 19
                            data_pre = ary_ch20_23[k]
                        else:
                            k = i - 23
                            data_pre = ary_ch24_25[k]

                        data_pre = data_pre.astype(np.float32)
                        invalid_index = np.logical_or(
                            data_pre <= vmin, data_pre > vmax)
                        data_pre[invalid_index] = np.nan
                        data[band] = data_pre
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_k0(self):
        """
        return K0
        """
        data = dict()

        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            satellite_type3 = ['FY3D']

            # FY3AB
            if self.satellite in satellite_type1:
                # vis_k, 54 = 19*3  （19通道 3个系数）
                ary_vis_coeff = self.file_attr['VIR_Cal_Coeff']
                K = np.full((19, 3), 0.)
                for i in range(19):
                    for j in range(3):
                        K[i, j] = ary_vis_coeff[i * 3 + j]
                # 变成20*3  k0,k1,k2
                values = np.array([0, 1, 0])
                K = np.insert(K, 4, values, 0)
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    # k0
                    channel_data = np.full(
                        self.data_shape, K[i, 0], dtype=np.float32)
                    data[band] = channel_data
            # FY3C
            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as h5r:
                    ary_vis_coeff = h5r.get('/Calibration/VIS_Cal_Coeff').value

                # 19*3 变成20*3 红外通道给定值不影响原dn值
                values = np.array([0, 1, 0])
                K = np.insert(ary_vis_coeff, 4, values, 0)
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    # k0
                    channel_data = np.full(
                        self.data_shape, K[i, 0], dtype=np.float32)
                    data[band] = channel_data

            # FY3D
            elif self.satellite in satellite_type3:
                with h5py.File(self.in_file, 'r') as h5r:

                    ary_ir_coeff = h5r.get('/Calibration/IR_Cal_Coeff').value
                    ary_vis_coeff = h5r.get('/Calibration/VIS_Cal_Coeff').value

                    # 转维度
                    s = self.data_shape
                    ary_vis_coeff1 = np.repeat(
                        ary_vis_coeff[:, 0], s[0] * s[1])
                    ary_ir_coeff1 = np.repeat(
                        ary_ir_coeff[:, 0, :], 10 * s[1], axis=1)

                    # 转维度 19*2000*2048，6*2000*2048
                    ary_vis_coeff2 = ary_vis_coeff1.reshape(
                        (-1,) + self.data_shape)
                    ary_ir_coeff2 = ary_ir_coeff1.reshape(
                        (-1,) + self.data_shape)

                    # 逐个通道处理
                    s = self.data_shape
                    for i in xrange(self.channels):
                        band = 'CH_{:02d}'.format(i + 1)

                        if i < 19:
                            k = i
                            data[band] = ary_vis_coeff2[k]
                        else:
                            k = i - 19
                            data[band] = ary_ir_coeff2[k]

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_k1(self):
        """
        return K1
        """
        data = dict()

        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            satellite_type3 = ['FY3D']

            # FY3AB
            if self.satellite in satellite_type1:
                # vis_k, 54 = 19*3  （19通道 3个系数）
                ary_vis_coeff = self.file_attr['VIR_Cal_Coeff']
                K = np.full((19, 3), 0.)
                for i in range(19):
                    for j in range(3):
                        K[i, j] = ary_vis_coeff[i * 3 + j]
                # 变成20*3  k0,k1,k2
                values = np.array([0, 1, 0])
                K = np.insert(K, 4, values, 0)
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    # k0
                    channel_data = np.full(
                        self.data_shape, K[i, 1], dtype=np.float32)
                    data[band] = channel_data
            # FY3C
            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as h5r:
                    ary_vis_coeff = h5r.get('/Calibration/VIS_Cal_Coeff').value

                # 19*3 变成20*3 红外通道给定值不影响原dn值
                values = np.array([0, 1, 0])
                K = np.insert(ary_vis_coeff, 4, values, 0)
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    # k0
                    channel_data = np.full(
                        self.data_shape, K[i, 1], dtype=np.float32)
                    data[band] = channel_data

            # FY3D
            elif self.satellite in satellite_type3:
                with h5py.File(self.in_file, 'r') as h5r:

                    ary_ir_coeff = h5r.get('/Calibration/IR_Cal_Coeff').value
                    ary_vis_coeff = h5r.get('/Calibration/VIS_Cal_Coeff').value

                    # 转维度
                    s = self.data_shape
                    ary_vis_coeff1 = np.repeat(
                        ary_vis_coeff[:, 1], s[0] * s[1])
                    ary_ir_coeff1 = np.repeat(
                        ary_ir_coeff[:, 1, :], 10 * s[1], axis=1)

                    # 转维度 19*2000*2048，6*2000*2048
                    ary_vis_coeff2 = ary_vis_coeff1.reshape(
                        (-1,) + self.data_shape)
                    ary_ir_coeff2 = ary_ir_coeff1.reshape(
                        (-1,) + self.data_shape)

                    # 逐个通道处理
                    s = self.data_shape
                    for i in xrange(self.channels):
                        band = 'CH_{:02d}'.format(i + 1)

                        if i < 19:
                            k = i
                            data[band] = ary_vis_coeff2[k]
                        else:
                            k = i - 19
                            data[band] = ary_ir_coeff2[k]

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_k2(self):
        """
        return K2
        """
        data = dict()

        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            satellite_type3 = ['FY3D']

            # FY3AB
            if self.satellite in satellite_type1:
                # vis_k, 54 = 19*3  （19通道 3个系数）
                ary_vis_coeff = self.file_attr['VIR_Cal_Coeff']
                K = np.full((19, 3), 0.)
                for i in range(19):
                    for j in range(3):
                        K[i, j] = ary_vis_coeff[i * 3 + j]
                # 变成20*3  k0,k1,k2
                values = np.array([0, 1, 0])
                K = np.insert(K, 4, values, 0)
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    # k0
                    channel_data = np.full(
                        self.data_shape, K[i, 2], dtype=np.float32)
                    data[band] = channel_data
            # FY3C
            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as h5r:
                    ary_vis_coeff = h5r.get('/Calibration/VIS_Cal_Coeff').value

                # 19*3 变成20*3 红外通道给定值不影响原dn值
                values = np.array([0, 1, 0])
                K = np.insert(ary_vis_coeff, 4, values, 0)
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    # k0
                    channel_data = np.full(
                        self.data_shape, K[i, 2], dtype=np.float32)
                    data[band] = channel_data

            # FY3D
            elif self.satellite in satellite_type3:
                with h5py.File(self.in_file, 'r') as h5r:

                    ary_ir_coeff = h5r.get('/Calibration/IR_Cal_Coeff').value
                    ary_vis_coeff = h5r.get('/Calibration/VIS_Cal_Coeff').value

                    # 转维度
                    s = self.data_shape
                    ary_vis_coeff1 = np.repeat(
                        ary_vis_coeff[:, 2], s[0] * s[1])
                    ary_ir_coeff1 = np.repeat(
                        ary_ir_coeff[:, 2, :], 10 * s[1], axis=1)

                    # 转维度 19*2000*2048，6*2000*2048
                    ary_vis_coeff2 = ary_vis_coeff1.reshape(
                        (-1,) + self.data_shape)
                    ary_ir_coeff2 = ary_ir_coeff1.reshape(
                        (-1,) + self.data_shape)

                    # 逐个通道处理
                    s = self.data_shape
                    for i in xrange(self.channels):
                        band = 'CH_{:02d}'.format(i + 1)

                        if i < 19:
                            k = i
                            data[band] = ary_vis_coeff2[k]
                        else:
                            k = i - 19
                            data[band] = ary_ir_coeff2[k]

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_k3(self):
        pass

    def get_ref(self):
        """
        return Ref
        """

        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            satellite_type2 = ['FY3D']

            # FY3A/B/C
            if self.satellite in satellite_type1:
                dn = self.get_dn()
                k0 = self.get_k0()
                k1 = self.get_k1()
                k2 = self.get_k2()

                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if 'CH_05' in band:
                        continue

                    channel_data = dn[band]**2 * k2[band] + dn[band] * \
                        k1[band] + k0[band]
                    data[band] = channel_data / 100.
            # FY3D
            elif self.satellite in satellite_type2:
                dn = self.get_dn()
                k0 = self.get_k0()
                k1 = self.get_k1()
                k2 = self.get_k2()

                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if i < 19:

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
        return rad
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            satellite_type2 = ['FY3D']
            if self.satellite in satellite_type1:
                dn = self.get_dn()
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if 'CH_05' in band:
                        data[band] = dn[band] / 100.
            elif self.satellite in satellite_type2:
                dn = self.get_dn()
                with h5py.File(self.in_file, 'r') as h5r:
                    ary_a1 = h5r.get('/Data/EV_1KM_Emissive').attrs['Slope']
                    ary_b1 = h5r.get(
                        '/Data/EV_1KM_Emissive').attrs['Intercept']

                    ary_a2 = h5r.get(
                        '/Data/EV_250_Aggr.1KM_Emissive').attrs['Slope']
                    ary_b2 = h5r.get(
                        '/Data/EV_250_Aggr.1KM_Emissive').attrs['Intercept']

                    a = np.concatenate((ary_a1, ary_a2))
                    b = np.concatenate((ary_b1, ary_b2))

                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)

                    if i >= 19:
                        k = i - 19
                        data[band] = dn[band] * a[k] + b[k]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb_k0(self):
        """
        return tbb_k0  dict one value
        """

        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            satellite_type2 = ['FY3D']

            if self.satellite in satellite_type1:
                data['CH_05'] = 1
            elif self.satellite in satellite_type2:
                data['CH_20'] = 1.00103
                data['CH_21'] = 1.00085
                data['CH_22'] = 1.00125
                data['CH_23'] = 1.00030
                data['CH_24'] = 1.00133
                data['CH_25'] = 1.00065

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb_k1(self):
        """
        return tbb_k1  dict one value
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            satellite_type2 = ['FY3D']

            if self.satellite in satellite_type1:
                data['CH_05'] = 0
            elif self.satellite in satellite_type2:
                data['CH_20'] = -0.4759
                data['CH_21'] = -0.3139
                data['CH_22'] = -0.2662
                data['CH_23'] = -0.0513
                data['CH_24'] = -0.0734
                data['CH_25'] = 0.0875
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb(self):
        """
        return tbb
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C', 'FY3D']

            if self.satellite in satellite_type1:
                # rad转tbb的修正系数，所有时次都是固定值
                tbb_k0 = self.get_tbb_k0()
                tbb_k1 = self.get_tbb_k1()
                rads = self.get_rad()
                central_wave_numbers = self.get_central_wave_number()
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    if band in rads.keys():
                        k0 = tbb_k0[band]
                        k1 = tbb_k1[band]
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
        return sv
        """
        data = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/SV_DN_average').value

                # 过滤无效值
                invalid_index = np.logical_or(data_pre <= 0, data_pre > 4095)
                data_pre = data_pre.astype(np.float32)
                data_pre[invalid_index] = np.nan
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    channel_data = np.full(
                        self.data_shape, np.nan, dtype=np.float32)
                    channel_data[:] = data_pre[i, :].reshape(-1, 1)
                    data[band] = channel_data

            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/Calibration/SV_DN_average').value

                # 过滤无效值
                invalid_index = np.logical_or(data_pre <= 0, data_pre > 4095)
                data_pre = data_pre.astype(np.float32)
                data_pre[invalid_index] = np.nan
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    channel_data = np.full(
                        self.data_shape, np.nan, dtype=np.float32)
                    # 把200 转成2000
                    data_pre_new = np.repeat(data_pre[i, :], 10)
                    channel_data[:] = data_pre_new.reshape(-1, 1)
                    data[band] = channel_data

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_bb(self):
        """
        return bb
        """
        data = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/BB_DN_average').value

                # 过滤无效值
                invalid_index = np.logical_or(data_pre <= 0, data_pre > 4095)
                data_pre = data_pre.astype(np.float32)
                data_pre[invalid_index] = np.nan
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    channel_data = np.full(
                        self.data_shape, np.nan, dtype=np.float32)
                    channel_data[:] = data_pre[i, :].reshape(-1, 1)
                    data[band] = channel_data

            elif self.satellite in satellite_type2:
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/Calibration/BB_DN_average').value

                # 过滤无效值
                invalid_index = np.logical_or(data_pre <= 0, data_pre > 4095)
                data_pre = data_pre.astype(np.float32)
                data_pre[invalid_index] = np.nan
                # 逐个通道处理
                for i in xrange(self.channels):
                    band = 'CH_{:02d}'.format(i + 1)
                    channel_data = np.full(
                        self.data_shape, np.nan, dtype=np.float32)
                    # 把200 转成2000
                    data_pre_new = np.repeat(data_pre[i, :], 10)
                    channel_data[:] = data_pre_new.reshape(-1, 1)
                    data[band] = channel_data

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_longitude(self):
        """
        return longitude
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/Longitude').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/Longitude').value

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
        return latitude
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/Latitude').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/Latitude').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < -90, data_pre > 90)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_land_sea_mask(self):
        """
        return land_sea_mask
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/LandSeaMask').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/LandSeaMask').value

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
        return land_cover
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/LandCover').value
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/LandCover').value

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
        return sensor_azimuth
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/SensorAzimuth').value
                vmin = -18000
                vmax = 18000
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/Geolocation/SensorAzimuth').value

                if 'FY3D' in self.satellite:
                    vmin = 0
                    vmax = 36000
                else:
                    vmin = -18000
                    vmax = 18000

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_sensor_zenith(self):
        """
        return sensor_zenith
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/SensorZenith').value

                vmin = 0
                vmax = 18000
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/SensorZenith').value
                vmin = 0
                vmax = 18000

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_solar_azimuth(self):
        """
        return solar_azimuth
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/SolarAzimuth').value

                vmin = -18000
                vmax = 18000

            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/SolarAzimuth').value

                if 'FY3D' in self.satellite:
                    vmin = 0
                    vmax = 36000
                else:
                    vmin = -18000
                    vmax = 18000

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_solar_zenith(self):
        """
        return solar_zenith
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get('/SolarZenith').value

                vmin = 0
                vmax = 18000
            elif self.satellite in satellite_type2:
                geo_file = self.__get_geo_file()
                with h5py.File(geo_file, 'r') as h5r:
                    data_pre = h5r.get('/Geolocation/SolarZenith').value

                vmin = 0
                vmax = 18000

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre / 100.

        return data

    def get_timestamp(self):
        """
        return from 1970-01-01 00:00:00 seconds
        """
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C', 'FY3D']
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
        """
        return 光谱波数和响应值，两个字典
        """
        data1 = dict()
        data2 = dict()
        if self.resolution == 1000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C', 'FY3D']
            if self.satellite in satellite_type1:
                dtype = {
                    'names': ('wave_length', 'response'), 'formats': ('f4', 'f4')}
                for i in xrange(self.channels):
                    k = i + 1
                    band = "CH_{:02d}".format(k)
                    file_name = '{}_{}_SRF_CH{:02d}_Pub.txt'.format(
                        self.satellite, self.sensor, k)
                    data_file = os.path.join(MainPath, 'SRF', file_name)
                    if not os.path.isfile(data_file):
                        continue
                    datas = np.loadtxt(data_file, dtype=dtype)
                    # 波长转波数
                    wave_length = datas['wave_length'][::-1]
                    wave_number = 10 ** 7 / wave_length
                    # 响应
                    response = datas['response'][::-1]

                    data1[band] = wave_number
                    data2[band] = response
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data1, data2

if __name__ == '__main__':
    #     L1File = 'D:/data/FY3C_MERSI/FY3C_MERSI_GBAL_L1_20150223_2340_1000M_MS.HDF'
    L1File = 'D:/data/FY3D+MERSI_HIRAS/FY3D_MERSI_GBAL_L1_20180326_0045_1000M_MS.HDF'
    mersi = ReadMersiL1(L1File)
    print mersi.satellite  # 卫星名
    print mersi.sensor  # 传感器名
    print mersi.ymd  # L1 文件年月日 YYYYMMDD
    print mersi.hms  # L1 文件时分秒 HHMMSS
    print mersi.resolution  # 分辨率
    print mersi.channels  # 通道数量
    print mersi.data_shape
#     print mersi.file_attr

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
#     print 'k0:'
#     t_data = mersi.get_k0()
#     print_channel_data(t_data)
#     print 'k1:'
#     t_data = mersi.get_k1()
#     print_channel_data(t_data)
#     print 'k2:'
#     t_data = mersi.get_k2()
#     print_channel_data(t_data)
#     print 'ref:'
#     t_data = mersi.get_ref()
#     print_channel_data(t_data)

#     print 'rad:'
#     t_data = mersi.get_rad()
#     print_channel_data(t_data)

    print 'tbb:'
    t_data = mersi.get_tbb()
    print_channel_data(t_data)

#     print 'sv:'
#     t_data = mersi.get_sv()
#     print_channel_data(t_data)
#
#     print 'bb:'
#     t_data = mersi.get_bb()
#     print_channel_data(t_data)

#     print 'longitude:'
#     t_data = mersi.get_longitude()
#     print_data_status(t_data)
#
#     print 'latitude:'
#     t_data = mersi.get_latitude()
#     print_data_status(t_data)

#     print 'land_sea_mask:'
#     t_data = mersi.get_land_sea_mask()
#     print_data_status(t_data)
#
#     print 'land_cover:'
#     t_data = mersi.get_land_cover()
#     print_data_status(t_data)

    print 'sensor_azimuth:'
    t_data = mersi.get_sensor_azimuth()
    print_data_status(t_data)
    print 'sensor_zenith:'
    t_data = mersi.get_sensor_zenith()
    print_data_status(t_data)
    print 'solar_azimuth:'
    t_data = mersi.get_solar_azimuth()
    print_data_status(t_data)
    print 'solar_zenith:'
    t_data = mersi.get_solar_zenith()
    print_data_status(t_data)
    print 'timestamp:'
    t_data = mersi.get_timestamp()
    print_data_status(t_data)

#     print 'get_spectral_response:'
#     wavenums, wave_spec = mersi.get_spectral_response()
#     print_channel_data(wavenums)
#     print_channel_data(wave_spec)
