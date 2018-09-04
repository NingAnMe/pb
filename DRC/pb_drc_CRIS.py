# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re
import time
import h5py

from PB.pb_io import attrs2dict
from PB.pb_sat import planck_r2t, spec_interp, spec_convolution
from pb_drc_base import ReadL1
import numpy as np


# from PB.pb_io import attrs2dict
__description__ = u'CRIS传感器读取'
__author__ = 'wangpeng'
__date__ = '2018-09-03'
__version__ = '1.0.0_beat'

MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class ReadCrisL1(ReadL1):
    """
    读取 IASI 传感器的 L1 数据
    分辨率：16KM | 3 x 3 14 km IFOV covering a 48 x 48 km2 cell (average sampling distance: 16 km)
    卫星： [NPP]
    通道数量：
    红外通道：2211 or 3369(this gapfilling)
    """

    def __init__(self, in_file):
        sensor = 'CRIS'
        super(ReadCrisL1, self).__init__(in_file, sensor)

    def set_resolution(self):
        """
        set satellite self.resolution
        """
        file_name = os.path.basename(self.in_file)
        if 'CRIS' in file_name:
            self.resolution = 16000
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def set_satellite(self):
        """
        set satellite name self.satellite
        :return:
        """
        self.satellite = 'NPP'

    def set_ymd_hms(self):
        """
        use filename  set self.ymd self.hms
        """
        file_name = os.path.basename(self.in_file)
        pat = u'\w{5}-.*_npp_d(\d{8})_t(\d{7})_e(\d{7})_b(\d{5})_c(\d{20})_\w{4}_ops.h5$'
        g = re.match(pat, file_name)
        if g:
            self.ymd = g.group(1)
            self.hms = g.group(2)[:-1]  # 舍弃秒的小数点以后位
        else:
            raise ValueError('Cant get the ymdhms from file name.')

    def set_file_attr(self):
        """
        get hdf5 file attrs self.file_attr
        :return:
        """
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
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
        use file dataset dims set self.data_shape
        :return:
        """
        # 如果分辨率是 1000 米
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                self.data_shape = (16200, 1)
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
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                self.channels = 3369
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def get_spectral_response(self):
        """
        return 光谱波数和响应值，1维，2维
        """

        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:

                s = self.data_shape
                # 增加切趾计算
                w0 = 0.23
                w1 = 0.54
                w2 = 0.23
                data_file = self.in_file
                with h5py.File(data_file, 'r') as h5r:
                    sds_name = '/All_Data/CrIS-FS-SDR_All/ES_RealLW'
                    real_lw = h5r.get(sds_name).value

                    sds_name = '/All_Data/CrIS-FS-SDR_All/ES_RealMW'
                    real_mw = h5r.get(sds_name).value

                    sds_name = '/All_Data/CrIS-FS-SDR_All/ES_RealSW'
                    real_sw = h5r.get(sds_name).value

                # 切趾计算 w0*n-1 + w1*n + w2*n+1 当作n位置的修正值
                # 开头和结尾不参与计算
                real_lw[:, :, :, 1:-1] = w0 * real_lw[:, :, :, :-2] + \
                    w1 * real_lw[:, :, :, 1:-1] + w2 * real_lw[:, :, :, 2:]
                real_mw[:, :, :, 1:-1] = w0 * real_mw[:, :, :, :-2] + \
                    w1 * real_mw[:, :, :, 1:-1] + w2 * real_mw[:, :, :, 2:]
                real_sw[:, :, :, 1:-1] = w0 * real_sw[:, :, :, :-2] + \
                    w1 * real_sw[:, :, :, 1:-1] + w2 * real_sw[:, :, :, 2:]

                real_lw = real_lw[:, :, :, 2:-2]
                real_mw = real_mw[:, :, :, 2:-2]
                real_sw = real_sw[:, :, :, 2:-2]

                # 波数范围和步长
                wave_lw = np.arange(650., 1095.0 + 0.625, 0.625)
                wave_mw = np.arange(1210.0, 1750 + 0.625, 0.625)
                wave_sw = np.arange(2155.0, 2550.0 + 0.625, 0.625)

                wave_number = np.arange(650., 2755.0 + 0.625, 0.625)

                wave_number_old = np.concatenate(
                    (wave_lw, wave_mw, wave_sw))
                response_old = np.concatenate(
                    (real_lw, real_mw, real_sw), axis=3)
                last_s = response_old.shape[-1]
                # 16200*最后一个光谱维度
                response_old = response_old.reshape(s[0], last_s)

                data_file = os.path.join(
                    MainPath, 'COEFF', 'cris_fs.GapCoeff.h5')

                if not os.path.isfile(data_file):
                    raise ValueError(
                        'Data file is not exist. {}'.format(data_file))
                with h5py.File(data_file, 'r') as h5r:
                    c0 = h5r.get('C0')[:]
                    p0 = h5r.get('P0')[:]
                    gapNum = h5r.get('GAP_NUM')[:]

                response_new = np.dot(response_old, p0)
                response_new = response_new + c0
                ch_part1 = gapNum[0]
                ch_part2 = gapNum[0] + gapNum[1]
                ch_part3 = gapNum[0] + gapNum[1] + gapNum[2]
                real_lw_e = response_new[:, 0:ch_part1]
                real_mw_e = response_new[:, ch_part1:ch_part2]
                real_sw_e = response_new[:, ch_part2:ch_part3]

                # 把原响应值 维度转成2维
                real_lw = real_lw.reshape(s[0], real_lw.shape[-1])
                real_mw = real_mw.reshape(s[0], real_mw.shape[-1])
                real_sw = real_sw.reshape(s[0], real_sw.shape[-1])
                response = np.concatenate(
                    (real_lw, real_lw_e, real_mw, real_mw_e, real_sw, real_sw_e), axis=1)
                print wave_number_old.shape, response_old.shape

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return wave_number, response

    def get_spectral_response_low(self):
        """
        return 光谱波数和响应值，1维，2维, 处理低分辨率的cris
        """

        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:

                s = self.data_shape
                # 增加切趾计算
                w0 = 0.23
                w1 = 1 - 2 * w0
                w2 = w0
                data_file = self.in_file
                with h5py.File(data_file, 'r') as h5r:
                    sds_name = '/All_Data/CrIS-SDR_All/ES_RealLW'
                    real_lw = h5r.get(sds_name).value

                    sds_name = '/All_Data/CrIS-SDR_All/ES_RealMW'
                    real_mw = h5r.get(sds_name).value

                    sds_name = '/All_Data/CrIS-SDR_All/ES_RealSW'
                    real_sw = h5r.get(sds_name).value

                # 切趾计算 w0*n-1 + w1*n + w2*n+1 当作n位置的修正值
                # 开头和结尾不参与计算
                real_lw[:, :, :, 1:-1] = w0 * real_lw[:, :, :, :-2] + \
                    w1 * real_lw[:, :, :, 1:-1] + w2 * real_lw[:, :, :, 2:]
                real_mw[:, :, :, 1:-1] = w0 * real_mw[:, :, :, :-2] + \
                    w1 * real_mw[:, :, :, 1:-1] + w2 * real_mw[:, :, :, 2:]
                real_sw[:, :, :, 1:-1] = w0 * real_sw[:, :, :, :-2] + \
                    w1 * real_sw[:, :, :, 1:-1] + w2 * real_sw[:, :, :, 2:]

                real_lw = real_lw[:, :, :, 2:-2]
                real_mw = real_mw[:, :, :, 2:-2]
                real_sw = real_sw[:, :, :, 2:-2]

                # 波数范围和步长
                wave_lw = np.arange(650., 1095.0 + 0.625, 0.625)
                wave_mw = np.arange(1210.0, 1750 + 1.25, 1.25)
                wave_sw = np.arange(2155.0, 2550.0 + 2.5, 2.5)

                wave_number = np.concatenate((wave_lw, wave_mw, wave_sw))
                response = np.concatenate((real_lw, real_mw, real_sw), axis=3)
                last_shape = self.radiance.shape[-1]
                response = response.reshape(s[0], last_shape)

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return wave_number, response

    def get_longitude(self):
        """
        return longitude
        """
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/All_Data/CrIS-SDR-GEO_All/Longitude').value
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
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/All_Data/CrIS-SDR-GEO_All/Latitude').value
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < -90, data_pre > 90)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_sensor_azimuth(self):
        """
        return sensor_azimuth
        """
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/All_Data/CrIS-SDR-GEO_All/SatelliteAzimuthAngle').value
                vmin = -18000
                vmax = 18000
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_sensor_zenith(self):
        """
        return sensor_zenith
        """
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                vmin = 0
                vmax = 18000
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/All_Data/CrIS-SDR-GEO_All/SatelliteZenithAngle').value

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_solar_azimuth(self):
        """
        return solar_azimuth
        """
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/All_Data/CrIS-SDR-GEO_All/SolarAzimuthAngle').value

                vmin = -18000
                vmax = 18000

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data

    def get_solar_zenith(self):
        """
        return solar_zenith
        """
        if self.resolution == 16000:
            satellite_type1 = ['NPP']
            if self.satellite in satellite_type1:
                vmin = 0
                vmax = 18000
                # s = self.data_shape  # FY3A数据不规整，存在 1810,2048 的数据，取 1800,2048
                with h5py.File(self.in_file, 'r') as h5r:
                    data_pre = h5r.get(
                        '/All_Data/CrIS-SDR-GEO_All/SolarZenithAngle').value
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))

            # 过滤无效值
            invalid_index = np.logical_or(data_pre < vmin, data_pre > vmax)
            data_pre = data_pre.astype(np.float32)
            data_pre[invalid_index] = np.nan
            data = data_pre

        return data


if __name__ == '__main__':
    T1 = datetime.now()

    L1File = 'D:/data/NPP_CRIS/GCRSO-SCRIF-SCRIS_npp_d20180303_t0016319_e0024297_b32881_c20180308030857410779_noac_ops.h5'
    viirs = ReadCrisL1(L1File)
    print viirs.satellite  # 卫星名
    print viirs.sensor  # 传感器名
    print viirs.ymd  # L1 文件年月日 YYYYMMDD
    print viirs.hms  # L1 文件时分秒 HHMMSS
    print viirs.resolution  # 分辨率
    print viirs.channels  # 通道数量
    print viirs.data_shape
#     print viirs.file_attr

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

    print 'get_spectral_response:'
    wavenums, response = viirs.get_spectral_response()
    print_data_status(wavenums)
    print_data_status(response)

    print 'longitude:'
    t_data = viirs.get_longitude()
    print_data_status(t_data)

    print 'latitude:'
    t_data = viirs.get_latitude()
    print_data_status(t_data)
#
    print 'sensor_azimuth:'
    t_data = viirs.get_sensor_azimuth()
    print_data_status(t_data)
    print 'sensor_zenith:'
    t_data = viirs.get_sensor_zenith()
    print_data_status(t_data)
    print 'solar_azimuth:'
    t_data = viirs.get_solar_azimuth()
    print_data_status(t_data)
    print 'solar_zenith:'
    t_data = viirs.get_solar_zenith()
    print_data_status(t_data)
