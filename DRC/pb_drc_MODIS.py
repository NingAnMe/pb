# -*- coding:utf-8 -*-
"""
@Time    : 2018/8/31 10:00
@Author  : YUSHUAI
"""
from datetime import datetime
import os
import pdb
import re
import sys
from pyhdf.SD import SD, SDC
import h5py
# sys.path.append('D:\work')
from PB import pb_sat, pb_name
from PB.pb_io import attrs2dict
from PB.pb_sat import planck_r2t
from PB.pb_time import get_ymd, get_hm
from pb_drc_base import ReadL1
import numpy as np


g_main_path, g_main_file = os.path.split(os.path.realpath(__file__))
"""
读取处理 L1 数据，1000m 和 250m
处理原则
1 过滤原数据无效值和填充值，过滤后无效数据统一使用 NaN 填充
2 统一 shape
3 统一数据 dtype
4 统一通道相关和通道无关数据的存放格式
"""


class ReadModisL1(ReadL1):
    """
     读取 modis 传感器的 L1 数据
     分辨率：1000
     卫星：AQUA TERRA
     通道数量：36
     可见光通道：1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 26
     红外通道：20 21 22 23 24 25 ,27,28,29,30,31,32,33,34,35,36
    """

    def __init__(self, in_file):
        sensor = 'MODIS'
        super(ReadModisL1, self).__init__(in_file, sensor)

    def set_resolution(self):
        """
        根据L1文件名 set self.resolution 分辨率
        :return:
        """
        file_name = os.path.basename(self.in_file)
        if 'MYD021KM' in file_name:
            self.resolution = 1000
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def set_satellite(self):
        """
        根据L1文件名 set self.satellite 卫星名
        :return:
        """
        if "AQUA" in self.in_file:
            self.satellite = "AQUA"
        elif "TERRA" in self.in_file:
            self.satellite = "TERRA"
        else:
            raise ValueError('Cant get the satellite name from file name.')

    def __get_obc_file(self):
        """
        返回 OBC 文件
        :return:
        """
        if self.resolution == 17000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                obc_file = self.in_file[:-12] + 'OBCXX_MS.HDF'
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))
        return obc_file

    def set_ymd_hms(self):
        """
        根据根据L1文件名 set self.level_1_ymd 和 self.level_1_ymd
        :return:
        """
        file_name = os.path.basename(self.in_file)
        yd = file_name.split(".")[1][1:]
        dt = datetime.strptime(yd, '%Y%j').date()
        fmt = '%Y%m%d'
        self.ymd = dt.strftime(fmt)
        self.hms = file_name.split(".")[2] + '00'

    def set_file_attr(self):
        """
        根据 self.file_level_1 获取 L1 文件的属性
        set self.level_1_attr
        储存格式是字典
        :return:
        """
        if self.resolution == 1000:
            satellite_type = ['AQUA', 'TERRA']
            if self.satellite in satellite_type:
                h4File = SD(self.in_file, SDC.READ)
                self.file_attr = attrs2dict(h4File.attributes())
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))

    def set_data_shape(self):
        """
        根据 self.satellite set self.dataset_shape
        :return:
        """
        # 如果分辨率是 1000 米
        if self.resolution == 1000:
            satellite_type = ['AQUA', 'TERRA']
            if self.satellite in satellite_type:
                h4File = SD(self.in_file, SDC.READ)
                in_data_r250 = h4File.select('EV_250_Aggr1km_RefSB').get()
                self.data_shape = in_data_r250.shape[1:]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                "Cant handle this resolution: ".format(self.resolution))

    def set_channels(self):
        """
        根据 self.satellite set self.sensor_channel
        :return:
        """
        if self.resolution == 1000:
            self.channels = 36
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def get_dn(self):
        """
        return DN
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type = ['AQUA', 'TERRA']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type:
                #                 with SD(data_file, SDC.READ) as h4r:
                h4r = SD(data_file, SDC.READ)
                ary_ch1_2 = h4r.select('EV_250_Aggr1km_RefSB').get()
                ary_ch3_7 = h4r.select('EV_500_Aggr1km_RefSB').get()
                ary_ch8_19_26 = h4r.select('EV_1KM_RefSB').get()
                ary_ch20_36 = h4r.select('EV_1KM_Emissive').get()
                h4r.end()
                vmin = 0
                vmax = 32767

                # 逐个通道处理
                for i in xrange(38):
                    band = 'CH_{:02d}'.format(i + 1)
                    if i < 2:
                        k = i
                        data_pre = ary_ch1_2[k]
                        # 开始处理
                    elif i >= 2 and i < 7:
                        k = i
                        data_pre = ary_ch3_7[k - 2]
                    elif i >= 7 and i < 22:
                        if i < 12:
                            band = 'CH_{:02d}'.format(i + 1)
                            k = i
                            data_pre = ary_ch8_19_26[k - 7]
                        elif i == 12 or i == 13:
                            continue
                        else:
                            band = 'CH_{:02d}'.format(i - 1)
                            k = i
                            data_pre = ary_ch8_19_26[k - 7]
                    else:
                        band = 'CH_{:02d}'.format(i - 1)
                        k = i
                        data_pre = ary_ch20_36[k - 22]
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

    def get_tbb(self):
        """
        从数据文件中获取 DNTBB值, set self.tbb
        function radiance2tbb: convert radiance data into brightness temperature (i.e., equivalent blackbody temperature)
        r: spectral radiance data in w/m2/sr/um
        w: wavelength in micro
        return: reture value, brightness temperature in K (absolute temperature)
        :return:
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 17000
            satellite_type = ['AQUA', 'TERRA']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type:
                # rad转tbb的修正系数，所有时次都是固定值
                tbb_k0 = self.get_tbb_k0()
                tbb_k1 = self.get_tbb_k1()
                rads = self.get_rad()
                central_wave_numbers = self.get_central_wave_number()
                # 逐个通道处理
#                 pdb.set_trace()
                for i in xrange(20, 37, 1):
                    band = 'CH_{:02d}'.format(i)
                    if i == 26:
                        continue
                    else:
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

    def sun_earth_dis_correction(self, ymd):
        '''
        Instantaneous distance between earth and sun correction factor ==(d0/d)^2
        ymd: yyyymmdd
        '''

        stime = datetime.strptime(ymd, '%Y%m%d')
        jjj = int(stime.strftime('%j'))
        OM = (0.9856 * (jjj - 4)) * np.pi / 180.
        dsol = 1. / ((1. - 0.01673 * np.cos(OM)) ** 2)
        return dsol

    def get_tbb_k0(self):
        """
        return K0
        """
        data = dict()
        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type = ['AQUA', 'TERRA']
            if self.satellite in satellite_type:
                data = {'CH_20': 9.993363E-01, 'CH_21': 9.998626E-01, 'CH_22': 9.998627E-01, 'CH_23': 9.998707E-01,
                        'CH_24': 9.998737E-01, 'CH_25': 9.998770E-01, 'CH_27': 9.995694E-01, 'CH_28': 9.994867E-01,
                        'CH_29': 9.995270E-01, 'CH_30': 9.997382E-01, 'CH_31': 9.995270E-01, 'CH_32': 9.997271E-01,
                        'CH_33': 9.999173E-01, 'CH_34': 9.999070E-01, 'CH_35': 9.999198E-01, 'CH_36': 9.999233E-01}
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb_k1(self):
        """
        return K1
        """
        data = dict()

        if self.resolution == 1000:  # 分辨率为 1000
            satellite_type = ['AQUA', 'TERRA']
            if self.satellite in satellite_type:
                data = {'CH_20': 4.818401E-01, 'CH_21': 9.426663E-02, 'CH_22': 9.458604E-02, 'CH_23': 8.736613E-02,
                        'CH_24': 7.873285E-02, 'CH_25': 7.550804E-02, 'CH_27': 1.848769E-01, 'CH_28': 2.064384E-01,
                        'CH_29': 1.674982E-01, 'CH_30': 8.304364E-02, 'CH_31': 1.343433E-01, 'CH_32': 7.135051E-02,
                        'CH_33': 1.948513E-02, 'CH_34': 2.131043E-02, 'CH_35': 1.804156E-02, 'CH_36': 1.683156E-02}

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
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
            satellite_type = ['AQUA', 'TERRA']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type:
                dn_data = self.get_dn()
                h4r = SD(data_file, SDC.READ)
                # 读取1-2通道数据
                ary_ch1_2_s = h4r.select(
                    'EV_250_Aggr1km_RefSB').attributes()['reflectance_scales']
                ary_ch1_2_o = h4r.select(
                    'EV_250_Aggr1km_RefSB').attributes()['reflectance_offsets']
                # 读取 3-7通道数据
                ary_ch3_7_s = h4r.select(
                    'EV_500_Aggr1km_RefSB').attributes()['reflectance_scales']
                ary_ch3_7_o = h4r.select(
                    'EV_500_Aggr1km_RefSB').attributes()['reflectance_offsets']
                # 读取8-20通道， 包含26通道
                ary_ch8_19_26_s = h4r.select(
                    'EV_1KM_RefSB').attributes()['reflectance_scales']
                ary_ch8_19_26_o = h4r.select(
                    'EV_1KM_RefSB').attributes()['reflectance_offsets']
                # 读取20-36通道 不包含26通道
                ary_ch20_36_s = h4r.select(
                    'EV_1KM_Emissive').attributes()['radiance_scales']
                ary_ch20_36_o = h4r.select(
                    'EV_1KM_Emissive').attributes()['radiance_offsets']
                h4r.end()

                a = np.concatenate(
                    (ary_ch1_2_s, ary_ch3_7_s, ary_ch8_19_26_s, ary_ch20_36_s))
                b = np.concatenate(
                    (ary_ch1_2_o, ary_ch3_7_o, ary_ch8_19_26_o, ary_ch20_36_o))

                for i in range(38):
                    band = 'CH_{:02d}'.format(i + 1)
                    if i < 2:
                        k = i
                        data[band] = (dn_data[band] - b[k]) * a[k]
                        # 开始处理
                    elif i >= 2 and i < 7:
                        k = i
                        data[band] = (dn_data[band] - b[k]) * a[k]
                    elif i >= 7 and i < 22:
                        if i < 12:
                            band = 'CH_{:02d}'.format(i + 1)
                            k = i
                            data[band] = (dn_data[band] - b[k]) * a[k]
                        elif i == 12 or i == 13:
                            continue
                        else:
                            band = 'CH_{:02d}'.format(i - 1)
                            k = i
                            data[band] = (dn_data[band] - b[k]) * a[k]
                    else:
                        band = 'CH_{:02d}'.format(i - 1)
                        k = i
                        data[band] = (dn_data[band] - b[k]) * a[k]

            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_height(self):
        """
              从数据文件中获取 DNTBB值, set self.height
        :return:
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    height = hdf5_file.get('/Height')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    height = hdf5_file.get('/Geolocation/DEM')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = height.astype(np.float32)
            idx = np.logical_or(
                height >= 10000, height <= -400)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_latitude(self):
        """
            从数据文件中获取 纬度值, set self.latitude
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    latitude = hdf5_file.get('/Latitude')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    latitude = hdf5_file.get('/Geolocation/Latitude')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = latitude.astype(np.float32)
            idx = np.logical_or(
                latitude >= 90, latitude <= -90)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_longitude(self):
        """
            从数据文件中获取 经度值, set self.longitude
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    longitude = hdf5_file.get('/Longitude')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    longitude = hdf5_file.get('/Geolocation/Longitude')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = longitude.astype(np.float32)
            idx = np.logical_or(
                longitude >= 180, longitude <= -180)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_land_sea_mask(self):
        """
            从数据文件中获取海陆类型, set self.land_sea_mask
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    landseamask = hdf5_file.get('/LandSeaMask')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    landseamask = hdf5_file.get(
                        '/Geolocation/LandSeaMask')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = landseamask.astype(np.float32)
            idx = np.logical_or(
                landseamask > 7, landseamask < 0)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_land_cover(self):
        """
            从数据文件中获取土地覆盖, set self.land_cover
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    landcover = hdf5_file.get('/LandCover')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    landcover = hdf5_file.get(
                        '/Geolocation/LandCover')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = landcover.astype(np.float32)
            idx = np.logical_or(
                landcover > 17, landcover < 0)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sensor_azimuth(self):
        """
            从数据文件中获取卫星方位角 , set self.sensor_azimuth
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    sensorazimuth = hdf5_file.get('/SensorAzimuth')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    sensorazimuth = hdf5_file.get(
                        '/Geolocation/SensorAzimuth')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = sensorazimuth.astype(np.float32)
            idx = np.logical_or(
                sensorazimuth >= 18000, sensorazimuth <= -18000)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_sensor_zenith(self):
        """
            从数据文件中获取卫星天顶角 , set self.sensor_zenith
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    sensorzenith = hdf5_file.get('/SensorZenith')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    sensorzenith = hdf5_file.get(
                        '/Geolocation/SensorZenith')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = sensorzenith.astype(np.float32)
            idx = np.logical_or(
                sensorzenith >= 18000, sensorzenith <= 0)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_solar_azimuth(self):
        """
            从数据文件中获取太阳的方位角 , set self.solar_azimuth
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    solarazimuth = hdf5_file.get('/SolarAzimuth')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    solarazimuth = hdf5_file.get(
                        '/Geolocation/SolarAzimuth')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = solarazimuth.astype(np.float32)
            idx = np.logical_or(
                solarazimuth >= 18000, solarazimuth <= -18000)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_solar_zenith(self):
        """
            从数据文件中获取太阳的天顶角 , set self.solar_zenith
        """
        data = None
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    solarzenith = hdf5_file.get('/SolarZenith')[:, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    solarzenith = hdf5_file.get(
                        '/Geolocation/SolarZenith')[:, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            data = solarzenith.astype(np.float32)
            idx = np.logical_or(
                solarzenith >= 18000, solarzenith <= 0)
            data[idx] = np.nan

        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_timestamp(self):

        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    scnlin_mscnt = hdf5_file.get('/Scnlin_mscnt')[:]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    scnlin_mscnt = hdf5_file.get('/Data/Scnlin_mscnt')[:]
            else:
                raise ValueError(
                    'Cant read this data, please check its resolution: {}'.format(self.in_file))
            seconds_of_file = (scnlin_mscnt[-1] - scnlin_mscnt[0]) / 1000

            file_date = datetime.strptime(self.ymd + self.hms, '%Y%m%d%H%M%S')
            timestamp = (file_date - datetime(1970, 1,
                                              1, 0, 0, 0)).total_seconds()
            row_length = self.data_shape[0]
            delta = np.linspace(0, seconds_of_file - 1, row_length)
            data = np.full(self.data_shape, np.nan, dtype=np.float64)
            data[:] = (delta + timestamp).reshape(-1, 1)
            data = data.astype(np.int32)
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_central_wave_number(self):
        # 固定值
        # 中心波数: wn(cm-1) = 10 ^ 7 / wave_length(nm)
        # 红外通道的中心波数，固定值，MERSI_Equiv Mid_wn (cm-1)
        central_wave_number = {'CH_20': 2.647409E+03, 'CH_21':  2.511760E+03, 'CH_22': 2.517908E+03,
                               'CH_23': 2.462442E+03, 'CH_24': 2.248296E+03, 'CH_25': 2.209547E+03,
                               'CH_27': 1.474262E+03, 'CH_28': 1.361626E+03, 'CH_29': 1.169626E+03,
                               'CH_30': 1.028740E+03, 'CH_31': 9.076813E+02, 'CH_32': 8.308411E+02,
                               'CH_33': 7.482978E+02, 'CH_34': 7.307766E+02, 'CH_35': 7.182094E+02,
                               'CH_36': 7.035007E+02}
        if self.resolution == 1000:
            satellite_type = ['AQUA', 'TERRA']
            if self.satellite in satellite_type:
                data = central_wave_number
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_spectral_response(self):
        wave_number_dict = dict()
        response_dict = dict()
        if self.resolution == 17000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
                dtype = {
                    'names': ('wave_length', 'response'), 'formats': ('f4', 'f4')}
                for i in xrange(self.channels):
                    k = i + 1
                    channel_name = "CH_{:02d}".format(k)
                    file_name = '{}_{}_SRF_CH{:02d}_Pub.txt'.format(
                        self.satellite, self.sensor, k)
                    data_file = os.path.join(g_main_path, 'SRF', file_name)
                    if not os.path.isfile(data_file):
                        continue
                    datas = np.loadtxt(data_file, dtype=dtype)
                    wave_length = datas['wave_length'][::-1]
                    wave_number_channel = 10 ** 7 / wave_length
                    wave_number_dict[channel_name] = wave_number_channel
                    response_channel = datas['response'][::-1]
                    response_dict[channel_name] = response_channel
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return wave_number_dict, response_dict


if __name__ == '__main__':
    t_in_file = r'E:\TEST\data\AQUA\MYD021KM.A2018215.0930.006.2018215233308.hdf'
#     t_in_file = r'E:\TEST\data\FY3B_IRASX_GBAL_L1_20180101_0018_017KM_MS.HDF'
#     t_in_file = r'E:\TEST\data\FY3C_IRASX_GBAL_L1_20180101_0124_017KM_MS.HDF'
    t_read_l1 = ReadModisL1(t_in_file)
    print 'attribute', '-' * 50
    print t_read_l1.satellite  # 卫星名
    print t_read_l1.sensor  # 传感器名
    print t_read_l1.ymd  # L1 文件年月日 YYYYMMDD
    print t_read_l1.hms  # L1 文件时分秒 HHMMSS
    print t_read_l1.resolution  # 分辨率
    print t_read_l1.channels  # 通道数量
    print t_read_l1.data_shape
#     print t_read_l1.file_attr  # L1 文件属性
#
#     print 'Channel', '-' * 50

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
    t_data = t_read_l1.get_dn()
    # print t_data
#     print_channel_data(t_data)
    print t_data["CH_20"][0, 0]

    print 'tbb:'
    t_data = t_read_l1.get_tbb()
#     pdb.set_trace()
#     print_channel_data(t_data)
    print t_data["CH_20"][0, 0]

    print 'rad'
    t_data = t_read_l1.get_rad()
#     print t_data
#     print_channel_data(t_data)
    print t_data["CH_20"][0, 0]
#

    t_data = t_read_l1.get_central_wave_number()
    print 'central_wave_number:'
    print t_data["CH_20"]
#     print t_data
#
#     t1_data, t2_data = t_read_l1.get_spectral_response()
#     print 'wave_number:'
#     print_channel_data(t1_data)
#     print_channel_data(t2_data)
#
#     print 'height'
#     t_data = t_read_l1.get_height()
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_latitude()
#     print 'latitude:'
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_longitude()
#     print 'longitude:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_land_sea_mask()
#     print 'land_sea_mask:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_land_cover()
#     print 'land_cover:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_sensor_azimuth()
#     print 'sensor_azimuth:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_sensor_zenith()
#     print 'sensor_zenith:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_solar_azimuth()
#     print 'solar_azimuth:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_solar_zenith()
#     print 'solar_zenith:'
#     print t_data
#     print_data_status(t_data)
#
#     t_data = t_read_l1.get_timestamp()
#     print 'timestamp:'
#     print t_data
#     print_data_status(t_data)
#     datetime_timestamp = datetime.utcfromtimestamp(t_data[0][0])
#     datetime_file = datetime.strptime(
#         t_read_l1.ymd + t_read_l1.hms, '%Y%m%d%H%M%S')
#     if datetime_timestamp != datetime_file:
#         print 'Error', '-' * 100
#         print t_data[0][0], datetime_timestamp
#         print t_read_l1.ymd + t_read_l1.hms, datetime_file
#         raise ValueError('Please check the get_timestamp')
