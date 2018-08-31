# -*- coding:utf-8 -*-
from datetime import datetime
import os
import re
import h5py

from PB import pb_sat
from PB.pb_io import attrs2dict
from PB.pb_time import get_ymd, get_hm
from pb_drc_base import ReadL1
import numpy as np

"""
@Time    : 2018/8/28 10:00
@Author  : YUSHUAI
"""
"""
读取处理 L1 数据，1000m 和 250m
处理原则
1 过滤原数据无效值和填充值，过滤后无效数据统一使用 NaN 填充
2 统一 shape
3 统一数据 dtype
4 统一通道相关和通道无关数据的存放格式
"""


# sys.path.append('E:\KY\git')


g_main_path, g_main_file = os.path.split(os.path.realpath(__file__))


class ReadIrasL1(ReadL1):
    """
     读取 VIRR 传感器的 L1 数据
     分辨率：17000
     卫星： FY3A FY3B FY3C
     通道数量：26
     可见光通道：1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
     红外通道：21 22 23 24 25 26
    """

    def __init__(self, in_file):
        sensor = 'IRAS'
        super(ReadIrasL1, self).__init__(in_file, sensor)

    def set_resolution(self):
        """
        根据L1文件名 set self.resolution 分辨率
        :return:
        """
        file_name = os.path.basename(self.in_file)
        if '017KM' in file_name:
            self.resolution = 17000
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
        self.ymd = get_ymd(file_name)
        self.hms = get_hm(file_name) + '00'

    def set_file_attr(self):
        """
        根据 self.file_level_1 获取 L1 文件的属性
        set self.level_1_attr
        储存格式是字典
        :return:
        """
        if self.resolution == 17000:
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
        data_file = self.in_file
        if self.resolution == 17000:
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    dn = hdf5_file.get('/FY3A_IRAS_DN')[:, :, :]
                    self.data_shape = dn[0, :, :].shape
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    dn = hdf5_file.get('/Data/IRAS_DN')[:, :, :]
                    self.data_shape = dn[0, :, :].shape
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
        if self.resolution == 17000:
            self.channels = 26
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))

    def get_dn(self):
        """
        从数据文件中获取 DN 值, set self.dn
        :return:
        """
        data = dict()
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch26_dn = hdf5_file.get('/FY3A_IRAS_DN')[:, :, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch26_dn = hdf5_file.get('/Data/IRAS_DN')[:, :, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                dn = ary_ch26_dn[i].astype(np.float32)
                idx = np.logical_or(
                    ary_ch26_dn[i] >= 4095, ary_ch26_dn[i] <= -4095)
                dn[idx] = np.nan
                data[channel_name] = dn
        else:
            raise ValueError(
                'Cant read this data, please check its resolution: {}'.format(self.in_file))
        return data

    def get_tbb(self):
        """
              从数据文件中获取 DNTBB值, set self.tbb
        :return:
        """
        data = dict()
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch26_tb = hdf5_file.get('/FY3A_IRAS_TB')[:, :, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch26_tb = hdf5_file.get('/Data/IRAS_TB')[:, :, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                tbb = ary_ch26_tb[i].astype(np.float32)
                idx = np.logical_or(
                    ary_ch26_tb[i] >= 350, ary_ch26_tb[i] <= 150)
                tbb[idx] = np.nan
                data[channel_name] = tbb
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
        if self.resolution == 17000:  # 分辨率为 17000
            satellite_type1 = ['FY3A', 'FY3B']
            satellite_type2 = ['FY3C']
            data_file = self.in_file
            if not os.path.isfile(data_file):
                raise ValueError(
                    'Data file is not exist. {}'.format(data_file))
            central_wave_number = self.get_central_wave_number()
            if self.satellite in satellite_type1:
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch26_tb = hdf5_file.get('/FY3A_IRAS_TB')[:, :, :]
            elif self.satellite in satellite_type2:
                with h5py.File(data_file, 'r') as hdf5_file:
                    ary_ch26_tb = hdf5_file.get('/Data/IRAS_TB')[:, :, :]
            else:
                raise ValueError(
                    'Cant read this satellite`s data.: {}'.format(self.satellite))
            for i in xrange(self.channels):
                channel_name = 'CH_{:02d}'.format(i + 1)
                tbb = ary_ch26_tb[i].astype(np.float32)
                idx = np.logical_or(
                    ary_ch26_tb[i] >= 350, ary_ch26_tb[i] <= 150)
                tbb[idx] = np.nan
                rad = np.full(self.data_shape, np.nan)
                rad = pb_sat.plank_iras_tb2rad(
                    tbb, central_wave_number[channel_name])
                data[channel_name] = rad
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
        central_wave_number = {'CH_01': 669.976914, 'CH_02': 680.162001, 'CH_03': 691.391561, 'CH_04': 702.858560,
                               'CH_05': 715.270436, 'CH_06': 732.203858, 'CH_07': 749.383836,
                               'CH_08': 801.671379, 'CH_09': 899.414299, 'CH_10': 1032.591246,
                               'CH_11': 1343.617931, 'CH_12': 1364.298075, 'CH_13': 1529.295554,
                               'CH_14': 2191.007796, 'CH_15': 2209.606615, 'CH_16': 2237.159430,
                               'CH_17': 2242.434450, 'CH_18': 2387.507219, 'CH_19': 2517.407819,
                               'CH_20': 2667.944995, 'CH_21': 14431.029680, 'CH_22': 11265.161110,
                               'CH_23': 10601.633020, 'CH_24': 10607.324440, 'CH_25': 8098.870570,
                               'CH_26': 6061.054448}
        if self.resolution == 17000:
            satellite_type1 = ['FY3A', 'FY3B', 'FY3C']
            if self.satellite in satellite_type1:
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
    t_in_file = r'E:\TEST\data\FY3A_IRASX_GBAL_L1_20160813_1606_017KM_MS.HDF'
#     t_in_file = r'E:\TEST\data\FY3B_IRASX_GBAL_L1_20180101_0018_017KM_MS.HDF'
#     t_in_file = r'E:\TEST\data\FY3C_IRASX_GBAL_L1_20180101_0124_017KM_MS.HDF'
    t_read_l1 = ReadIrasL1(t_in_file)
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
    print_channel_data(t_data)

    print 'tbb:'
    t_data = t_read_l1.get_tbb()
    print_channel_data(t_data)

    print 'rad'
    t_data = t_read_l1.get_rad()
    print_channel_data(t_data)

    t_data = t_read_l1.get_central_wave_number()
    print 'central_wave_number:'
    print t_data

    t1_data, t2_data = t_read_l1.get_spectral_response()
    print 'wave_number:'
    print_channel_data(t1_data)
    print_channel_data(t2_data)

    print 'height'
    t_data = t_read_l1.get_height()
    print_data_status(t_data)

    t_data = t_read_l1.get_latitude()
    print 'latitude:'
    print_data_status(t_data)

    t_data = t_read_l1.get_longitude()
    print 'longitude:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_land_sea_mask()
    print 'land_sea_mask:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_land_cover()
    print 'land_cover:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_sensor_azimuth()
    print 'sensor_azimuth:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_sensor_zenith()
    print 'sensor_zenith:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_solar_azimuth()
    print 'solar_azimuth:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_solar_zenith()
    print 'solar_zenith:'
    print t_data
    print_data_status(t_data)

    t_data = t_read_l1.get_timestamp()
    print 'timestamp:'
    print t_data
    print_data_status(t_data)
    datetime_timestamp = datetime.utcfromtimestamp(t_data[0][0])
    datetime_file = datetime.strptime(
        t_read_l1.ymd + t_read_l1.hms, '%Y%m%d%H%M%S')
    if datetime_timestamp != datetime_file:
        print 'Error', '-' * 100
        print t_data[0][0], datetime_timestamp
        print t_read_l1.ymd + t_read_l1.hms, datetime_file
        raise ValueError('Please check the get_timestamp')
