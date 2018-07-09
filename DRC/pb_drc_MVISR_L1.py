#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/6 9:41
@Author  : AnNing
"""
from datetime import datetime, timedelta

import sys
from pyhdf.SD import SD, SDC

import numpy as np

from PB.pb_time import time_block
from DV.dv_map import dv_map

class CLASS_MVISR_L1(object):

    def __init__(self):

        # 定标使用
        self.sat = 'FY1C'
        self.sensor = 'MVISR'
        self.res = 1100
        self.Band = 4
        self.obrit_direction = []
        self.obrit_num = []

        self.Dn = {}
        self.Ref = {}
        self.Rad = {}
        self.Tbb = {}

        self.satAzimuth = []
        self.satZenith = []
        self.sunAzimuth = []
        self.sunZenith = []
        self.Lons = []
        self.Lats = []
        self.Time = []
        self.SV = {}
        self.BB = {}
        self.LandSeaMask = []
        self.LandCover = []

        # 新添加
        self.ir_coeff_k0 = {}
        self.ir_coeff_k1 = {}
        self.RelativeAzimuth = []

        # 其他程序使用
        self.LutFile = []
        self.IR_Coeff = []
        self.VIS_Coeff = []

        # 红外通道的中心波数，固定值，MERSI_Equiv Mid_wn (cm-1)
        self.WN = {}
        # 红外转tbb的修正系数，固定值
        self.TeA = {}
        self.TeB = {}

        # 所有通道的中心波数和对应的响应值 ，SRF
        self.waveNum = {}
        self.waveRad = {}

    def Load(self, in_file):
        hdf4 = SD(in_file, SDC.READ)
        # try:
        year_dataset = hdf4.select('Year_Count')[:]
        msec_dataset = hdf4.select('Msec_Count')[:]
        day_dataset = hdf4.select('Day_Count')[:]

        dn_dataset = hdf4.select('Earth_View')[:]
        sv_dataset = hdf4.select('Space_View')[:]
        bb_dataset = hdf4.select('Black_Body_View')[:]

        sensor_zenith_dataset = hdf4.select('Sensor_Zenith')[:]
        solar_zenith_dataset = hdf4.select('Solar_Zenith')[:]
        relative_azimuth = hdf4.select('Relative_Azimuth')[:]

        longitude_dataset = hdf4.select('Longitude')[:]
        latitude_dataset = hdf4.select('Latitude')[:]

        coeff_dataset = hdf4.select('Calibration_coeff')[:]

        # 过滤无效值
        idx_vaild = np.where(year_dataset != 0)[0]

        year = year_dataset[idx_vaild][0]
        day = day_dataset[idx_vaild][0]
        msec_dataset = msec_dataset[idx_vaild]

        dn_dataset = dn_dataset[:, idx_vaild, :]
        sv_dataset = sv_dataset[:, idx_vaild, :]
        bb_dataset = bb_dataset[:, idx_vaild, :]

        sensor_zenith_dataset = sensor_zenith_dataset[idx_vaild, :]
        solar_zenith_dataset = solar_zenith_dataset[idx_vaild, :]
        relative_azimuth = relative_azimuth[idx_vaild, :]

        longitude_dataset = longitude_dataset[idx_vaild, :]
        latitude_dataset = latitude_dataset[idx_vaild, :]

        coeff_dataset = coeff_dataset[idx_vaild, :]

        time = self.create_time(year, day, msec_dataset)  # （x,）

        cols_data = 1018
        self.Time = self.extend_matrix_2d(time, 1, cols_data)
        self.Lons = self.interpolate_lat_lon(longitude_dataset, 51, cols_data)
        self.Lats = self.extend_matrix_2d(latitude_dataset, 51, cols_data)
        self.satZenith = self.extend_matrix_2d(sensor_zenith_dataset, 51, cols_data)
        self.sunZenith = self.extend_matrix_2d(solar_zenith_dataset, 51, cols_data)
        self.RelativeAzimuth = self.extend_matrix_2d(relative_azimuth, 51, cols_data)

        for i in xrange(self.Band):
            channel_name = 'CH_{:02d}'.format(i + 1)
            self.Dn[channel_name] = dn_dataset[i, :]
            self.SV[channel_name] = self.extend_matrix_2d(sv_dataset[i, :], 10, cols_data)
            self.BB[channel_name] = self.extend_matrix_2d(bb_dataset[i, :], 6, cols_data)
            k0_dataset = self.change_1d_to_2d(coeff_dataset[:, i + 1])
            k1_dataset = self.change_1d_to_2d(coeff_dataset[:, i])
            self.ir_coeff_k0[channel_name] = self.extend_matrix_2d(k0_dataset, 1, cols_data)
            self.ir_coeff_k1[channel_name] = self.extend_matrix_2d(k1_dataset, 1, cols_data)

        # except Exception as why:
        #     print why
        # finally:
        #     hdf4.end()

        # for i in xrange(self.Band):
        #     channel_name = 'CH_{:02d}'.format(i + 1)

    def create_time(self, year, day, msec_dataset):
        time = []
        for msec in msec_dataset:
            timestamp = self.year_days_msec_to_timestamp(year, day, msec)
            time.append(timestamp)
        time = self.change_1d_to_2d(time)
        return time

    @staticmethod
    def year_days_msec_to_timestamp(year, day_in_year, msec):
        """
        第几天，第几天，当天第多少毫秒，转为距离1970年的时间戳
        :param year:
        :param day_in_year:
        :param msec:
        :return:
        """
        year_date = datetime.strptime(str(year), '%Y')
        date_1970 = datetime.strptime('1970', '%Y')
        day_delta = timedelta(days=int(day_in_year) - 1)
        second_delata = timedelta(seconds=(int(msec) / 1000))
        delta_since_1970 = year_date + day_delta + second_delata - date_1970
        timestamp = delta_since_1970.total_seconds()
        return timestamp

    @staticmethod
    def change_1d_to_2d(data):
        return np.array(data).reshape(len(data), -1)

    @staticmethod
    def interpolate_lat_lon(data, cols_data, cols_count):
        """
        传入的数据必须是 2 维
        原数据每两列按线性比例插值
        :param data: 2D [[]]
        :param cols_data: int
        :param cols_count: int
        :return: 2D [[]]
        """
        if cols_data == 1:
            data_one = data[:].reshape(len(data), -1)
            data_extend = np.tile(data_one, cols_count)
        else:
            data_extend = None
            times = int(np.ceil(float(cols_count) / cols_data))
            for col_data in xrange(int(cols_data) - 1):
                data_times = None
                data_one = data[:, col_data]
                data_next = data[:, col_data + 1]
                if col_data < (int(cols_data) - 2):
                    times = times
                else:
                    times = cols_count - data_extend.shape[1]
                for row_data, value_one in enumerate(data_one):
                    data_inter = np.linspace(data_one[row_data], data_next[row_data], times)
                    data_inter = data_inter.reshape(1, -1)
                    if data_times is None:
                        data_times = data_inter
                    else:
                        data_times = np.concatenate((data_times, data_inter), axis=0)
                if data_extend is None:
                    data_extend = data_times
                else:
                    data_extend = np.concatenate((data_extend, data_times), axis=1)

            if data_extend is not None:
                data_extend = data_extend[:, :cols_count]
        return data_extend

    @staticmethod
    def extend_matrix_2d(data, cols_data, cols_count):
        """
        传入的数据必须是 2 维
        原数据每一列按一定比例扩展到多少列
        :param data:
        :param cols_data:
        :param cols_count:
        :return:
        """
        data_extend = None
        times = int(np.ceil(float(cols_count) / cols_data))
        for i in xrange(cols_data):
            data_one = data[:, i].reshape(len(data), -1)
            data_times = np.tile(data_one, times)
            if data_extend is None:
                data_extend = data_times
            else:
                data_extend = np.concatenate((data_extend, data_times), axis=1)
        if data_extend is not None:
            data_extend = data_extend[:, :cols_count]
        return np.array(data_extend)


if __name__ == '__main__':
    in_file = r'E:\projects\six_sv\FY1C_L1_GDPT_20000205_0707.HDF'
    mvisr = CLASS_MVISR_L1()
    mvisr.Load(in_file)
    print mvisr.ir_coeff_k0['CH_01'].shape
    print mvisr.ir_coeff_k1['CH_01'].shape
    print mvisr.Time
    print mvisr.BB['CH_01'].shape
    print mvisr.Lats.shape
    print mvisr.Lons.shape

    p = dv_map()
    p.easyplot(mvisr.Lats, mvisr.Lons, mvisr.satZenith,
               ptype=None, markersize=0.1, marker='o')
    p.savefig('test0.png')
