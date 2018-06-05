# coding: utf-8

'''
Created on 2017年9月7日

@author: wangpeng
'''

import os, h5py
import numpy as np
from PB.pb_time import fy3_ymd2seconds
from PB import pb_sat
from PB import pb_space

# 获取类py文件所在的目录
MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class CLASS_HIRAS_L1():
    '''
    解析HIRAS传感器数据
    '''

    def __init__(self, BandLst):

        self.sat = 'FY3D'
        self.sensor = 'HIRAS'
        self.obrit_direction = []
        self.obrit_num = []

        # 字典类型物理量
        self.DN = {}
        self.Ref = {}
        self.Tbb = {}
        self.Rad = {}
        self.SV = {}
        self.BB = {}

        # 二维矩阵
        self.Lons = []
        self.Lats = []
        self.Time = []
        self.satAzimuth = []
        self.satZenith = []
        self.sunAzimuth = []
        self.sunZenith = []

        self.LandSeaMask = None
        self.LandCover = None
        # 高光谱位置信息，ECEF坐标系位置
        self.hiras_pos = []
        # hiras扫描探元号记录
        self.pixel_num = []

        # 光谱信息
        self.wavenumber = []
        self.radiance = []

        # 增加矢量计算 G,P,L
        self.G_pos = None
        self.L_pos = None
        self.P_pos = None

        # 按通道初始化
        for Band in BandLst:
            self.Tbb[Band] = None
            self.Rad[Band] = None
            self.SV[Band] = None
            self.BB[Band] = None

    def Load(self, L1File):
        # 增加切趾计算
        w0 = 0.23
        w1 = 1 - 2 * w0
        w2 = w0
        ipath = os.path.dirname(L1File)
        iname = os.path.basename(L1File)
        obcFile = os.path.join(ipath, iname[0:-12] + 'OBCXX_MS.HDF')
        print (u'读取 L1 %s' % L1File)
        try:
            h5File_R = h5py.File(L1File, 'r')
            obrit_direction = h5File_R.attrs.get('Orbit Direction')
            obrit_num = h5File_R.attrs.get('Orbit Number')
            self.obrit_direction.append(obrit_direction)
            self.obrit_num.append(obrit_num)

            real_lw = h5File_R.get('/Data/ES_RealLW')[:]
            real_mw = h5File_R.get('/Data/ES_RealMW1')[:]
            real_sw = h5File_R.get('/Data/ES_RealMW2')[:]
            ary_alt = h5File_R.get('/Geolocation/Height')[:]
            ary_lat = h5File_R.get('/Geolocation/Latitude')[:]
            ary_lon = h5File_R.get('/Geolocation/Longitude')[:]
            ary_sata = h5File_R.get('/Geolocation/Sensor_Azimuth')[:]
            ary_satz = h5File_R.get('/Geolocation/Sensor_Zenith')[:]
            ary_suna = h5File_R.get('/Geolocation/Solar_Azimuth')[:]
            ary_sunz = h5File_R.get('/Geolocation/Solar_Zenith')[:]
            ary_LandSeaMask = h5File_R.get('/Geolocation/LandSeaMask')[:]
            ary_LandCover = h5File_R.get('/Geolocation/Land_Cover')[:]
            ary_day = h5File_R.get('/Geolocation/Daycnt')[:]
            ary_time = h5File_R.get('/Geolocation/Mscnt')[:]
        except Exception as e:
            print str(e)
            return
        finally:
            h5File_R.close()

        # 记录探元号
        pixel_num = np.full(ary_lat.shape, 0, 'i1')
        pixel_num[:, :, 0] = 1
        pixel_num[:, :, 1] = 2
        pixel_num[:, :, 2] = 3
        pixel_num[:, :, 3] = 4
        self.pixel_num = pixel_num
        self.pixel_num = self.pixel_num.reshape(ary_lon.size, 1)

        # 开头和结尾不参与计算，'切趾计算 w0*n-1 + w1*n + w2*n+1 当作n位置的修正值'
        real_lw[:, :, :, 1:-1] = w0 * real_lw[:, :, :, :-2] + w1 * real_lw[:, :, :, 1:-1] + w2 * real_lw[:, :, :, 2:]
        real_mw[:, :, :, 1:-1] = w0 * real_mw[:, :, :, :-2] + w1 * real_mw[:, :, :, 1:-1] + w2 * real_mw[:, :, :, 2:]
        real_sw[:, :, :, 1:-1] = w0 * real_sw[:, :, :, :-2] + w1 * real_sw[:, :, :, 1:-1] + w2 * real_sw[:, :, :, 2:]

        # print '删除开头和结尾的俩个光谱 ', real_lw.shape
        real_lw = real_lw[:, :, :, 2:-2]
        real_mw = real_mw[:, :, :, 2:-2]
        real_sw = real_sw[:, :, :, 2:-2]

        self.radiance = np.concatenate((real_lw, real_mw, real_sw), axis=3)
        self.radiance = self.radiance.reshape(ary_lon.size, 1, self.radiance.shape[-1])
        # HIRAS光谱波长范围
        ary_wave_lw = np.arange(650., 1135.0 + 0.625, 0.625)
#         print ary_wave_lw.shape
        ary_wave_mw = np.arange(1210., 1750. + 0.625, 0.625)
#         print ary_wave_mw.shape
        ary_wave_sw = np.arange(2155., 2550.0 + 0.625, 0.625)
#         print ary_wave_sw.shape

        self.wavenumber = np.concatenate((ary_wave_lw, ary_wave_mw, ary_wave_sw), axis=0)

#         print '11', self.wavenumber.shape
        # 时间计算

        v_ymd2seconds = np.vectorize(fy3_ymd2seconds)
        T1 = v_ymd2seconds(ary_day, ary_time)
        self.Time = np.full_like(ary_lon, -999, 'i4')

#         print time.gmtime(T1[0, 0])
        self.Time = np.repeat(T1[:, 0:29], 4).reshape(self.Time.shape)
#         print  time.gmtime(self.Time[0, 0, 0])

        self.Time = self.Time.reshape(self.Time.size, 1)

        print (u'读取geo %s' % obcFile)
        try:
            # 读取OBC文件
            h5File_R = h5py.File(obcFile, 'r')
            ary_obc_x = h5File_R.get('/Geo/Ecr_sc_position_X')[:]
            ary_obc_y = h5File_R.get('/Geo/Ecr_sc_position_Y')[:]
            ary_obc_z = h5File_R.get('/Geo/Ecr_sc_position_Z')[:]
        except Exception as e:
            print str(e)
            return
        finally:
            h5File_R.close()

        # ecef位置信息
        ary_pos_shape = ary_alt.shape + (3,)
        ary_pos = np.full(ary_pos_shape, 0.)
        for i in xrange(ary_pos.shape[2]):
            ary_pos[:, :, i, 0] = ary_obc_x
            ary_pos[:, :, i, 1] = ary_obc_y
            ary_pos[:, :, i, 2] = ary_obc_z
        self.hiras_pos = ary_pos.reshape(ary_lon.size, 1, 3)

        # 土地覆盖
        ary_LandCover_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_LandCover >= 0, ary_LandCover <= 254)
        ary_LandCover_idx[condition] = ary_LandCover[condition]
        self.LandCover = ary_LandCover_idx
        self.LandCover = self.LandCover.reshape(ary_lon.size, 1)

        # 海陆掩码
        ary_LandSeaMask_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_LandSeaMask >= 0, ary_LandSeaMask <= 7)
        ary_LandSeaMask_idx[condition] = ary_LandSeaMask[condition]
        self.LandSeaMask = ary_LandSeaMask_idx
        self.LandSeaMask = self.LandSeaMask.reshape(ary_lon.size, 1)

        # 经纬度
        ary_lon_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_lon > -180., ary_lon < 180.)
        ary_lon_idx[condition] = ary_lon[condition]
        self.Lons = ary_lon_idx
        self.Lons = self.Lons.reshape(ary_lon.size, 1)

        ary_lat_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_lat > -90., ary_lat < 90.)
        ary_lat_idx[condition] = ary_lat[condition]
        self.Lats = ary_lat_idx
        self.Lats = self.Lats.reshape(ary_lon.size, 1)

        # 卫星方位角 天顶角
        ary_sata_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_sata > 0, ary_sata < 36000)
        ary_sata_idx[condition] = ary_sata[condition]
        self.satAzimuth = ary_sata_idx / 100.
        self.satAzimuth = self.satAzimuth.reshape(ary_lon.size, 1)

        ary_satz_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_satz > 0, ary_satz < 18000)
        ary_satz_idx[condition] = ary_satz[condition]
        self.satZenith = ary_satz_idx / 100.
        self.satZenith = self.satZenith.reshape(ary_lon.size, 1)

        # 太阳方位角 天顶角
        ary_suna_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_suna > 0, ary_suna < 36000)
        ary_suna_idx[condition] = ary_suna[condition]
        self.sunAzimuth = ary_suna_idx / 100.
        self.sunAzimuth = self.sunAzimuth.reshape(ary_lon.size, 1)

        ary_sunz_idx = np.full(ary_lon.shape, np.nan)
        condition = np.logical_and(ary_sunz > 0, ary_sunz < 18000)
        ary_sunz_idx[condition] = ary_sunz[condition]
        self.sunZenith = ary_sunz_idx / 100.
        self.sunZenith = self.sunZenith.reshape(ary_lon.size, 1)

    def get_rad_tbb(self, D1, BandLst):
        '''
        D1是目标类的实例
        '''
        # iasi 的光谱波数范围
        WaveNum2 = self.wavenumber
        for Band in BandLst:
            WaveNum1 = D1.waveNum[Band]
            WaveRad1 = D1.waveRad[Band]
            WaveRad2 = pb_sat.spec_interp(WaveNum1, WaveRad1, WaveNum2)
            newRad = pb_sat.spec_convolution(WaveNum2, WaveRad2, self.radiance)
            tbb = pb_sat.planck_r2t(newRad, D1.WN[Band], D1.TeA[Band], D1.TeB[Band])

            self.Tbb[Band] = tbb.reshape(tbb.size, 1)
            self.Rad[Band] = newRad.reshape(newRad.size, 1)
            self.SV[Band] = None
            self.BB[Band] = None

    def get_G_P_L(self):

        # 增加G矢量计算
        # 第一组经纬度（成像仪）的ECEF坐标系下的值
        G_pos = np.zeros(np.append(self.Lons.shape, 3))
        high = np.zeros_like(self.Lons)
        G_pos[:, :, 0], G_pos[:, :, 1], G_pos[:, :, 2] = pb_space.LLA2ECEF(self.Lons, self.Lats, high)

        self.G_pos = G_pos
        self.P_pos = self.hiras_pos
        self.L_pos = G_pos - self.P_pos


if __name__ == '__main__':
    BandLst = ['CH_20', 'CH_21', 'CH_22', 'CH_23', 'CH_24', 'CH_25']
    L1File = 'D:/data/FY3D+MERSI_HIRAS/FY3D_HIRAS_GBAL_L1_20180326_0045_016KM_MS.HDF'
    hiras = CLASS_HIRAS_L1(BandLst)
    hiras.Load(L1File)


