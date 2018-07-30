# coding: utf-8
"""
Created on 2017年9月7日

@author: wangpeng
@modify: anning 2018-07-30
"""

from datetime import datetime
import os

import h5py

from PB import pb_name, pb_sat
import numpy as np


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class CLASS_VIRR_L1():

    def __init__(self):

        # 定标使用
        self.sat = 'FY3C'
        self.sensor = 'VIRR'
        self.res = 1000
        self.Band = 10
        self.obrit_direction = []
        self.obrit_num = []

        self.Dn = {}
        self.Ref = {}
        self.Rad = {}
        self.Tbb = {}

        self.SV = {}
        self.BB = {}
        self.Height = []
        self.satAzimuth = []
        self.satZenith = []
        self.sunAzimuth = []
        self.sunZenith = []
        self.relaAzimuth = []
        self.relaZenith = []
        self.LandSeaMask = []
        self.LandCover = []
        self.Lons = []
        self.Lats = []
        self.Time = []

        # 其他程序使用
        self.LutFile = []
        self.IR_Coeff = []
        self.VIS_Coeff = []
        self.Rad_non = {}
        self.Tbb_coeff = {}

        # 红外通道的中心波数，固定值，MERSI_Equiv Mid_wn (cm-1)
        self.WN = {'CH_03': 2673.796, 'CH_04': 925.925, 'CH_05': 833.333}
        # 红外转tbb的修正系数，固定值
        self.TeA = {'CH_03': 1, 'CH_04': 1, 'CH_05': 1}
        self.TeB = {'CH_03': 0, 'CH_04': 0, 'CH_05': 0}

        # 所有通道的中心波数和对应的响应值 ，SRF
        self.waveNum = {}
        self.waveRad = {}

    def Load(self, L1File):
        ipath = os.path.dirname(L1File)
        iname = os.path.basename(L1File)

        if 'FY3C' in iname[:4]:
            # 根据输入的L1文件自动拼接GEO文件

            geoFile = os.path.join(ipath, iname[0:-12] + 'GEOXX_MS.HDF')
            obcFile = os.path.join(ipath, iname[0:-12] + 'OBCXX_MS.HDF')
            print u'%s' % L1File
            print u'%s' % geoFile
            print u'%s' % obcFile

            #################### 读取L1文件 ######################
            try:
                with h5py.File(L1File, 'r') as h5File_R:
                    ary_ch3 = h5File_R.get('/Data/EV_Emissive')[:]
                    ary_ch7 = h5File_R.get('/Data/EV_RefSB')[:]
                    ary_offsets = h5File_R.get(
                        '/Data/Emissive_Radiance_Offsets')[:]
                    ary_scales = h5File_R.get('/Data/Emissive_Radiance_Scales')[:]
                    ary_ref_cal = h5File_R.attrs['RefSB_Cal_Coefficients']
                    ary_Nonlinear = h5File_R.attrs[
                        'Prelaunch_Nonlinear_Coefficients']
                    ary_tb_coeff = h5File_R.attrs['Prelaunch_Nonlinear_Coefficients']
            except Exception as e:
                print str(e)
                return
            #################### 读取GEO文件 ######################
            try:
                with h5py.File(geoFile, 'r') as h5File_R:
                    ary_satz = h5File_R.get('/Geolocation/SensorZenith')[:]
                    ary_sata = h5File_R.get('/Geolocation/SensorAzimuth')[:]
                    ary_sunz = h5File_R.get('/Geolocation/SolarZenith')[:]
                    ary_suna = h5File_R.get('/Geolocation/SolarAzimuth')[:]
                    ary_lon = h5File_R.get('/Geolocation/Longitude')[:]
                    ary_lat = h5File_R.get('/Geolocation/Latitude')[:]
                    ary_LandCover = h5File_R.get('/Geolocation/LandCover')[:]
                    ary_LandSeaMask = h5File_R.get('/Geolocation/LandSeaMask')[:]
            except Exception as e:
                print str(e)
            #################### 读取OBC文件 ####################
            try:
                with h5py.File(obcFile, 'r') as h5File_R:
                    ary_sv = h5File_R.get('/Calibration/Space_View')[:]
            except Exception as e:
                print str(e)
                return

        else:
            # FY3A/FY3B VIRR
            # 根据输入的L1文件自动拼接OBC文件
            obcFile = os.path.join(ipath, iname[0:-12] + 'OBCXX_MS.HDF')
            #################### 读取L1文件 ####################
            try:
                with h5py.File(L1File, 'r') as h5File_R:
                    ary_ch3 = h5File_R.get('/EV_Emissive')[:]
                    ary_ch7 = h5File_R.get('/EV_RefSB')[:]
                    ary_offsets = h5File_R.get('/Emissive_Radiance_Offsets')[:]
                    ary_scales = h5File_R.get('/Emissive_Radiance_Scales')[:]
                    ary_ref_cal = h5File_R.attrs['RefSB_Cal_Coefficients']
                    ary_Nonlinear = h5File_R.attrs['Prelaunch_Nonlinear_Coefficients']
                    ary_tb_coeff = h5File_R.attrs['Prelaunch_Nonlinear_Coefficients']
                    ary_satz = h5File_R.get('/SensorZenith')[:]
                    ary_sata = h5File_R.get('/SensorAzimuth')[:]
                    ary_sunz = h5File_R.get('/SolarZenith')[:]
                    ary_suna = h5File_R.get('/SolarAzimuth')[:]
                    ary_lon = h5File_R.get('/Longitude')[:]
                    ary_lat = h5File_R.get('/Latitude')[:]
                    ary_LandCover = h5File_R.get('/LandCover')[:]
                    ary_LandSeaMask = h5File_R.get('/LandSeaMask')[:]
            except Exception as e:
                print str(e)
                return
            #################### 读取OBC文件 ####################
            try:
                with h5py.File(obcFile, 'r') as h5File_R:
                    ary_sv = h5File_R.get('Space_View')[:]
            except Exception as e:
                print (str(e))
                return

        # 通道的中心波数和光谱响应
        for i in xrange(self.Band):
            BandName = 'CH_%02d' % (i + 1)
            srfFile = os.path.join(
                MainPath, 'SRF', '%s_%s_SRF_CH%02d_Pub.txt' % (self.sat, self.sensor, (i + 1)))
            dictWave = np.loadtxt(
                srfFile, dtype={'names': ('num', 'rad'), 'formats': ('f4', 'f4')})
            if BandName in ['CH_03', 'CH_04', 'CH_05']:
                waveNum = 10 ** 4 / dictWave['num'][::-1]
            else:
                waveNum = 10 ** 7 / dictWave['num'][::-1]
            waveRad = dictWave['rad'][::-1]
            self.waveNum[BandName] = waveNum
            self.waveRad[BandName] = waveRad
        ############### 数据大小 使用经度维度 ###############
        dshape = ary_lon.shape

        ##################可见光数据进行定标 ########
#         LutAry = np.loadtxt(self.LutFile, dtype={'names': ('TBB', 'CH_03', 'CH_04', 'CH_05'),
#                             'formats': ('i4', 'f4', 'f4', 'f4')})
        # 定标系数b,a
        proj_Cal_Coeff = np.full((7, 2), np.nan)
        for i in range(7):
            for j in range(2):
                proj_Cal_Coeff[i, j] = ary_ref_cal[i * 2 + j - 1]

        # 1 - 6 通道的ref
        for i in xrange(self.Band):
            BandName = 'CH_%02d' % (i + 1)
            if i < 2 or i >= 5:
                # 下标k (1 2 6 7 8 9 10存放在一个三维数组中)
                if i < 2:
                    k = i
                elif i >= 5:
                    k = i - 3

                # DN值存放无效值用nan填充
                DN = np.full(dshape, np.nan)
                idx = np.logical_and(ary_ch7[k] < 32767, ary_ch7[k] > 0)
                DN[idx] = ary_ch7[k][idx]
                self.Dn[BandName] = DN

                # 反射率值存放无效值用nan填充
                Ref = (DN * proj_Cal_Coeff[k][1] + proj_Cal_Coeff[k][0]) / 100.
                self.Ref[BandName] = Ref

            if 2 <= i <= 4:
                # DN Rad Tbb 值存放,默认 nan填充
                DN = np.full(dshape, np.nan)
                Rad = np.full(dshape, np.nan)

                condition = np.logical_and(ary_ch3[k] < 32767, ary_ch3[k] > 0)
                idx = np.where(condition)
                k = i - 2
                # 下标i-2 (3,4,5通道DN存放在一个三维数组中)
                DN[idx] = ary_ch3[k][idx]
                self.Dn[BandName] = DN

                Rad[idx] = DN[idx] * ary_scales[idx[0], k] + \
                    ary_offsets[idx[0], k]
                k0 = ary_Nonlinear[k]
                k1 = ary_Nonlinear[3 + k]
                k2 = ary_Nonlinear[6 + k]
                Rad_nonlinearity = Rad ** 2 * k2 + Rad * k1 + k0
                self.Rad_non[BandName] = Rad_nonlinearity
                self.Rad[BandName] = Rad + Rad_nonlinearity

                Tbb = pb_sat.planck_r2t(
                    Rad, self.WN[BandName], self.TeA[BandName], self.TeB[BandName])
                self.Tbb[BandName] = Tbb
                k0 = ary_tb_coeff[k * 2 + 1]
                k1 = ary_tb_coeff[k * 2]
                Tbb_coeff = Tbb * k0 + k1
                self.Tbb_coeff[BandName] = Tbb_coeff

                # 亮温存放无效值用nan填充
#                 Tbb = np.full(ary_lon.shape, np.nan)
#                 Tbb[idx] = np.interp(Rad[idx], LutAry[BandName], LutAry['TBB'], 0, 0)
#                 self.Tbb[BandName] = Tbb

        ##################### 全局信息赋值 ############################
        # 对时间进行赋值合并
        Time = np.full(dshape, -999.)
        nameClass = pb_name.nameClassManager()
        info = nameClass.getInstance(iname)
        secs = int((info.dt_s - datetime(1970, 1, 1, 0, 0, 0)).total_seconds())
        Time[:] = secs
        if self.Time == []:
            self.Time = Time
        else:
            self.Time = np.concatenate((self.Time, Time))

        # SV, BB
        for i in xrange(self.Band):
            BandName = 'CH_%02d' % (i + 1)
            SV = np.full(dshape, np.nan)
            BB = np.full(dshape, np.nan)
            for j in xrange(dshape[0]):
                SV[j, :] = ary_sv[i, j, 0]

            if BandName not in self.SV.keys():
                self.SV[BandName] = SV
                self.BB[BandName] = BB
            else:
                self.SV[BandName] = np.concatenate((self.SV[BandName], SV))
                self.BB[BandName] = np.concatenate((self.BB[BandName], BB))

        # 土地覆盖
        ary_LandCover_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_LandCover >= 0, ary_LandCover <= 254)
        ary_LandCover_idx[condition] = ary_LandCover[condition]

        if self.LandCover == []:
            self.LandCover = ary_LandCover_idx
        else:
            self.LandCover = np.concatenate(
                (self.LandCover, ary_LandCover_idx))

        # 海陆掩码
        ary_LandSeaMask_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_LandSeaMask >= 0, ary_LandSeaMask <= 7)
        ary_LandSeaMask_idx[condition] = ary_LandSeaMask[condition]

        if self.LandSeaMask == []:
            self.LandSeaMask = ary_LandSeaMask_idx
        else:
            self.LandSeaMask = np.concatenate(
                (self.LandSeaMask, ary_LandSeaMask_idx))

        # 经纬度
        ary_lon_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_lon > -180., ary_lon < 180.)
        ary_lon_idx[condition] = ary_lon[condition]
        if self.Lons == []:
            self.Lons = ary_lon_idx
        else:
            self.Lons = np.concatenate((self.Lons, ary_lon_idx))

        ary_lat_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_lat > -90., ary_lat < 90.)
        ary_lat_idx[condition] = ary_lat[condition]
        if self.Lats == []:
            self.Lats = ary_lat_idx
        else:
            self.Lats = np.concatenate((self.Lats, ary_lat_idx))

        # 卫星方位角 天顶角
        ary_sata_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_sata > -18000, ary_sata < 18000)
        ary_sata_idx[condition] = ary_sata[condition]

        if self.satAzimuth == []:
            self.satAzimuth = ary_sata_idx / 100.
        else:
            self.satAzimuth = np.concatenate(
                (self.satAzimuth, ary_sata_idx / 100.))

        ary_satz_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_satz > 0, ary_satz < 18000)
        ary_satz_idx[condition] = ary_satz[condition]
        if self.satZenith == []:
            self.satZenith = ary_satz_idx / 100.
        else:
            self.satZenith = np.concatenate(
                (self.satZenith, ary_satz_idx / 100.))

        # 太阳方位角 天顶角
        ary_suna_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_suna > -18000, ary_suna < 18000)
        ary_suna_idx[condition] = ary_suna[condition]

        if self.sunAzimuth == []:
            self.sunAzimuth = ary_suna_idx / 100.
        else:
            self.sunAzimuth = np.concatenate(
                (self.sunAzimuth, ary_suna_idx / 100.))

        ary_sunz_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_sunz > 0, ary_sunz < 18000)
        ary_sunz_idx[condition] = ary_sunz[condition]

        if self.sunZenith == []:
            self.sunZenith = ary_sunz_idx / 100.
        else:
            self.sunZenith = np.concatenate(
                (self.sunZenith, ary_sunz_idx / 100.))


if __name__ == '__main__':
    T1 = datetime.now()

    L1File = r'D:\nsmc\fix_data\FY3ABC\FY3C_VIRRX_GBAL_L1_20150101_0030_1000M_MS.HDF'
    virr = CLASS_VIRR_L1()
    virr.Load(L1File)
    T2 = datetime.now()
    print np.nanmean(virr.Rad['CH_03'])
    print np.nanstd(virr.Rad['CH_03'])
    print np.nanmax(virr.Rad['CH_03'])
    print np.nanmin(virr.Rad['CH_03'])
    print np.nanmean(virr.Rad_non['CH_03'])
    print np.nanstd(virr.Rad_non['CH_03'])
    print np.nanmax(virr.Rad_non['CH_03'])
    print np.nanmin(virr.Rad_non['CH_03'])
