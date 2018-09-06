# coding: utf-8
from datetime import datetime

import numpy as np


__author__ = 'wangpeng'

'''
FileName:     pb_sat.py
Description:  配置文件处理的函数
Author:       wangpeng
Date:         2018-04-28
version:      1.0
'''


def solar_zen(yy, mm, dd, hh, xlon, xlat):
    '''
    calculate solar zenith
    '''
    stime = datetime.strptime('%s%s%s' % (yy, mm, dd), '%Y%m%d')
    xj = int(stime.strftime('%j'))
    # xj = getJulianDay(yy, mm, dd)

    tsm = hh + xlon / 15.  # GMT(Hour)-->local time(Hour)
    fac = np.pi / 180.

#     xlo=xlon.*fac #degree to rad
    xla = xlat * fac

    a1 = (1.00554 * xj - 6.28306) * fac
    a2 = (1.93946 * xj + 23.35089) * fac
    et = -7.67825 * np.sin(a1) - 10.09176 * np.sin(a2)
    tsv = tsm + et / 60. - 12.

    ah = tsv * 15. * fac

    a3 = (0.9683 * xj - 78.00878) * fac
    delta = 23.4856 * np.sin(a3) * fac

    amuzero = np.sin(xla) * np.sin(delta) + np.cos(xla) * \
        np.cos(delta) * np.cos(ah)

    elev = np.arcsin(amuzero) * 180. / np.pi

    asol = 90. - elev
    return asol


def getasol6s(ymd, hms, lons, lats):
    '''
    Function:    getasol6s
    Description: 计算太阳天顶角
    author:      陈林提供C源码( wangpeng转)
    date:        2017-03-22
    Input:       ymd hms : 20180101 030400 
    Output:      
    Return:      太阳天顶角弧度类型(修改为度类型 2018年4月28日)
    Others: 
    '''
    # jays(儒略日,当年的第几天), GMT(世界时 小时浮点计数方式 )
    dtime = datetime.strptime('%s %s' % (ymd, hms), '%Y%m%d %H%M%S')
    jday = int(dtime.strftime('%j'))
#     print jday
    GMT = float(hms[0:2]) + float(hms[2:4]) / 60.
#     print GMT
    lats = lats * np.pi / 180.
#     lons = lons * np.pi / 180.
    b1 = 0.006918
    b2 = 0.399912
    b3 = 0.070257
    b4 = 0.006758
    b5 = 0.000907
    b6 = 0.002697
    b7 = 0.001480

    a1 = 0.000075
    a2 = 0.001868
    a3 = 0.032077
    a4 = 0.014615
    a5 = 0.040849
    A = 2 * np.pi * jday / 365.0
    delta = b1 - b2 * np.cos(A) + b3 * np.sin(A) - b4 * np.cos(2 * A) + \
        b5 * np.sin(2 * A) - b6 * np.cos(3 * A) + b7 * np.sin(3 * A)
    ET = 12 * (a1 + a2 * np.cos(A) - a3 * np.sin(A) - a4 *
               np.cos(2 * A) - a5 * np.sin(2 * A)) / np.pi
    MST = GMT + lons / 15.0
    TST = MST + ET
    t = 15.0 * np.pi / 180.0 * (TST - 12.0)

    asol = np.arccos(
        np.cos(delta) * np.cos(lats) * np.cos(t) + np.sin(delta) * np.sin(lats))
    return np.rad2deg(asol)


def sun_earth_dis_correction(ymd):
    '''
    Instantaneous distance between earth and sun correction factor ==(d0/d)^2
    ymd: yyyymmdd
    '''

    stime = datetime.strptime(ymd, '%Y%m%d')
    jjj = int(stime.strftime('%j'))
    OM = (0.9856 * (jjj - 4)) * np.pi / 180.
    dsol = 1. / ((1. - 0.01673 * np.cos(OM)) ** 2)
    return dsol


def sun_glint_cal(obs_a, obs_z, sun_a, sun_z):
    '''
    计算太阳耀斑角
    '''
    ti = np.deg2rad(sun_z)
    tv = np.deg2rad(obs_z)
    phi = np.deg2rad(sun_a - obs_a)
    cos_phi = np.cos(phi)

    cos_tv = np.cos(tv)
    cos_ti = np.cos(ti)
    sin_tv = np.sin(tv)
    sin_ti = np.sin(ti)
#     cos_res = cos_ti * cos_tv + sin_ti * sin_tv * cos_phi
    cos_res = cos_ti * cos_tv - sin_ti * sin_tv * cos_phi  # 徐寒冽修正
    v_arrayMin = np.vectorize(arrayMin)
#     Min = v_arrayMin(-cos_res, 1.0)
    Min = v_arrayMin(cos_res, 1.0)  # 徐寒冽修正

    v_arrayMax = np.vectorize(arrayMax)
    Max = v_arrayMax(Min, -1)
    res = np.arccos(Max)
    glint = np.rad2deg(res)

    return glint


def arrayMax(array_a, b):
    return max(array_a, b)


def arrayMin(array_a, b):
    return min(array_a, b)


def spec_interp(WaveNum1, WaveRad1, WaveNum2):
    '''
    IN, WaveNum1:原光谱的波数(cm-1)
    IN, WaveRad1:原光谱的响应值（0-1）
    IN, WaveNum2:目标光谱的波数
    Rturn, Wave_rad2：目标谱的响应值（0-1）
    '''

    # 插值插值，过滤掉最大值的千分之9的数据
    maxRad = np.max(WaveRad1)
    idx = np.where(WaveRad1 < maxRad * 0.009)
    WaveRad1[idx] = 0.
    WaveRad2 = np.interp(WaveNum2, WaveNum1, WaveRad1, 0, 0)
    return WaveRad2


def spec_convolution(WaveNum, WaveRad, RealRad):
    '''
    IN, WaveNum: 光谱的波数(cm-1)
    IN, WaveRad: 光谱的响应值（0-1）
    IN, RealRad: 光谱的真实响应值
    return , S 卷积后的响应值
    '''
    # RealRad的光谱信息必须和WaveNum,WaveRad的长度一致
    if WaveNum.shape[-1] != WaveRad.shape[-1] != RealRad.shape[-1]:
        print 'The spectral response length must be the same'
        return -1

    # 默认对最后一维进行卷积 numpy.trapz(y, x=None, dx=1.0, axis=-1)
    s1 = np.trapz(RealRad * WaveRad, WaveNum)
    s2 = np.trapz(WaveRad, WaveNum)
    S = (s1 / s2)
    return S


def planck_r2t(r, w):
    '''
    function radiance2tbb: convert radiance data into brightness temperature (i.e., equivalent blackbody temperature)
    r: spectral radiance data in w/m2/sr/um  单位(mW/(m2.cm-1.sr))
    w: wavelength in micro  Equiv Mid_wn (cm-1)  等效中心波数
    a: TbbCorr_Coeff A  (典型温度黑体辐亮度以及通道亮温修正系数)
    b: TbbCorr_Coeff B
    return: reture value, brightness temperature in K (absolute temperature)
    '''
    c1 = 1.1910439e-16  # 1.19104*10-5 mW/m2.sr.cm-1
    c2 = 1.438769e-2  # 1.43877 K/cm-1
    vs = 1.0E+2 * w
    tbb = c2 * vs / np.log(c1 * vs ** 3 / (1.0E-5 * r) + 1.0)

    return tbb

# def plank_iras_rad2tb(ch_num, Rad_array):
#     '''
#     plank for IRAS rad2tb
#     ch_num starts from 1
#     '''
#     c1 = 0.000011910659
#     c2 = 1.438833
#     ich = ch_num - 1
#     a1 = c2 * wn[ich]
#     a2 = np.log(1 + (c1 * wn[ich] ** 3) / Rad_array)
#     # a2 = np.log(1 + (c1 * wn[ich] ** 3) / np.array(Rad_array))
#     Tbb = a1 / a2
#     return Tbb


def plank_iras_tb2rad(T, W, a=None, b=None):
    '''
    plank for IRAS tb2rad
    T : TBB
    W : center wavenums

    '''
    c1 = 0.000011910659
    c2 = 1.438833
    a1 = c1 * W ** 3
    a2 = (np.exp(c2 * W / T) - 1.0)
    Rad = a1 / a2
    return Rad


def planck_t2r(T, W):
    '''
    plank for IRAS tb2rad
    T : TBB
    W : center wavenums

    '''
    c1 = 0.000011910659
    c2 = 1.438833
    a1 = c1 * W ** 3
    a2 = (np.exp(c2 * W / T) - 1.0)
    Rad = a1 / a2
    return Rad

if __name__ == '__main__':
    #     print solar_zen(2018, 3, 26, 00, 105.37498, 81.54135)
    #     print getasol6s('20180326', '004500', 105.37498, 81.54135)
    #     print sun_glint_cal(90, 90, -90, 90)
    #     print sun_glint_cal(359, 179, 359, 179)

    pass
