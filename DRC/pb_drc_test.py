# -*- coding: utf-8 -*-

import os
import sys

from pb_drc_CRIS import ReadCrisL1
from pb_drc_IASI import ReadIasiL1
from pb_drc_MERSI import ReadMersiL1
import numpy as np


__description__ = u'MERSI传感器读取类'
__author__ = 'wangpeng'
__date__ = '2018-08-28'
__version__ = '1.0.0_beat'


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


def fun2222222():
    print(sys._getframe().f_code.co_name)


def fun1(sensor):
    print(sys._getframe().f_code.co_name)
    idx = np.where(sensor > 0)
    sensor[idx] = -1
    pass
if __name__ == '__main__':
    #     L1File1 = 'D:/data/FY3D+MERSI_HIRAS/FY3D_MERSI_GBAL_L1_20180326_0045_1000M_MS.HDF'
    #     mersi = ReadMersiL1(L1File1)
    #
    #     L1File2 = 'D:/data/METOP/IASI_xxx_1C_M02_20180502060857Z_20180502061152Z_N_O_20180502072426Z__20180502072755'
    #     iasi1 = ReadIasiL1(L1File2)
    #
    #     L1File3 = 'D:/data/NPP_CRIS/GCRSO-SCRIF-SCRIS_npp_d20180303_t0016319_e0024297_b32881_c20180308030857410779_noac_ops.h5'
    #     cris = ReadCrisL1(L1File3)
    #
    #     a = mersi.get_tbb_k0()
    #     b = mersi.get_tbb_k1()
    #     print a.keys()
    #     central_wave_numbers = mersi.get_central_wave_number()
    #     wave_nums, wave_spec = mersi.get_spectral_response()
    #     rad = cris.get_rad(wave_nums, wave_spec, central_wave_numbers.keys())
    #     tbb = cris.get_tbb(wave_nums, wave_spec, central_wave_numbers, a, b)
    #     print rad
    #     print tbb
    #
    #     rad = 0.0003790
    #
    #     c1 = 1.1910439e-16  # 1.19104*10-5 mW/m2.sr.cm-1
    #     c2 = 1.438769e-2  # 1.43877 K/cm-1
    #     vs = 1.0E+2 * 2.647409E+03
    #     tbb = c2 * vs / np.log(c1 * vs ** 3 / (1.0E-5 * rad) + 1.0)
    #     tbb = tbb * 9.993363E-01 + 4.818401E-01
    #     print c1, c2, vs, tbb
    #
    #     h = 6.62606876e-34  # Planck constant (Joule second)
    #     c = 2.99792458e+8
    #     k = 1.3806503e-23
    #
    #     c1 = 2.0 * h * c * c
    #     c2 = (h * c) / k
    #     w = 1.0e+4 / 2.647409E+03
    #     ws = 1.0e-6 * w
    #
    #     tt = c2 / (ws * np.log(c1 / (1.0e+6 * rad * ws ** 5) + 1.0))
    #     tt = (tt - 4.818401E-01) / 9.993363E-01
    #     print c1, c2, ws, tt
    #
    #     a = np.array([1, 1.0E-5 * 0.005, np.nan, 1, 1, 1, 1, 1])
    #     idx = np.logical_and(True, a > 0)
    #     ddd = np.where(idx)
    #     print ddd.size()
    #     b = np.array([1, 2.25857044237, np.nan])
    #     print np.log(b / a + 1)
    #     print 3836.71637415 / np.log(b / a + 1)
    #
    #     print 222 / -(np.inf)
    #     try:
    #         cc = 2.25857044237 / (1.0E-5 * 0.)
    #         print cc
    #     except Exception as e:
    #         print e

    #     print np.log(2.25857044237 / (1.0E-5 * 0.))
    #     print '12', 3836.71637415 / np.log(2.25857044237 / (1.0E-5 * 0.) +1)
    #     print np.logical_and(c > 0, True)
    #     print np.logical_and(, c > 0)
    #
    #     dict1 = {'a': 1, 'b': 2}
    #
    #     dict1["c"] = dict1.pop("a")
    #     print dict1
    fun2222222()
#     sensor = np.full((3, 4), 9)
#     print sensor
#     fun1(sensor)
#     print sensor
