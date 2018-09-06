# -*- coding: utf-8 -*-

import os
import sys

import numpy as np

# from pb_drc_IASI import ReadIasiL1
# from pb_drc_MERSI import ReadMersiL1


__description__ = u'MERSI传感器读取类'
__author__ = 'wangpeng'
__date__ = '2018-08-28'
__version__ = '1.0.0_beat'


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


if __name__ == '__main__':

    #     L1File1111 = 'D:/data/METOP/IASI_xxx_1C_M02_20180502060857Z_20180502061152Z_N_O_20180502072426Z__20180502072755'
    #     iasi1 = ReadIasiL1(L1File1111)
    #     L1File22 = 'D:/data/FY3D+MERSI_HIRAS/FY3D_MERSI_GBAL_L1_20180326_0045_1000M_MS.HDF'
    #     mersi = ReadMersiL1(L1File22)
    #     a = mersi.get_tbb_k0()
    #     b = mersi.get_tbb_k1()
    #     print a.keys()
    #     central_wave_numbers = mersi.get_central_wave_number()
    #     wave_nums, wave_spec = mersi.get_spectral_response()
    # #     rad = iasi1.get_rad(wave_nums, wave_spec)
    #     tbb = iasi1.get_tbb(wave_nums, wave_spec, central_wave_numbers, a, b)

    rad = 0.0003790

    c1 = 1.1910439e-16  # 1.19104*10-5 mW/m2.sr.cm-1
    c2 = 1.438769e-2  # 1.43877 K/cm-1
    vs = 1.0E+2 * 2.647409E+03
    tbb = c2 * vs / np.log(c1 * vs ** 3 / (1.0E-5 * rad) + 1.0)
    tbb = tbb * 9.993363E-01 + 4.818401E-01
    print c1, c2, vs, tbb

    h = 6.62606876e-34  # Planck constant (Joule second)
    c = 2.99792458e+8
    k = 1.3806503e-23

    c1 = 2.0 * h * c * c
    c2 = (h * c) / k
    w = 1.0e+4 / 2.647409E+03
    ws = 1.0e-6 * w

    tt = c2 / (ws * np.log(c1 / (1.0e+6 * rad * ws ** 5) + 1.0))
    tt = (tt - 4.818401E-01) / 9.993363E-01
    print c1, c2, ws, tt
