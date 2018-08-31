# -*- coding: utf-8 -*-

import os
import sys


from pb_drc_MERSI import ReadMersiL1
from pb_drc_IASI import ReadIasiL1


__description__ = u'MERSI传感器读取类'
__author__ = 'wangpeng'
__date__ = '2018-08-28'
__version__ = '1.0.0_beat'


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


if __name__ == '__main__':

    L1File1111 = 'D:/data/METOP/IASI_xxx_1C_M02_20180502060857Z_20180502061152Z_N_O_20180502072426Z__20180502072755'
    iasi1 = ReadIasiL1(L1File1111)
    L1File22 = 'D:/data/FY3D+MERSI_HIRAS/FY3D_MERSI_GBAL_L1_20180326_0045_1000M_MS.HDF'
    mersi = ReadMersiL1(L1File22)
    a = mersi.get_tbb_k0()
    b = mersi.get_tbb_k1()
    print a.keys()
    central_wave_numbers = mersi.get_central_wave_number()
    wave_nums, wave_spec = mersi.get_spectral_response()
#     rad = iasi1.get_rad(wave_nums, wave_spec)
    tbb = iasi1.get_tbb(wave_nums, wave_spec, central_wave_numbers, a, b)

    print tbb['CH_20'].shape
    print tbb['CH_20']
