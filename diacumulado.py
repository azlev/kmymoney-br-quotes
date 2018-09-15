#!/usr/bin/python3

# http://www.b3.com.br/pt_br/market-data-e-indices/indices/indices-de-segmentos-e-setoriais/metodologia-do-di.htm

import decimal
import unittest

from decimal import Decimal
from typing import List


def dib3(taxas: List[Decimal], p: Decimal) -> Decimal:
    if not taxas:
        return Decimal(0)

    ret = Decimal(1)
    for taxa in taxas:
        di = tdik(taxa)
        c = Decimal(1) + di * (p / Decimal(100))
        ret *= c
    return ret


def tdik(di: Decimal) -> Decimal:
    decimal.getcontext().rounding = decimal.ROUND_FLOOR
    ret = (di / Decimal(100) + 1)**(Decimal(1) / Decimal(252)) - 1
    return round(ret, 8)


class TestDI(unittest.TestCase):

    def test_tdik_one_day(self):
        ret = tdik(Decimal('6.89'))
        self.assertEqual(ret, Decimal('0.00026444'))

    def test_dib3_zero_days(self):
        ret = dib3([], Decimal('100.0000'))
        self.assertEqual(ret, Decimal(0))
        
    def test_dib3_one_day(self):
        ret = dib3([Decimal('6.89')], Decimal('100.0000'))
        self.assertEqual(ret, Decimal('1.00026444'))

