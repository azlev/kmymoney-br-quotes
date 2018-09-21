#!/usr/bin/python3

import argparse
import datetime
import decimal
import logging
import sys
import unittest


from datetime import date
from decimal import Decimal
from typing import List


def prefixado(dias: int, diasano: int, taxa: Decimal) -> Decimal:
    base = Decimal(1) + Decimal(taxa) / Decimal(100)
    logging.debug("base: %s", base)
    expoente = dias / Decimal(diasano)
    logging.debug("expoente: %s", expoente)
    ret = base ** expoente
    ret = round(ret, 6)
    return ret


class TestPre(unittest.TestCase):

    def test_pre360_1(self):
        dias = (date(2018, 9, 19) - date(2018, 4, 12)).days
        ret = prefixado(dias, 360, Decimal(12))
        self.assertEqual(ret, Decimal('1.051658'))

    def test_pre360_2(self):
        dias = (date(2018, 9, 19) - date(2018, 2, 27)).days
        ret = prefixado(dias, 360, Decimal(12))
        self.assertEqual(ret, Decimal('1.066422'))


def getparser():
    parser = argparse.ArgumentParser(description="Calculo de Pre-fixado acumulado entre datas")
    parser.add_argument('--inicial', type=str, required=True)
    final_default = date.today() - datetime.timedelta(days=1)
    parser.add_argument('--final', type=str, default=final_default.strftime('%Y-%m-%d'))
    parser.add_argument('--porcentagem', type=str, required=True)
    return parser

def main(inicio: date, fim: date, porcentagem: Decimal):
    dias = (fim - inicio).days
    ret = prefixado(dias, 360, porcentagem)
    print('"{}","{}"'.format(fim.strftime("%Y-%m-%d"), ret))

if __name__ == '__main__':
    parser = getparser()
    args = parser.parse_args()
    start = datetime.datetime.strptime(args.inicial, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(args.final, '%Y-%m-%d').date()
    p = Decimal(args.porcentagem)
    main(start, end, p)

