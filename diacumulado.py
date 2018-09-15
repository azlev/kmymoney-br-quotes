#!/usr/bin/python3

# http://www.b3.com.br/pt_br/market-data-e-indices/indices/indices-de-segmentos-e-setoriais/metodologia-do-di.htm

import argparse
import datetime
import decimal
import ftplib
import io
import logging
import sqlite3
import sys
import unittest


from datetime import date
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
    decimal.getcontext().rounding = decimal.ROUND_HALF_EVEN
    return round(ret, 8)


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

    def test_dib3_two_days(self):
        ret = dib3([Decimal('6.89')] * 2, Decimal('100.0000'))
        self.assertEqual(ret, Decimal('1.00052895'))

    def test_dib3_january_2018(self):
        ret = dib3([Decimal('6.89')] * 22, Decimal('100.0000'))
        self.assertEqual(ret, Decimal('1.00583386'))

    def test_dib3_2018_03_22(self):
        ret = dib3([Decimal('6.89')] * 122, Decimal('100.0000'))
        self.assertEqual(ret, Decimal('1.03044173'))


# menor data encontrada no FTP da CETIP
def mindate() -> date:
    return date(2012, 8, 20)


def daterange(start: date, end: date):
    d = start
    oneday = datetime.timedelta(days=1)
    while d < end:
        yield d
        d += oneday


def setupdb(filename: str):
    sqlite3.register_adapter(Decimal, lambda d: str(d))
    sqlite3.register_converter("numeric", lambda n: Decimal(n.decode('utf-8')))

    params = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES 
    conn = sqlite3.connect(filename, detect_types=params)

    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS di (
        id DATE PRIMARY KEY,
        tax NUMERIC(9,2) NOT NULL)""")
    return conn


def cachemindate(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM di ORDER BY id DESC LIMIT 1")
    ret = cursor.fetchone()
    if ret:
        return ret[0]
    else:
        return mindate() - datetime.timedelta(days=1)

def makecache(conn, start: date, end: date):
    ftp = ftplib.FTP('ftp.cetip.com.br')
    ftp.login()
    ftp.cwd('MediaCDI')
    cursor = conn.cursor()
    for d in daterange(start, end):
        weekday = d.weekday()
        if weekday == 5 or weekday == 6:
            continue
        contents = io.BytesIO()
        fname = d.strftime('%Y%m%d') + '.txt'
        try:
            ftp.retrbinary("RETR " + fname, contents.write)
        except ftplib.error_perm as e:
            logging.debug("Feriado:", d)
            continue
        contents.seek(0)
        strtax = contents.read(9)
        tax = Decimal(strtax.decode("utf-8"))
        tax = tax / 100
        cursor.execute("INSERT INTO di VALUES (?, ?)", (d, tax))
    conn.commit()


def getquotes(conn, start: date, end: date) -> List[Decimal]:
    cursor = conn.cursor()
    cursor.execute("""SELECT tax
                        FROM di
                       WHERE id BETWEEN ? AND DATE(?, '-1 day')
                       ORDER BY id""",
            (start, end))
    return [x[0] for x in cursor]


def main(start: date, end: date, p: Decimal):
    if start < mindate():
        print("Data inicial nao pode ser menor que {}".format(mindate()))
        sys.exit(1)

    maxdate = datetime.date.today() - datetime.timedelta(days=1)
    if end > maxdate:
        print("Data final nao pode ser maior que {}".format(maxdate))
        sys.exit(1)

    conn = setupdb("di.sqlite3")
    cmindate = cachemindate(conn)
    if cmindate < (end - datetime.timedelta(days=1)):
        makecache(conn, cmindate + datetime.timedelta(days=1), end)

    quotes = getquotes(conn, start, end)

    ret = dib3(quotes, p)
    print(ret)
    # mindate(), date(2018, 9, 14), Decimal(100)
    # print("1,82143297")

def getparser():
    parser = argparse.ArgumentParser(description="Calculo de DI acumulado entre datas")
    parser.add_argument('--inicial', type=str, default='2012-08-20')
    final_default = date.today() - datetime.timedelta(days=1)
    parser.add_argument('--final', type=str, default=final_default.strftime('%Y-%m-%d'))
    parser.add_argument('--porcentagem', type=str, default='100')
    return parser

if __name__ == '__main__':
    parser = getparser()
    args = parser.parse_args()
    start = datetime.datetime.strptime(args.inicial, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(args.final, '%Y-%m-%d').date()
    p = Decimal(args.porcentagem)
    main(start, end, p)

