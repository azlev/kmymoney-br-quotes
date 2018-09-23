#!/usr/bin/python3

import argparse
import datetime
import decimal
import ftplib
import io
import logging
import pandas
import requests
import sqlite3
import sys
import unittest
import tempfile
import warnings


from datetime import date
from decimal import Decimal
from html.parser import HTMLParser
from typing import List


# http://www.b3.com.br/pt_br/market-data-e-indices/indices/indices-de-segmentos-e-setoriais/metodologia-do-di.htm
def dib3(taxas: List[Decimal], p: Decimal) -> Decimal:
    if not taxas:
        return Decimal(0)


    decimal.getcontext().rounding = decimal.ROUND_FLOOR
    p = round(p, 4)
    ret = Decimal(1)
    for taxa in taxas:
        di = tdik(taxa)
        c = Decimal(1) + di * (p / Decimal(100))
        decimal.getcontext().rounding = decimal.ROUND_FLOOR
        c = round(c, 16)
        ret *= c
        ret = round(ret, 16)
    decimal.getcontext().rounding = decimal.ROUND_HALF_EVEN
    return round(ret, 8)


def tdik(di: Decimal) -> Decimal:
    ret = (di / Decimal(100) + 1)**(Decimal(1) / Decimal(252)) - 1
    decimal.getcontext().rounding = decimal.ROUND_HALF_EVEN
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

    def test_dib3_2018_01_01__2018_03_01(self):
        taxas = ([Decimal('6.64')] * 13) + ([Decimal('6.89')] * 27)
        self.assertEqual(len(taxas), 40)
        ret = dib3(taxas, Decimal(100))
        self.assertEqual(ret, Decimal('1.01051031'))

    def test_dib3_2018_03_22__2018_09_14(self):
        ret = dib3([Decimal('6.39')] * 122, Decimal('100.0000'))
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


def cachemaxdate(conn):
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

def getdays(conn, start: date, end: date) -> int:
    cursor = conn.cursor()
    cursor.execute("""SELECT COUNT(*)
                        FROM di
                       WHERE id BETWEEN ? AND ?""",
            (start, end))
    return cursor.fetchone()[0]


def maindi(start: date, end: date, p: Decimal, cachefile: str):
    if start < mindate():
        print("Data inicial nao pode ser menor que {}".format(mindate()))
        sys.exit(1)

    maxdate = datetime.date.today()
    if end > maxdate:
        print("Data final nao pode ser maior que {}".format(maxdate))
        sys.exit(1)

    conn = setupdb(cachefile)
    cmaxdate = cachemaxdate(conn)
    if cmaxdate < (end - datetime.timedelta(days=1)):
        makecache(conn, cmaxdate + datetime.timedelta(days=1), end)

    quotes = getquotes(conn, start, end)

    ret = dib3(quotes, p)
    return '"{}","{}"'.format(end.strftime("%Y-%m-%d"), ret)


def mainpre(start: date, end: date, p: Decimal, cachefile: str):
    conn = setupdb(cachefile)
    dias = getdays(conn, start, end)
    ret = (Decimal(1) + (p / Decimal(100))) ** (Decimal(dias) / Decimal(252))
    return '"{}","{}"'.format(end.strftime("%Y-%m-%d"), ret)


def maintd(data: date, titulo: str, cachefile: str):
    sigla, vencimento = titulo.split('_')

    class MyHTMLParser(HTMLParser):

        def __init__(self):
            HTMLParser.__init__(self)
            self.tables = {}
            self.current_table = None
            self.href = None
            end = datetime.date.today().year
            for i in range(2002, end + 1):
                k = "{} - ".format(i)
                self.tables[k] = {}

        def handle_starttag(self, tag, attrs):
            if self.current_table is not None and tag == 'a':
                for k, v in attrs:
                    if k == 'href':
                        self.href = v
                        break

        def handle_data(self, data):
            if data in self.tables.keys():
                self.current_table = self.tables[data]
            elif self.href:
                self.current_table[data] = self.href
                self.href = None

        def geturls(self):
            baseurl = "https://sisweb.tesouro.gov.br/apex/"
            years = sorted(self.tables.keys())
            for y in years:
                year = int(y[0:3])
                tabela = self.tables[y]
                for titulo, path in tabela.items():
                    yield year, titulo, baseurl + path


    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # indice
        r = requests.get("https://sisweb.tesouro.gov.br/apex/f?p=2031:2:::::", verify=False)
        parser = MyHTMLParser()
        parser.feed(r.content.decode('utf-8'))
        for ano, s, path in parser.geturls():
            if ano != data.year:
                continue
            if s == sigla:
                break
        r = requests.get(path, verify=False)

    tf = tempfile.NamedTemporaryFile()
    tf.file.write(r.content)
    xls = pandas.ExcelFile(tf.name)

    xls.sheet_names

    pd = xls.parse('{} {}'.format(sigla, vencimento), header=1)

    for index, row in pd.iterrows():
        d = datetime.datetime.strptime(row[0], '%d/%m/%Y').date()
        if d == data:
            break

    return '"{}","{}"'.format(data.strftime("%Y-%m-%d"), row[5])


def getparser():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--cachefile', type=str, default='di.sqlite3')

    final_default = date.today() - datetime.timedelta(days=1)

    subparsers = parser.add_subparsers(dest='command')
    parser_di = subparsers.add_parser("DI", help="Calculo de DI acumulado entre datas")
    parser_di.add_argument('--inicial', type=str, default='2012-08-20')
    parser_di.add_argument('--final', type=str, default=final_default.strftime('%Y-%m-%d'))
    parser_di.add_argument('--porcentagem', type=str, default='100')

    parser_td = subparsers.add_parser("TD", help="Tesouro Direto")
    parser_td.add_argument('--titulo', type=str, required=True, help="Nome_vencimento do titulo, (exemplo: 'LFT_010323')")
    parser_td.add_argument('--data', type=str, default=final_default.strftime('%Y-%m-%d'))

    parser_di = subparsers.add_parser("PRE", help="Calculo de Titulo PRE entre datas")
    parser_di.add_argument('--inicial', type=str, required=True)
    parser_di.add_argument('--final', type=str, default=final_default.strftime('%Y-%m-%d'))
    parser_di.add_argument('--porcentagem', type=str, required=True)
    
    return parser

if __name__ == '__main__':
    parser = getparser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    ret = None

    if args.command == 'DI':
        start = datetime.datetime.strptime(args.inicial, '%Y-%m-%d').date()
        end = datetime.datetime.strptime(args.final, '%Y-%m-%d').date()
        p = Decimal(args.porcentagem)
        ret = maindi(start, end, p, args.cachefile)

    if args.command == 'TD':
        data = datetime.datetime.strptime(args.data, '%Y-%m-%d').date()
        ret = maintd(data, args.titulo, args.cachefile)

    if args.command == 'PRE':
        inicial = datetime.datetime.strptime(args.inicial, '%Y-%m-%d').date()
        final = datetime.datetime.strptime(args.final, '%Y-%m-%d').date()
        p = Decimal(args.porcentagem)
        ret = mainpre(inicial, final, p, args.cachefile)

    print(ret)
