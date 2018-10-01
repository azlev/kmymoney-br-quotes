#!/usr/bin/python3

import argparse
import datetime
import decimal
import ftplib
import io
import logging
import os
import pandas
import requests
import sqlite3
import subprocess
import sys
import unittest
import tempfile
import warnings


from datetime import date
from decimal import Decimal
from html.parser import HTMLParser
from typing import List
from typing import Tuple


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
        preco NUMERIC(9,2) NOT NULL)""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS td (
        titulo VARCHAR(10) NOT NULL,
        prazo  VARCHAR(6) NOT NULL,
        data DATE NOT NULL,
        preco NUMERIC(9,2) NOT NULL,
        PRIMARY KEY (titulo, prazo, data))""")
    return conn


def cachemaxdate(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM di ORDER BY id DESC LIMIT 1")
    ret = cursor.fetchone()
    if ret:
        return ret[0]
    else:
        return mindate() - datetime.timedelta(days=1)


def makecachedi(conn: sqlite3.Connection, start: date, end: date):
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
        preco = Decimal(strtax.decode("utf-8"))
        preco = preco / 100
        cursor.execute("INSERT INTO di VALUES (?, ?)", (d, preco))
    conn.commit()


def maindi(start: date, end: date, p: Decimal, conn: sqlite3.Connection):
    if start < mindate():
        print("Data inicial nao pode ser menor que {}".format(mindate()))
        sys.exit(1)

    maxdate = datetime.date.today()
    if end > maxdate:
        print("Data final nao pode ser maior que {}".format(maxdate))
        sys.exit(1)

    def getquotes(conn, start: date, end: date) -> List[Decimal]:
        cursor = conn.cursor()
        cursor.execute("""SELECT preco
                            FROM di
                           WHERE id BETWEEN ? AND DATE(?, '-1 day')
                           ORDER BY id""",
                (start, end))
        return [x[0] for x in cursor]

    quotes = getquotes(conn, start, end)

    ret = dib3(quotes, p)
    return '"{}","{}"'.format(end.strftime("%Y-%m-%d"), ret)


def mainpre(start: date, end: date, p: Decimal, conn: sqlite3.Connection):
    def getdays(conn: sqlite3.Connection, start: date, end: date) -> int:
        cursor = conn.cursor()
        cursor.execute("""SELECT COUNT(*)
                            FROM di
                           WHERE id BETWEEN ? AND ?""",
                (start, end))
        return cursor.fetchone()[0]

    dias = getdays(conn, start, end)
    ret = (Decimal(1) + (p / Decimal(100))) ** (Decimal(dias) / Decimal(252))
    return '"{}","{}"'.format(end.strftime("%Y-%m-%d"), ret)

def normalizatitulo(titulo: str) -> str:
    titulo = titulo.replace('-', '')
    titulo = titulo.replace('Principal', 'P')
    titulo = titulo.replace('Princ', 'P')
    titulo = titulo.replace(' ', '')
    return titulo


class TestNormaliza(unittest.TestCase):
    def test_normaliza(self):
        self.assertEqual(normalizatitulo("LFT"), "LFT")
        self.assertEqual(normalizatitulo("LTN"), "LTN")
        self.assertEqual(normalizatitulo("NTN-B"), "NTNB")
        self.assertEqual(normalizatitulo("NTN-B Princ"), "NTNBP")
        self.assertEqual(normalizatitulo("NTN-B Principal"), "NTNBP")
        self.assertEqual(normalizatitulo("NTN-C"), "NTNC")
        self.assertEqual(normalizatitulo("NTNBP"), "NTNBP")
        self.assertEqual(normalizatitulo("NTNC"), "NTNC")
        self.assertEqual(normalizatitulo("NTNF"), "NTNF")


def maintd(data: date, titulo: str, prazo: str, conn: sqlite3.Connection):
    def maketdcache(conn: sqlite3.Connection, inicio: date):
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
                    year = int(y[0:4])
                    tabela = self.tables[y]
                    for titulo, path in tabela.items():
                        yield year, titulo, baseurl + path


        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # indice
            r = requests.get("https://sisweb.tesouro.gov.br/apex/f?p=2031:2:::::", verify=False)

        parser = MyHTMLParser()
        parser.feed(r.content.decode('utf-8'))

        cursor = conn.cursor()
        for ano, s, path in parser.geturls():
            if ano < inicio.year:
                continue

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                r = requests.get(path, verify=False)

            tf = tempfile.NamedTemporaryFile()
            tf.file.write(r.content)
            xls = pandas.ExcelFile(tf.name)
            for sheet in xls.sheet_names:
                t = sheet[:-7]
                t = normalizatitulo(t)
                p = sheet[-6:]
                pd = xls.parse(sheet, header=1)
                for _, row in pd.iterrows():
                    d = pandas.to_datetime(row[0], dayfirst=True).date()
                    if d <= inicio:
                        continue
                    preco = row[-1]
                    if pandas.isnull(preco):
                        continue
                    preco = Decimal(row[-1])
                    insert_tuple = (t, p, d, preco) 
                    #print("INSERT: %s" % (insert_tuple,))
                    cursor.execute("INSERT INTO td VALUES (?, ?, ?, ?)", insert_tuple)
        conn.commit()

    def gettdpreco(conn: sqlite3.Connection, titulo: str, prazo: str, data: date) -> Tuple[date, Decimal]:
        cursor = conn.cursor()
        cursor.execute("""SELECT data, preco
                            FROM td
                           WHERE titulo = ?
                             AND prazo = ?
                             AND data <= ?
                           ORDER BY data DESC
                           LIMIT 1""", (titulo, prazo, data))
        ret = cursor.fetchone()
        return ret

    titulo = normalizatitulo(titulo)

    cursor = conn.cursor()
    ret = cursor.execute("SELECT data FROM td ORDER BY data DESC LIMIT 1")
    datecache = ret.fetchone()
    if datecache is None:
        datecache = date(2002, 1, 1)
    else:
        datecache = datecache[0]
    cursor.close()

    if datecache < data:
        maketdcache(conn, datecache)
 
    ret = gettdpreco(conn, titulo, prazo, data)
    if not ret:
        return "Erro: titulo='{}' prazo='{}' nao encontrados".format(titulo, prazo)

    return '"{}","{}"'.format(ret[0].strftime("%Y-%m-%d"), ret[1])


def register(cachefile: str):
    ret = subprocess.check_output(["kf5-config", "--path", "config"])
    head, *tail = ret[:-1].split(b':')

    config = os.path.join(head, b'kmymoney', b'kmymoneyrc')

    if not cachefile[0] == '/':
        cachefile = os.path.join(os.path.abspath('.'), cachefile)

    with open(config, 'a') as f:
        f.writelines([
                "\n",
                "[Online-Quote-Source-Brazilian Quotes]\n",
                "CSVURL=\n"
                "DateFormatRegex=%y-%m-%d\n",
                'DateRegex="(\\\\d{4,4}-\\\\d{2,2}-\\\\d{2,2})",".*"\n',
                "IDBy=0\n",
                "IDRegex=\n",
                'PriceRegex="\\\\d{4,4}-\\\\d{2,2}-\\\\d{2,2}","(.*)"\n',
                "URL=file://{} --cachefile={} %1\n".
                        format(os.path.abspath(sys.argv[0]), cachefile),
                "\n"])


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
    parser_td.add_argument('--titulo', type=str, required=True, help="LFT, NTN-B Princ, NTN-F, ...")
    parser_td.add_argument('--prazo', type=str, required=True, help="010129")
    parser_td.add_argument('--data', type=str, default=final_default.strftime('%Y-%m-%d'))

    parser_di = subparsers.add_parser("PRE", help="Calculo de Titulo PRE entre datas")
    parser_di.add_argument('--inicial', type=str, required=True)
    parser_di.add_argument('--final', type=str, default=final_default.strftime('%Y-%m-%d'))
    parser_di.add_argument('--porcentagem', type=str, required=True)

    parser_register = subparsers.add_parser("REGISTER",
            help="Registra o nome 'Brazilian Quotes' no kmymoney")
    
    return parser

if __name__ == '__main__':
    parser = getparser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == 'REGISTER':
        register(args.cachefile)
        sys.exit(0)

    ret = None

    conn = setupdb(args.cachefile)

    if args.command in ['DI', 'PRE']:
        inicial = datetime.datetime.strptime(args.inicial, '%Y-%m-%d').date()
        final = datetime.datetime.strptime(args.final, '%Y-%m-%d').date()
        p = Decimal(args.porcentagem)


        cmaxdate = cachemaxdate(conn)
        if cmaxdate < (final - datetime.timedelta(days=1)):
            makecachedi(conn, cmaxdate + datetime.timedelta(days=1), final)

        if args.command == 'DI':
            ret = maindi(inicial, final, p, conn)
        else:
            ret = mainpre(inicial, final, p, conn)

    elif args.command == 'TD':
        data = datetime.datetime.strptime(args.data, '%Y-%m-%d').date()
        ret = maintd(data, args.titulo, args.prazo, conn)

    print(ret)

