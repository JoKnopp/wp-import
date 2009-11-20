# -*- coding: UTF-8 -*-

# © Copyright 2009 Wolodja Wentland. All Rights Reserved.

# This file is part of wp-import.
#
# wp-import is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# wp-import is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with wp-import. If not, see <http://www.gnu.org/licenses/>.

"""Tests for wp_import.utils
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import os
import re
from nose.tools import eq_

import wp_import.utils as wpi_utils

PREFIX = os.path.join(*os.path.split(os.path.dirname(__file__))[:-1])
TEST_DATA_DIR = os.path.join(PREFIX, 'test', 'data')
DOWNLOAD_DIR = os.path.join(TEST_DATA_DIR, 'download')


def test_find():
    eq_(list(wpi_utils.find('dewiki*redirect*.sql.gz', DOWNLOAD_DIR)),
        [os.path.join(DOWNLOAD_DIR, 'de', '20091023',
                      'dewiki-20091023-redirect.sql.gz')])
    eq_(list(wpi_utils.find('enwiki*langlinks*.sql.gz', DOWNLOAD_DIR)),
        [os.path.join(DOWNLOAD_DIR, 'en', '20091017',
                      'enwiki-20091017-langlinks.sql.gz')])


def test_dump_file_paths():
    fn_regex = r'(?P<language>\w+)wiki-(?P<date>\d{8})-(?P<table>[\w_-]+).*'
    file_paths = [
        os.path.join(DOWNLOAD_DIR, 'de', '20091023',
                     'dewiki-20091023-categorylinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'de', '20091023',
                     'dewiki-20091023-langlinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'de', '20091023',
                     'dewiki-20091023-pagelinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'de', '20091023',
                     'dewiki-20091023-redirect.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'en', '20091017',
                     'enwiki-20091017-categorylinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'en', '20091017',
                     'enwiki-20091017-langlinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'en', '20091017',
                     'enwiki-20091017-pagelinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'en', '20091017',
                     'enwiki-20091017-redirect.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'zh', '20091023',
                     'zhwiki-20091023-categorylinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'zh', '20091023',
                     'zhwiki-20091023-langlinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'zh', '20091023',
                     'zhwiki-20091023-pagelinks.sql.gz'),
        os.path.join(DOWNLOAD_DIR, 'zh', '20091023',
                     'zhwiki-20091023-redirect.sql.gz'),
    ]
    # test data download directory + path to a file to check it is not
    # included twice in the list of filenames
    search_paths = [
        DOWNLOAD_DIR,
        os.path.join(DOWNLOAD_DIR, 'zhwiki-20091023-pagelinks.sql.gz')]

    for (res, exp) in itertools.izip_longest(
        wpi_utils.dump_file_paths(fn_regex, *search_paths),
        file_paths, fillvalue='-'):

        eq_(res, exp)


def test_dump_info():

    fn_regex = r'(?P<language>[\w_]+)wiki-(?P<date>\d{8})-(?P<table>[\w_-]+).*'
    file_paths = [os.path.join(os.path.sep, 'path', 'to',
                               'dewiki-20091023-langlinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'dewiki-20091023-pagelinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'dewiki-20091023-pages-articles.xml.bz2'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'dewiki-20091023-redirect.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'enwiki-20091017-langlinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'enwiki-20091017-pagelinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'enwiki-20091017-pages-articles.xml.bz2'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'enwiki-20091017-redirect.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zhwiki-20091023-langlinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zhwiki-20091023-pagelinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zhwiki-20091023-pages-articles.xml.bz2'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zhwiki-20091023-redirect.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zh_classicalwiki-20091023-langlinks.sql.gz'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zh_classicalwiki-20091023-pagelinks.sql.gz'),
                  os.path.join(
                      os.path.sep, 'path', 'to',
                      'zh_classicalwiki-20091023-pages-articles.xml.bz2'),
                  os.path.join(os.path.sep, 'path', 'to',
                               'zh_classicalwiki-20091023-redirect.sql.gz')]

    dump_infos = wpi_utils.dump_info(file_paths, fn_regex)

    for (lang, date, table, filename) in [
        ('de', '20091023', 'langlinks', 'dewiki-20091023-langlinks.sql.gz'),
        ('de', '20091023', 'pagelinks', 'dewiki-20091023-pagelinks.sql.gz'),
        ('de', '20091023', 'pages-articles',
         'dewiki-20091023-pages-articles.xml.bz2'),
        ('de', '20091023', 'redirect', 'dewiki-20091023-redirect.sql.gz'),
        ('en', '20091017', 'langlinks', 'enwiki-20091017-langlinks.sql.gz'),
        ('en', '20091017', 'pagelinks', 'enwiki-20091017-pagelinks.sql.gz'),
        ('en', '20091017', 'pages-articles',
         'enwiki-20091017-pages-articles.xml.bz2'),
        ('en', '20091017', 'redirect', 'enwiki-20091017-redirect.sql.gz'),
        ('zh', '20091023', 'langlinks', 'zhwiki-20091023-langlinks.sql.gz'),
        ('zh', '20091023', 'pagelinks', 'zhwiki-20091023-pagelinks.sql.gz'),
        ('zh', '20091023', 'pages-articles',
         'zhwiki-20091023-pages-articles.xml.bz2'),
        ('zh', '20091023', 'redirect', 'zhwiki-20091023-redirect.sql.gz'),
        ('zh_classical', '20091023', 'langlinks',
         'zh_classicalwiki-20091023-langlinks.sql.gz'),
        ('zh_classical', '20091023', 'pagelinks',
         'zh_classicalwiki-20091023-pagelinks.sql.gz'),
        ('zh_classical', '20091023', 'pages-articles',
         'zh_classicalwiki-20091023-pages-articles.xml.bz2'),
        ('zh_classical', '20091023', 'redirect',
         'zh_classicalwiki-20091023-redirect.sql.gz')]:

        dump_info = dump_infos.next()

        eq_(dump_info.language, lang)
        eq_(dump_info.date, date)
        eq_(dump_info.table, table)
        eq_(dump_info.path, os.path.join(os.path.sep, 'path', 'to',
                                            filename))


def test_filter_strings():
    eq_(wpi_utils.filter_strings(
        '^INSERT', ["INSERT INTO `witch` VALUES ('使用者')",
                    'ALTER TABLE']).next(),
        "INSERT INTO `witch` VALUES ('使用者')")
    eq_(list(wpi_utils.filter_strings(r'foo', ['bar', 'baz'])), [])
    eq_(list(wpi_utils.filter_strings(r'foo', [])), [])


def test_field_map():
    eq_(wpi_utils.field_map([{'witch': b'duck'}], 'witch', str).next(),
        {'witch': 'duck'})


def test_convert_to_unicode():
    eq_(wpi_utils.convert_to_unicode(
        [b'\xe4\xbd\xbf\xe7\x94\xa8\xe8\x80\x85'], 'utf8').next(),
        '\u4f7f\u7528\u8005')
    eq_(wpi_utils.convert_to_unicode(
        [b'Well, she turned me into a newt'], 'utf8').next(),
        'Well, she turned me into a newt')
    eq_(list(wpi_utils.convert_to_unicode([], 'utf8')), [])


def test_convert_multirow_to_unicode():
    mul_row = [b"INSERT INTO `witch` VALUES ('\xc3\xa5',23),('\xe5',42);"]
    eq_(wpi_utils.convert_multirow_to_unicode(mul_row, 'utf8').next(),
        "INSERT INTO `witch` VALUES ('å',23);")
    mul_row = [b'''INSERT INTO "witch" VALUES ('\xc3\xa5',23),('\xe5',42);''']
    eq_(wpi_utils.convert_multirow_to_unicode(mul_row, 'utf8').next(),
        '''INSERT INTO "witch" VALUES ('å',23);''')


def test_single_row():
    mul_row = b'''INSERT INTO "witch" VALUES ('ne (wt)',23),('ni',42);'''
    eq_(list(wpi_utils.single_rows(mul_row)), ["('ne (wt)',23)", "('ni',42)"])
