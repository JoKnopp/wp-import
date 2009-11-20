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

"""Tests for wp_import.postgresql
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import tempfile

from nose.tools import *

import wp_import.utils as wpi_utils
import wp_import.postgresql as wpi_psql

PREFIX = os.path.join(*os.path.split(os.path.dirname(__file__))[:-1])
TEST_DATA_DIR = os.path.join(PREFIX, 'test', 'data')
DOWNLOAD_DIR = os.path.join(TEST_DATA_DIR, 'download')
EXPECTED_STMTS = {
    'categorylinks': [
        """INSERT INTO "categorylinks" VALUES """ \
        "(130,'Linux','Linux\u5185\u6838','2006-07-25T19:03:22Z')"],
    'langlinks': [
        """INSERT INTO "langlinks" VALUES """ \
        "(43017,'af','Dante Alighieri')"],
    'pagelinks': [
        """INSERT INTO "pagelinks" VALUES (12,0,'P/NP\u554f\u984c')"""],
    'redirect': [
        """INSERT INTO "redirect" VALUES (71247,0,'ASCII\u827a\u672f')"""]}


class FakeOptions(object):
    pass


def test_insert_statements():
    fn_pat = re.compile(
        r'''(?P<language>\w+)wiki-(?P<date>\d{8})-(?P<table>[\w_]+).*''')
    for dump_path in sorted(wpi_utils.find('*.sql.gz', DOWNLOAD_DIR)):
        filename = os.path.basename(dump_path)
        mat = fn_pat.match(filename)
        stmts = list(wpi_psql.insert_statements(dump_path))
        eq_(list(wpi_psql.insert_statements(dump_path)),
            EXPECTED_STMTS[mat.group('table')])


def test_categorylink_pipeline():
    for file_path in wpi_utils.find('*categorylinks*.sql.gz', DOWNLOAD_DIR):
        with wpi_utils.open_compressed(file_path) as cl_file:
            eq_(list(wpi_psql.categorylinks_pipeline(cl_file)),
                EXPECTED_STMTS['categorylinks'])


def test_psql_quotation():
    eq_(list(wpi_psql.psql_quotation(['f `b`', 'baz', 'shrubbery ``'])),
        ['f "b"', 'baz', 'shrubbery ""'])


def test_timestamp_to_iso_8601():
    eq_(list(wpi_psql.timestamp_to_iso_8601([',20080218135752) foo'])),
        [",'2008-02-18T13:57:52Z') foo"])


def test_parse_pgpass():
    with tempfile.NamedTemporaryFile() as tmp_f:
        tmp_f.write('*:*:*:*:GrailQuest\n')
        tmp_f.seek(0)
        eq_(wpi_psql._parse_pgpass(tmp_f.name).next(),
            {'user': '*', 'host': '*', 'port': '*', 'database': '*',
             'password': 'GrailQuest'})
        tmp_f.write('hostname:port:database:username:password\n')
        tmp_f.seek(0)
        eq_(wpi_psql._parse_pgpass(tmp_f.name).next(),
            {'user': 'username', 'host': 'hostname', 'port': 'port',
             'database': 'database',
             'password': 'password'})


def test_password_from_pgpass():
    with tempfile.NamedTemporaryFile() as tmp_f:
        options = FakeOptions()
        options.pg_passfile = tmp_f.name
        options.pg_user = 'KingArthur'
        options.pg_port = '2342'
        options.pg_host = 'Camelot'

        # test generic pgpass line
        tmp_f.write('*:*:*:*:GrailQuest\n')
        tmp_f.seek(0)
        eq_(wpi_psql.password_from_pgpass(options),
            'GrailQuest')

        # test specific pgpass line
        tmp_f.write('Camelot:2342:postgres:KingArthur:GrailQuest\n')
        tmp_f.seek(0)
        eq_(wpi_psql.password_from_pgpass(options),
            'GrailQuest')

        # test pick most specific
        tmp_f.write('Jerusalem:2342:postgres:Brian:Jehova\n')
        tmp_f.write('Camelot:2342:postgres:KingArthur:GrailQuest\n')
        tmp_f.write('*:*:*:*:UnladenSwallow\n')
        tmp_f.seek(0)
        eq_(wpi_psql.password_from_pgpass(options),
            'GrailQuest')

        tmp_f.write('*:*:*:*\n')
        tmp_f.seek(0)
        assert_raises(KeyError, wpi_psql.password_from_pgpass,
                      options=options)
