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

"""This module contains classes for importing Wikipedia database dumps into
various database backends.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import fnmatch
import logging
import os
import re
import string
import subprocess

import mwdb
import sqlalchemy.exc

from . import utils
from . import postgresql

_log = logging.getLogger(__name__)


class Importer(object):
    """Base class for vender specific MW dump importer"""

    def __init__(self, config, options):
        super(Importer, self).__init__()
        self.config = config
        self.options = options

        self.db_name_template = string.Template(self.config.get(
            'Database', 'db_name_template'))
        self.enabled_languages = sorted((lang for lang in
                                         self.config.options('Languages')
                                         if self.config.getboolean(
                                             'Languages', lang)))
        self.dump_file_pat = re.compile(self.config.get('Patterns',
                                                        'dump_file_pattern'))

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    @config.deleter
    def config(self):
        del self._config

    @property
    def dump_file_pat(self):
        return self._dump_file_pat

    @dump_file_pat.setter
    def dump_file_pat(self, value):
        self._dump_file_pat = value

    @dump_file_pat.deleter
    def dump_file_pat(self):
        del self._dump_file_pat

    @property
    def enabled_languages(self):
        return self._enabled_languages

    @enabled_languages.setter
    def enabled_languages(self, value):
        self._enabled_languages = value

    @enabled_languages.deleter
    def enabled_languages(self):
        del self._enabled_languages

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):
        self._options = value

    @options.deleter
    def options(self):
        del self._options

    def _database_name(self, dump_info):
        """Get database name for given dump_info dictionary.
        """
        return self.db_name_template.substitute(dump_info)


class PostgreSQLImporter(Importer):
    """Importer for PostgreSQL.

    This class provides functionality to import Wikipedia dump files into a
    PostgreSQL database.
    """

    def __init__(self, config, options):
        """Constructor.
        """
        super(PostgreSQLImporter, self).__init__(config, options)

    def _connect_to_db(self, dump_info):
        """Connect to the suitable database for given dump.

        The database will be created if it does not exist yet.
        """
        dump_db = mwdb.orm.database.PostgreSQLDatabase(
            self.options.pg_driver,
            self.options.pg_user,
            self.options.pg_password,
            self.options.pg_host,
            self._database_name(dump_info),
            dump_info.language)

        if self._database_name(dump_info) not in dump_db.all_databases():
            _log.info('{0}: Create database'.format(
                self._database_name(dump_info)))
            dump_db.create()

        dump_db.connect()
        return dump_db

    def _create_table(self, dump_db, table_name):
        """Create table for dump within given database.
        """
        if table_name not in dump_db.table_names:
            dump_db.create_table(table_name=table_name, pkey=False,
                                 index=False)
            return

        if self.options.reimport:
            dump_db.drop_pkey_constraint(table_name)
            dump_db.drop_indexes(table_name)
            dump_db.truncate_table(table_name)

    def _get_insert_statements(self, dump_info):
        """Get insert statement iterator.
        """
        insert_statements = postgresql.insert_statements(dump_info.path)
        if self.options.pg_driver == 'psycopg2':
            insert_statements = (el.replace('%', '%%') for el in
                                 insert_statements)
        return insert_statements

    def _psql_pipe(self, db_name, table, statements):
        """Pipe given statements into psql.

        :param db_name:     Name of the database psql should connect to.
        :type db_name:      str

        :param table:       Name of the table the statements insert into.
        :type table:        str

        :param statements:  Sequence of SQL statements.
        :type statements:   iterable
        """
        psql_process = subprocess.Popen([
            'psql',
            '--quiet',
            '--host={0}'.format(self.options.pg_host),
            '--username={0}'.format(self.options.pg_user),
            '--no-password',
            '--dbname={0}'.format(db_name),
        ],
            stdin=subprocess.PIPE,
        )

        _log.info('{0}.{1}: Importing data'.format(db_name, table))

        for stmt in statements:
            if isinstance(stmt, unicode):
                stmt = stmt.encode('utf8')

            stmt = stmt.strip()
            psql_process.stdin.write(b'{0}\n'.format(stmt))

        psql_process.stdin.close()
        psql_process.wait()

        _log.info('psql [{0:d}]: Exited with {1}'.format(
            psql_process.pid, psql_process.returncode))

        return psql_process.returncode

    def _import_sql_dump(self, dump_info):
        """Import dump.

        :param dump_info:   Dump file information. This information is used to
                            select the appropriate database and table.
        :type dump_info:    DumpInfo
        """
        _log.info('Processing: {0.filename}'.format(dump_info))
        dump_db = self._connect_to_db(dump_info)

        if (dump_info.table in dump_db.table_names
            and not self.options.reimport):
            _log.info('{0}.{1.table}: Skipped import of {1.filename}'.format(
                dump_db.name, dump_info))
            return

        self._create_table(dump_db, dump_info.table)
        insert_statements = self._get_insert_statements(dump_info)

        psql_returncode = self._psql_pipe(self._database_name(dump_info),
                                          dump_info.table, insert_statements)

        if psql_returncode != 0:
            _log.info('{0}.{1}: Import failed. Drop Table'.format(
                self._database_name(dump_info), dump_info.table))
            dump_db.drop_table(table)
            return

        try:
            dump_db.create_pkey_constraint(dump_info.table)
        except sqlalchemy.exc.IntegrityError as integrity_error:
            _log.error(integrity_error)
            _log.error(
                '{0}.{1.table}: Could not create pkey constraint'.format(
                    dump_db.name, dump_info))

        _log.info('{0}.{1.table}: Create indexes'.format(
            dump_db.name, dump_info))
        dump_db.create_indexes(dump_info.table)

    def _convert_pages_articles(self, pa_path):
        """Convert the pages-articles XML dump to SQL.

        This method calls xml2sql in a subprocess and returns a dictionary
        with the paths to page.sql, revision.sql and text.sql.
        """
        _log.info('Converting: {0}'.format(os.path.basename(pa_path)))
        converter_process = subprocess.Popen(['xml2sql',
                                              '--postgresql=8.4',
                                              '--output-dir={0}'.format(
                                                  os.path.dirname(pa_path)),
                                             ],
                                             stdin=subprocess.PIPE,
                                            )

        with utils.open_compressed(pa_path) as pa_dump_f:
            for line in pa_dump_f:
                converter_process.stdin.write(line)
        converter_process.stdin.close()

        converter_process.wait()

        _log.info('xml2sql [{0:d}]: Exited with {1:d}'.format(
            converter_process.pid, converter_process.returncode))

        base_path = os.path.dirname(pa_path)
        return {
            'page': os.path.join(base_path, 'page.sql'),
            'revision': os.path.join(base_path, 'revision.sql'),
            'text': os.path.join(base_path, 'text.sql'),
        }

    def _import_pages_articles(self, dump_info):

        _log.info('Processing: {0.filename}'.format(dump_info))
        dump_db = self._connect_to_db(dump_info)

        # skip conversion if all {table, revision, text} tables are
        # present in the database *and* reimport is disabled
        if (not self.options.reimport
            and ('revision' in dump_db.table_names)
            and ('text' in dump_db.table_names)
            and ('page' in dump_db.table_names)):

            return

        file_path_dict = self._convert_pages_articles(dump_info.path)

        for table, path in file_path_dict.iteritems():

            # skip table if present and reimport disabled
            if (table in dump_db.table_names and not self.options.reimport):

                _log.info('{0}.{1}: Skipped import of {1}'.format(
                    dump_db.name, table))

                continue

            self._create_table(dump_db, table)

            with open(path) as dump_f:
                psql_returncode = self._psql_pipe(
                    self._database_name(dump_info), table, dump_f)

                if psql_returncode != 0:
                    _log.info('{0}.{1}: Import failed. Drop Table'.format(
                        self._database_name(dump_info), table))
                    dump_db.drop_table(table)
                    return

            try:
                dump_db.create_pkey_constraint(table)
            except sqlalchemy.exc.IntegrityError as integrity_error:
                _log.error(integrity_error)
                _log.error(
                    '{0}.{1}: Could not create pkey constraint'.format(
                        dump_db.name, table))

            dump_db.create_indexes(table)
            os.remove(path)

    def import_dumps(self, paths):
        """Import newest dumps found at or beneath given paths.

        :param paths:   List of paths to dump files or directories.
        :type paths:    Iterable
        """
        dump_file_paths = utils.dump_file_paths(self.dump_file_pat, *paths)
        dump_info = sorted(utils.dump_info(dump_file_paths,
                                           self.dump_file_pat))
        grouped_dumps = itertools.groupby(dump_info, lambda di: di.language)
        grouped_dumps = itertools.ifilter(lambda (lang, dumps): lang in
                                      self.enabled_languages, grouped_dumps)

        for (lang, dumps) in grouped_dumps:
            _log.info('Processing language: {0}'.format(lang))

            for dump in dumps:
                if fnmatch.fnmatch(dump.filename, '*pages-articles.xml.bz2'):
                    self._import_pages_articles(dump)
                else:
                    self._import_sql_dump(dump)
