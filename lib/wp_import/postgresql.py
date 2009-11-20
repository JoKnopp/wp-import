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

"""wp_import.postgresql

This module contains functions that are specific to PostgreSQL.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import fnmatch
import logging
import os
import re

import wp_import
import wp_import.exceptions as wpi_exc
import wp_import.utils as wpi_utils

_log = logging.getLogger(__name__)


def psql_quotation(seq):
    """Generator that replaces ` with ".

    :param seq:  Sequence of strings in which to replace quotations.
    :type seq:   list of unicode strings
    """
    return (el.replace('`', '"') for el in seq)


def timestamp_to_iso_8601(seq):
    """Converts MySQL timestamps to ISO 8601 format in given sequence.

    The generator will convert all substrings matching YYYYMMDDHHMMSS
    to ISO 8601 format.

    :param seq:  Sequence of unicode strings in which to convert timestamps.
    :type seq:   list of unicode strings
    """
    timestamp_pat = re.compile(
        r'([,)])(\d\d\d\d)([0,1]\d)([0-3]\d)([0-5]\d)([0-5]\d)([0-5]\d)([,)])')
    return (timestamp_pat.sub(r"\1'\2-\3-\4T\5:\6:\7Z'\8", el) for el in seq)


def insert_statements(file_path):
    """Get insert statements from given file"""
    with wpi_utils.open_compressed(file_path) as dump_file:
        if fnmatch.fnmatch(os.path.basename(file_path), '*categorylinks*'):
            statements = categorylinks_pipeline(dump_file)
        else:
            statements = generic_pipeline(dump_file)

        for stmt in statements:
            yield stmt


def generic_pipeline(seq):
    """Preprocessing pipeline needed for all dump files.

    Steps in this pipeline:

        * Extract INSERT statements
        * Convert strings to unicode
        * Strip strings
        * Replace MySQL quotes with psql ones

    :param seq: Sequence of strings
    :type seq:  Iterable
    """
    seq = wpi_utils.filter_strings(r'^INSERT', seq)
    seq = wpi_utils.convert_multirow_to_unicode(seq)
    seq = (el.strip() for el in seq)
    seq = psql_quotation(seq)
    return seq


def categorylinks_pipeline(seq):
    """Preprocessing pipeline for categorylinks.

    Steps in this pipeline:

        * Timestamp conversion

    :param seq: Sequence of strings containing INSERT statements
    :type seq:  Iterable
    """
    insert_statements = generic_pipeline(seq)
    return timestamp_to_iso_8601(insert_statements)


def _parse_pgpass(path):
    """Parse pgpass configuration.

    :returns:   A sequence of dictionaries holding the values from ~/.pgpass
    :rtype:     Iterable
    """
    colnames = ('host', 'port', 'database', 'user', 'password')
    pgpass_f = open(path, 'rb')
    pgpass_lines = (line.decode('utf8') for line in pgpass_f)
    pgpass_lines = (line.strip() for line in pgpass_lines)
    pgpass_tuples = (line.split(':') for line in pgpass_lines)
    return (dict(zip(colnames, t)) for t in pgpass_tuples)


def password_from_pgpass(options):
    """Read the password from pgpass.

    :param options: Command line options obtained from optparse
    :type options:  optparse.Values

    :raises IOError:        If pgpass is not present
    :raises StopIteration:  If no matching credentials are found within
                            pgpass
    """
    match_credentials = itertools.ifilter(
        lambda d: (
            (d['user'] == options.pg_user or d['user'] == '*')
            and (d['host'] == options.pg_host or d['host'] == '*')
            and (d['port'] == options.pg_port or d['port'] == '*')
            and (d['database'] == 'postgres' or d['database'] == '*')),
        _parse_pgpass(options.pg_passfile))

    most_specific_match = next(match_credentials)
    return most_specific_match['password']
