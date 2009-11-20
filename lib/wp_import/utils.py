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

"""wp_import.utils

This module contains some utilities that are used in the Wikipedia dump import
process.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import bz2
import fnmatch
import gzip
import itertools
import logging
import os
import re

from contextlib import contextmanager

import wp_import

_log = logging.getLogger(__name__)


class DumpInfo(object):
    """Information about a database dump file
    """

    def __init__(self, path, fn_regex):
        super(DumpInfo, self).__init__()

        self.fn_regex = fn_regex
        self.path = path

    def __cmp__(self, other):
        return cmp(self.path, other.path)

    def __getitem__(self, item):
        return getattr(self, item)

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value):
        self._filename = value

    @filename.deleter
    def filename(self):
        del self._filename

    @property
    def fn_regex(self):
        return self._fn_regex

    @fn_regex.setter
    def fn_regex(self, value):
        self._fn_regex = value
        self._fn_pat = re.compile(value)

    @fn_regex.deleter
    def fn_regex(self):
        del self._fn_regex
        del self._fn_pat

    @property
    def language(self):
        return self._language

    @language.setter
    def language(self, value):
        self._language = value

    @language.deleter
    def language(self):
        del self._language

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        self._date = value

    @date.deleter
    def date(self):
        del self._date

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value
        self.filename = os.path.basename(value)

        # assign extracted values
        fn_mat = self._fn_pat.match(self.filename)
        self.language = fn_mat.group('language')
        self.date = fn_mat.group('date')
        self.table = fn_mat.group('table')

    @path.deleter
    def path(self):
        del self._path
        del self.language
        del self.date
        del self.table


@contextmanager
def open_compressed(filename):
    """Open a compressed file.

    :param filenames:   Sequence of filenames to open.
    :type filenames:    iterable
    """
    try:
        if filename.endswith('.gz'):
            open_file = gzip.open(filename)
        elif filename.endswith('.bz2'):
            open_file = bz2.BZ2File(filename)
        else:
            open_file = open(filename)
        yield open_file
    finally:
        open_file.close()


def filter_strings(pat, seq):
    """Generator that yields only those strings matching the given regular
    expression.

    :param pat:     Regular expression used for filtering.
    :type pat:      str

    :param seq:     Sequence of strings to filter.
    :type seq:      list of unicode strings
    """
    patc = re.compile(pat)
    for element in seq:
        if patc.search(element):
            yield element


def field_map(dictseq, name, func):
    """Generator for dictionary field conversion.

    This generator will convert the field with given name using the supplied
    function.

    :param dictseq:     A sequence of dictionaries
    :type dictseq:      iterable yielding dictionaries

    :param name:        The name of the field to convert
    :type name:         Hashable

    :param func:        The function used for conversion
    :type func:         function
    """
    for d in dictseq:
        d[name] = func(d[name])
        yield d


def find(fn_pat, root):
    """Get all files matching given pattern

    :param fn_pat:  Pattern that filenames must match
    :type fn_pat:   string

    :param root:    Root of the file system tree in which files are
                    considered.
    :type root:     string
    """
    for path, dirlist, filelist in os.walk(root):
        for name in fnmatch.filter(filelist, fn_pat):
            yield os.path.join(path, name)


def file_paths(root):
    """Get paths to all files beneath given root.

    If root is a path to a file the path will be returned.

    :param root:    Root of the file system tree in which files are
                    considered.
    :type root:     string
    """
    if os.path.isfile(root):
        yield root
    else:
        for path, dirlist, filelist in os.walk(root):
            for name in filelist:
                yield os.path.join(path, name)


def convert_to_unicode(seq, encoding):
    """Generator that converts a sequence of data with given encoding
    to strings ones.

    :param seq:         Sequence of bytes with given encoding to convert
                        to strings.
    :type seq:          iterable

    :param encoding:    Encoding of bytes in sequence
    :type encoding:     string
    """
    for el in seq:
        try:
            yield el.decode(encoding)
        except UnicodeDecodeError as unidec_err:
            _log.warning('Dropped {row}: Not {encoding} encoded'.format(
                row=el.decode(encoding, 'replace'), encoding=encoding))
            continue


def convert_multirow_to_unicode(seq, encoding='utf8'):
    """Decode a sequence of bytes containing multirow INSERT statements from
    given encoding.

    It might be that Wikipedia dumps contain rows that contain strings that
    are *not* utf8 encoded. If we encounter an UnicodeDecodeError we will
    split the multirow INSERT statements into single rows and drop all rows
    that can't be decoded.

    :param seq:         Sequence of bytes with given encoding to convert
                        to strings.
    :type seq:          iterable

    :param encoding:    Encoding of bytes in sequence
    :type encoding:     string
    """
    table_name_pat = re.compile(
        r'''^INSERT\sINTO\s(?P<quote>[`"])(?P<table>[\w-]+)(?P=quote)''',
                           re.IGNORECASE)
    for el in seq:
        try:
            yield el.decode(encoding)
        except UnicodeDecodeError as unidec_err:
            _log.warning('Decoding error: Decode single rows')
            mat_dict = table_name_pat.match(el).groupdict()
            rows = single_rows(el)
            rows = convert_to_unicode(rows, encoding)
            yield 'INSERT INTO {0}{1}{0} VALUES {2};'.format(
                mat_dict['quote'],
                mat_dict['table'],
                ','.join(rows))


def dump_file_paths(fn_regex, *paths):
    """Generator for dump file paths.

    :param fn_regex:    Regular expression that filenames must match to be
                        considered a dump file.
    :type fn_regex:     string

    :param paths:       List of paths where dump files are located.
    :type paths:        iterable
    """
    fn_pat = re.compile(fn_regex)
    # all files within given paths
    dump_file_paths = itertools.chain(*(file_paths(path) for path in paths))
    # matching ones
    dump_file_paths = (path for path in dump_file_paths
                       if fn_pat.match(os.path.basename(path)))
    # we want a unique and sorted list
    return iter(sorted(set(dump_file_paths)))


def dump_info(paths, fn_regex):
    """Construct dump info instances from dump file paths.

    :param paths:       List of dump file paths
    :type paths:        iterable

    :param fn_regex:    Regular expression used to extract attributes
                        from found filenames.

                        The regular expression *must* define the
                        following groups:

                        * language: The language of the dump file
                        * date:     Dump file creation date
                        * table     Name of the database table.

    :type fn_regex:    string
    """
    for filepath in paths:
        yield DumpInfo(filepath, fn_regex)


def single_rows(multirow_insert):
    """Get single rows from a multirow INSERT.

    :param multirow_insert:     Multirow INSERT statement
    :type multirow_insert:      string
    """
    multirow_insert = re.sub(b'^INSERT\sINTO\s[`"][\w-]+[`"]\sVALUES\s', b'',
                             multirow_insert, 1)
    multirow_insert = re.sub(b';$', b'', multirow_insert)
    multirow_insert = multirow_insert.replace(b'),(', b')\n(')
    rows = multirow_insert.split(b'\n')
    row_pat = re.compile(b'^\(.+\)$')
    return itertools.ifilter(lambda el: row_pat.match(el) is not None, rows)
