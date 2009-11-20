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

"""Error definition for wp_import.

This module defines errors raised within wp_import. Errors specific to
wp_import are subclasses of WPError.
"""

# -----------
# exit status

# wrong or missing argument
EARGUMENT = 2

# no such file or directory
ENOENT = 3

# wrong password
EPASS = 4
