#*********************************************************************************************
# Copyright (C) 2019 by MorphStore-Team                                                      *
#                                                                                            *
# This file is part of MorphStore - a compression aware vectorized column store.             *
#                                                                                            *
# This program is free software: you can redistribute it and/or modify it under the          *
# terms of the GNU General Public License as published by the Free Software Foundation,      *
# either version 3 of the License, or (at your option) any later version.                    *
#                                                                                            *
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;  *
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  *
# See the GNU General Public License for more details.                                       *
#                                                                                            *
# You should have received a copy of the GNU General Public License along with this program. *
# If not, see <http://www.gnu.org/licenses/>.                                                *
#*********************************************************************************************

"""
Constants for the purposes of the translated query program.

While the translated program will always execute the query, it might do some
additional things, depending on what the program shall be used for.
"""


# The supported purposes of the translated query program.
# Keep these consistent with the purposes in "Benchmarks/ssb/ssb.sh"!

# Obtain the query result in a format easily comparable with MonetDB's output.
PP_CHECK = "c"
PP_CHECK_LONG = "check"
# Like PP_CHECK, no difference in the translated programs, but only in their
# usage in ssb.sh.
PP_RESULTS = "r"
PP_RESULTS_LONG = "results"
# Measure the runtimes of the entire query and each individual operator.
PP_TIME = "t"
PP_TIME_LONG = "time"
# Analyze all base and intermediate columns and output their data
# characteristics.
PP_DATACH = "d"
PP_DATACH_LONG = "datacharacteristics"
# Record the sizes of each base and intermediate column in all formats.
PP_SIZE = "s"
PP_SIZE_LONG = "size"


# A list of all purposes.
PURPOSES = [
    PP_CHECK,
    PP_RESULTS,
    PP_TIME,
    PP_DATACH,
    PP_SIZE,
]
PURPOSES_LONG = [
    PP_CHECK_LONG,
    PP_RESULTS_LONG,
    PP_TIME_LONG,
    PP_DATACH_LONG,
    PP_SIZE_LONG,
]
