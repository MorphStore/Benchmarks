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
Facilities for handling MorphStore's processing styles.

In MorphStore, all operators can be executed in different processing styles
(provided that an implementation exists). The processing style specifies, which
kind of instructions are used, e.g., only scalar instructions, or SIMD
instructions of different vector extensions.

This module provides the means for taking the processing style into account in
the program translation.
"""


# Names of the processing styles in MorphStore.
# These must be the exact names of the elements of MorphStore's enum
# processing_style_t!
PS_SCALAR = "scalar"
PS_VEC128 = "vec128"
PS_VEC256 = "vec256"
PS_VEC512 = "vec512"

# Maps the name of a processing style to the name of the subdirectory of 
# MorphStore's include directory in which the operator implementations for the
# processing style can be found.
INCLUDE_DIR_BY_PS = {
    PS_SCALAR: "scalar",
    PS_VEC128: "vectorized",
    PS_VEC256: "vectorized",
    PS_VEC512: "vectorized",
}

# The name of the variable (in the generated query program) which stands for
# the processing style all operators shall use.
PS_VAR = "ps"

# A key to be used in format strings representing the C++ header paths.
INCLUDE_DIR_KEY = "ps_dir"