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


# Names of the Vector extensions in MorphStore.
# These must follow Morphstore's VectorExtension template:
# VectorExtension<VectorSize<BaseType>
PS_SCALAR = "scalar<v64<uint64_t>>"
PS_VEC128 = "sse<v128<uint64_t>>"
PS_VEC256 = "avx2<v256<uint64_t>>"
PS_VEC512 = "avx512<v512<uint64_t>>"
PS_VEC128_NEON = "neon<v128<uint64_t>>" 

# Maps the name of a processing style to the name of the subdirectory of 
# MorphStore's include directory in which the operator implementations for the
# processing style can be found.
INCLUDE_DIR_HANDCODED = {
    PS_SCALAR: "scalar",
    PS_VEC128: "vectorized",
    PS_VEC128_NEON: "vectorized",
    PS_VEC256: "vectorized",
    PS_VEC512: "vectorized",
}

INCLUDE_DIR_LIB = {
    PS_SCALAR: "general_vectorized",
    PS_VEC128: "general_vectorized",
    PS_VEC128_NEON: "general_vectorized",
    PS_VEC256: "general_vectorized",
    PS_VEC512: "general_vectorized",
}

# The name of the variable (in the generated query program) which stands for
# the processing style all operators shall use.
PS_VAR = "ps"

# A key to be used in format strings representing the C++ header paths.
INCLUDE_DIR_KEY = "ps_dir"