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
Internal representation of the (un)compressed formats in MorphStore.

Our cost model works with such representations, therefore we need them here.
However, since MorphStore's formats usually have (template) parameters, we need
to extend the classes from the algo-module.

In addition to the interface of the base classes in the algo-module, these
format classes here provide the following:
- a field named 'headers', which is a list of the C++ headers in MorphStore's
  Engine repository required for the respective format
- a method 'getSimpleName', which returns a short name used for command line
  argument input
"""


import mal2morphstore.processingstyles as pss

import sys
# TODO This is relative to ssb.sh.
sys.path.append("../../LC-BaSe/cm")
import algo

# *****************************************************************************
# Constants
# *****************************************************************************

# -----------------------------------------------------------------------------
# Block size of cascades
# -----------------------------------------------------------------------------
# There is only one global block size for the entire program, since we do not
# consider using different cascade block sizes for different columns. Note that
# this default can be set by a command line argument.

CASC_BLOCKSIZE_LOG = 1024

# *****************************************************************************
# Internal representations of formats
# *****************************************************************************

class MorphStoreStandAloneFormat(algo.StandAloneAlgo):
    def __init__(self, name, mode=None):
        if not name.endswith("_f"):
            raise RuntimeError(
                    "the format name must end with '_f' following the naming "
                    "convention in MorphStore's Engine repository"
            )
        super().__init__(name, mode, False)
        
    def getSimpleName(self):
        # Assumes that the name ends with "_f".
        return self._name[:-2]

class UncomprFormat(MorphStoreStandAloneFormat):
    headers = [
        # TODO this header is only required, since uncompr_f is still declared
        # there
        "core/morphing/format.h",
        "core/morphing/uncompr.h"
    ]

    def __init__(self, mode=None):
        super().__init__("uncompr_f", mode)
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, UncomprFormat):
            return other._mode == self._mode
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return self._name
            
    def changeMode(self, mode):
        return UncomprFormat(mode)
    
# -----------------------------------------------------------------------------
# Physical-level
# -----------------------------------------------------------------------------

class StaticVBPFormat(MorphStoreStandAloneFormat):
    headers = [
        "core/morphing/static_vbp.h",
        "core/morphing/vbp.h"
    ]
    
    def __init__(self, ps, bw=None, mode=None):
        super().__init__("static_vbp_f", mode)
        self._ps = ps
        self._bw = bw
        self._step = pss.PS_INFOS[ps].vectorElementCount
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, StaticVBPFormat):
            return (
                    other._step == self._step and
                    other._bw == self._bw and
                    other._mode == self._mode
            )
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<vbp_l<{}, {}> >".format(
                self._name,
                "bw" if self._bw is None else self._bw,
                self._step
        )
            
    def changeMode(self, mode):
        return StaticVBPFormat(self._ps, self._bw, mode)
    
    def changeBw(self, bw):
        return StaticVBPFormat(self._ps, bw, self._mode)

class DynamicVBPFormat(MorphStoreStandAloneFormat):
    headers = ["core/morphing/dynamic_vbp.h"]

    def __init__(self, ps, mode=None):
        super().__init__("dynamic_vbp_f", mode)
        self._ps = ps
        self._blockSizeLog = pss.PS_INFOS[ps].vectorSizeBit
        self._pageSizeBlocks = pss.PS_INFOS[ps].vectorSizeByte
        self._step = pss.PS_INFOS[ps].vectorElementCount
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, DynamicVBPFormat):
            return (
                    other._blockSizeLog == self._blockSizeLog and
                    other._pageSizeBlocks == self._pageSizeBlocks and
                    other._step == self._step and
                    other._mode == self._mode
            )
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<{}, {}, {}>".format(
                self._name,
                self._blockSizeLog,
                self._pageSizeBlocks,
                self._step
        )
            
    def changeMode(self, mode):
        return DynamicVBPFormat(self._ps, mode)

class KWiseNSFormat(MorphStoreStandAloneFormat):
    headers = ["core/morphing/k_wise_ns.h"]

    def __init__(self, ps, mode=None):
        if ps != pss.PS_VEC128:
            raise RuntimeError(
                    "the format 'k_wise_ns_f' is only available for the "
                    "processing style '{}'".format(pss.PS_VEC128)
            )
        super().__init__("k_wise_ns_f", mode)
        self._ps = ps
        self._blockSizeLog = pss.PS_INFOS[ps].vectorElementCount
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, KWiseNSFormat):
            return (
                    other._blockSizeLog == self._blockSizeLog and
                    other._mode == self._mode
            )
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<{}>".format(self._name, self._blockSizeLog)
            
    def changeMode(self, mode):
        return KWiseNSFormat(self._ps, mode)
    
# -----------------------------------------------------------------------------
# Logical-level
# -----------------------------------------------------------------------------
    
class _DeltaFormat(MorphStoreStandAloneFormat):
    """
    This format does not exist stand-alone in MorphStore, but having it here
    makes things easier, especially w.r.t. the cost model.
    """
    
    headers = ["core/morphing/delta.h"]

    def __init__(self, ps, mode=None):
        super().__init__("delta_f", mode)
        self._ps = ps
        self._step = pss.PS_INFOS[ps].vectorElementCount
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, _DeltaFormat):
            return other._step == self._step and other._mode == self._mode
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<{}>".format(self._name, self._step)
            
    def changeMode(self, mode):
        return _DeltaFormat(self._ps, mode)
    
class _ForFormat(MorphStoreStandAloneFormat):
    """
    This format does not exist stand-alone in MorphStore, but having it here
    makes things easier, especially w.r.t. the cost model.
    """
    
    headers = ["core/morphing/for.h"]
    
    def __init__(self, ps, mode=None):
        super().__init__("for_f", mode)
        self._ps = ps
        self._pageSizeBlocks = pss.PS_INFOS[ps].vectorElementCount
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, _ForFormat):
            return (
                    other._pageSizeBlocks == self._pageSizeBlocks and
                    other._mode == self._mode
            )
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<{}>".format(self._name, self._pageSizeBlocks)
            
    def changeMode(self, mode):
        return _ForFormat(self._ps, mode)
    
# -----------------------------------------------------------------------------
# Cascades
# -----------------------------------------------------------------------------
    
class MorphStoreCascFormat(algo.CascadeAlgo):
    def __init__(self, bs, log, phy, mode=None):
        super().__init__("", "", bs, mode, False)
        self._log = log
        self._phy = phy
    
    def getLogAlgo(self):
        return self._log
    
    def getPhyAlgo(self):
        return self._phy
    
    def getDisplayName(self):
        raise NotImplemented()
    
    def getSimpleName(self):
        return "{}+{}".format(
                self._log.getSimpleName(), self._phy.getSimpleName()
        )

class DeltaCascFormat(MorphStoreCascFormat):
    def __init__(self, bs, ps, phy, mode=None):
        super().__init__(bs, _DeltaFormat(ps, mode), phy, mode)
        self._ps = ps
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, DeltaCascFormat):
            return (
                    other._bs == self._bs and
                    other._log == self._log and
                    other._phy == self._phy and
                    other._mode == self._mode
            )
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<{}, {}, {} >".format(
                self._log._name,
                self._bs,
                self._log._step,
                self._phy.getInternalName()
        )
            
    def changeMode(self, mode):
        return DeltaCascFormat(
                self._bs, self._ps, self._phy.changeMode(mode), mode
        )

class ForCascFormat(MorphStoreCascFormat):
    def __init__(self, bs, ps, phy, mode=None):
        super().__init__(bs, _ForFormat(ps, mode), phy, mode)
        self._ps = ps
        
    def __hash__(self):
        # TODO non-dummy implementation
        return 0
        
    def __eq__(self, other):
        if isinstance(other, ForCascFormat):
            return (
                    other._bs == self._bs and
                    other._log == self._log and
                    other._phy == self._phy and
                    other._mode == self._mode
            )
        elif isinstance(other, algo.Algo):
            return False
        else:
            return NotImplemented
    
    def getInternalName(self):
        return "{}<{}, {}, {} >".format(
                self._log._name,
                self._bs,
                self._log._pageSizeBlocks,
                self._phy.getInternalName()
        )
            
    def changeMode(self, mode):
        return ForCascFormat(
                self._bs, self._ps, self._phy.changeMode(mode), mode
        )
    
    
# *****************************************************************************
# Utilities
# *****************************************************************************

def getAllFormats(ps):
    """
    Return a list of all formats available for the given processing style.
    """
    
    res = [
        UncomprFormat(),
        StaticVBPFormat(ps),
    ]
    dynamicNSFormats = [
        DynamicVBPFormat(ps),
    ]
    if ps == pss.PS_VEC128:
        dynamicNSFormats.append(KWiseNSFormat(ps))
    res.extend(dynamicNSFormats)
    for fmt in dynamicNSFormats:
        res.append(DeltaCascFormat(CASC_BLOCKSIZE_LOG, ps, fmt))
        res.append(ForCascFormat(CASC_BLOCKSIZE_LOG, ps, fmt))
    return res

def getAllSimpleNames():
    """
    Returns a list of the simple names of all formats.
    """
    return [fmt.getSimpleName() for fmt in getAllFormats(pss.PS_VEC128)]
    
def byName(name, ps):
    """
    Returns the format of the given simple name for the given processing style.
    """
    
    for fmt in getAllFormats(ps):
        if name == fmt.getSimpleName():
            return fmt
    raise RuntimeError("unknown format: '{}'".format(name))