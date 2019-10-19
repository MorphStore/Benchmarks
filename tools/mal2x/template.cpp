/**********************************************************************************************
 * Copyright (C) 2019 by MorphStore-Team                                                      *
 *                                                                                            *
 * This file is part of MorphStore - a compression aware vectorized column store.             *
 *                                                                                            *
 * This program is free software: you can redistribute it and/or modify it under the          *
 * terms of the GNU General Public License as published by the Free Software Foundation,      *
 * either version 3 of the License, or (at your option) any later version.                    *
 *                                                                                            *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;  *
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  *
 * See the GNU General Public License for more details.                                       *
 *                                                                                            *
 * You should have received a copy of the GNU General Public License along with this program. *
 * If not, see <http://www.gnu.org/licenses/>.                                                *
 **********************************************************************************************/

// ##### mal2morphstore docu #####

#include <core/memory/mm_glob.h>
#include <core/morphing/format.h>
#include <core/morphing/dynamic_vbp.h> // @todo Only when required!
#include <core/morphing/k_wise_ns.h> // @todo Only when required!
#include <core/persistence/binary_io.h>
#include <core/storage/column.h>
#include <core/utils/basic_types.h>
#include <core/utils/printing.h>

#include <vector/vector_extension_structs.h>
#include <vector/vector_primitives.h>

// ##### mal2morphstore headers #####

#include <iostream>

using namespace morphstore;

// ****************************************************************************
// Schema (only the part required for this query)
// ****************************************************************************

// ##### mal2morphstore schema #####

// ****************************************************************************
// Main program
// ****************************************************************************

int main(int argc, const char ** argv) {
    if(argc != 2) {
        // TODO Whether there is a slash at the end should not matter.
        std::cerr << "This query program expects exactly one argument: the "
                "relative or absolute path to the directory containing the "
                "column files (without trailing slash)." << std::endl;
        return 1;
    }
    const std::string dataPath(argv[1]);
    
    // ##### mal2morphstore processingstyle #####
    
    // ------------------------------------------------------------------------
    // Loading the base data
    // ------------------------------------------------------------------------
    
    std::cerr << "Loading the base data started... ";
    std::cerr.flush();
    
    // ##### mal2morphstore dataload #####
    
    std::cerr << "done." << std::endl;
    
    // ------------------------------------------------------------------------
    // Query execution
    // ------------------------------------------------------------------------
    
    std::cerr << "Query execution started... ";
    std::cerr.flush();
    
    // ##### mal2morphstore prog #####
    
    std::cerr << "done." << std::endl;
    
    // ------------------------------------------------------------------------
    // Result output
    // ------------------------------------------------------------------------
    
    std::cerr << "Result output started... ";
    std::cerr.flush();
    
    // ##### mal2morphstore result #####
    
    std::cerr << "done." << std::endl;
    
    return 0;
    
    // ------------------------------------------------------------------------
    // Automatic analysis of the translated program
    // ------------------------------------------------------------------------
    
    // ##### mal2morphstore analysis #####
}