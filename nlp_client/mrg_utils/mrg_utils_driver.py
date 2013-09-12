#    mrg_utils_driver.py
#    Driver for mrg_utils.py
#
#    Created by Robert Elwell, University of Texas at Austin, Department of Linguistics
#    http://comp.ling.utexas.edu/relwell
#
#
#    This file is part of mrg_utils.py.
#
#    mrg_utils.py is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Foobar is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with mrg_utils.py.  If not, see <http://www.gnu.org/licenses/>.


# Usage: python mrg_utils_driver.py (insert absolute address of .mrg-style file you want to process here)

import sys

from mrg_utils import *

MRG_Document(sys.argv[1])

print [a.string for a in x.termHeads]

print 'No errors encountered'
