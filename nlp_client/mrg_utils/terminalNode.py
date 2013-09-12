#    terminalNode.py
#    Class definition for TerminalNode
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
#    mrg_utils.py is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with mrg_utils.py.  If not, see <http://www.gnu.org/licenses/>.

from node import *

class TerminalNode(Node):
    def __init__(self, aList, aParent, aSentCount):
        Node.__init__(self, aList, aParent, aSentCount)
        self.isTerminal = True
        self.isNonTerminal = False
        self.head = self
        self.termHead = self
        self.string = self.getString()
        self.headOf = []
        
        
    def getString(self):
        string = ''
        for i in self.listForm[1:]:
            if type(i) == list:
                string += self.getString(i)
                string += ' '
            else:
                string += i
        return string