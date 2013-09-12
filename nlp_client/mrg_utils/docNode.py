#    docNode.py
#    Class definition for DocNode
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




from nonTerminalNode import *

class DocNode(NonTerminalNode):
    def __init__(self, nodeList):
        self.isTop = True
        self.pos = 'DOC'
        self.isTerminal = False
        self.isNonTerminal = True
        self.children = nodeList
        self.string = ' '.join([x.string for x in self.children])
        self.head = self.getHead()
        self.termhead = getTermHead(self.head, self)
        self.isDominated = False
        self.domNode = None
        self.oneRight = None
        self.oneLeft = None
        self.oneUp = None
        for i in range(0, len(self.children)):
            try:
                self.children[i].oneRight = self.children[i+1]
            except:
                pass
            if i > 0:
                self.children[i].oneLeft = self.children[i-1]
            self.children[i].oneUp = self
            
