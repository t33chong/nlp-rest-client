#    rootNode.py
#    Class definition for RootNode
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
from nonTerminalNode import *

class RootNode(NonTerminalNode):
    def __init__(self, aList, aParent, aSentCount):
        if len(aList) == 1:
            NonTerminalNode.__init__(self, aList[0], aParent, aSentCount)
        else:
            NonTerminalNode.__init__(self, aList, aParent, aSentCount)
        gorncount = 0
        self.children = self.populateChildren(self.listForm)
        self.gorn = [aSentCount]
        self.oneUp = None
        self.oneRight = None
        self.oneLeft = None
        self.flat = self.getFlat()   #Flat representation with traces
        self.flat2 = self.getFlat2() #Flat representation without traces
        self.valuateGorns(self.children)  #Create Gorn addresses, a useful way of locating nodes within a tree
        
    def getFlat(self):
        def recurse_get(node):
            li = []
            for child in node.children:
                try:
                    child.children[0]
                    li.extend(recurse_get(child))
                except:
                    li.append(child)
            return li
            
        lis = []
        for child in self.children:
            try:
                child.children[0]
                lis.extend(recurse_get(child))
            except:
                lis.append(child)
        return lis


    def getFlat2(self):
        x = [n for n in self.flat]
        y = []
        for n in x:
            try:
                ['-NONE-', '*T*-1', '*T*-2', '*T*', '*', '*-1', '*-2', '*?*'].index(n.pos)
            except:
                try:
                    y.children[0]
                except:
                    y.append(n)
        return y
        
        
    def valuateGorns(self, ls):
        for i in range(0, len(ls)):
            ls[i].gorn = ls[i].oneUp.gorn + [i]
            try:
                valuateGorns(ls[i].children)
            except:
                pass