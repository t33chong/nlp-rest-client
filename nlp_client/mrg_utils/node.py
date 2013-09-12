#    node.py
#    Class definition for Node
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


#####################################
#Abstract class for syntactic nodes##
#####################################

from headRules import *

class Node:
    def __init__(self, aList, aParent, aSentCount):
        if self.__class__ == Node:
            raise NotImplementedError, 'class Node is abstract'
        self.children = None ## List of children
        self.isTerminal = False
        self.isNonTerminal = True
        self.oneUp = None
        self.oneRight = None
        self.oneLeft = None
        self.listForm = aList
        self.string = None
        if not aParent:
            self.isDominated = False
        else:
            self.oneUp = aParent
            self.isDominated = True
        self.pos = self.listForm[0].split('=')[0].split('|')[0]
        self.isTop = False  ## Is root node
        self.index = aSentCount
        self.gorn = []

        
    def hasNode(self, target):   ## Test whether Node dominates target
        f = False
        if self.children:
            if node in self.children:
                return True
            for child in self.children:
                if child.hasNode(node) == True:
                    return True
        else:
            return False

    def getHead(self):           ## Get syntactic head, based on Collins rules, modified by Marneffe et al (2006)
        if self.pos == "ROOT":
            return self.children[0].getHead()
        if self.pos == "NP":
            if isCoord(self):
                return pickCoordHead(self)
        elif self.pos == "VP":
            if hasAux(self):
                for case in auxCase:
                    if headRules[case[0]](self, case[1]) != None:
                        return headRules[case[0]](self, case[1])
            elif hasCop(self):
                for case in vpCop:
                    if headRules[case[0]](self, case[1]) != None:
                        return headRules[case[0]](self, case[1])
        elif self.pos == "SQ":
            if hasAux(self):
                for case in auxCase:
                    if headRules[case[0]](self, case[1]) != None:
                        return headRules[case[0]](self, case[1])
            elif hasCop(self):
                for case in sqCop:
                    if headRules[case[0]](self, case[1]) != None:
                        return headRules[case[0]](self, case[1])
        for tup in semHeadRules[self.pos.split('-')[0]]:
            if headRules[tup[0]](self, tup[1]) != None:
                return headRules[tup[0]](self, tup[1])
        return self.children[0]     
        

    def terminalCheck(self, listNode):
        for item in listNode[1:]:
            if type(item) == list:
                return False            
        return True






            
