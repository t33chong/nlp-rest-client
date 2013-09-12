#    sentence.py
#    Class definition for Sentence
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

import sys

from rootNode import *
from sexpr_parse import *

class Sentence:
    def __init__(self, strng, counter=0):
        self.fullTree = parse_string(strng).next()
        #try:
        self.nodes = RootNode(self.fullTree, self, counter)
        #except:
        #    print 'There was an error processing sentence %d.' % counter
        #    sys.exit()
        self.children = self.nodes.children
        
        
        
    def getHeads(self):
        def recurse_get(node):
            li = [node.head]
            for child in node.children:
                try:
                    child.children[0]
                    li.extend(recurse_get(child))
                except:
                    pass
            return li
        
        headlist = []        
        headlist.extend(recurse_get(self.nodes))
        return headlist      
	
