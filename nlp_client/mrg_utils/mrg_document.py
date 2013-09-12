#    mrg_document.py
#    Class definition for MRG_Document
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



from sexpr_parse import *
from cuecorp import *

##################################################
# Class for each .mrg document, containing a list# 
# of sentences, all heads within the document,   #
# all terminal heads in the document,            #
# and all words within the document.             #
##################################################

from sentence import *
from docNode import *
from nonTerminalNode import *

class MRG_Document:
    def __init__(self, path):
        #path variable should be absolute path of .mrg file
        self.sentences = self.get_sentences(path)
        self.heads = self.getHeads()
        self.allWords = self.getAllWords()
        self.termHeads = self.getTermHeads()
        self.doc = self.getDoc()


    # Method for getting all terminal node objects available
    # within the document
    def getAllWords(self, traces=False):  #change traces to True to get traces
        li = []
        if traces:
            for sent in self.sentences:
                li += sent.nodes.flat
        else:
            for sent in self.sentences:
                li += sent.nodes.flat2
        return li


    # Creates a document node which governs all root nodes 
    # for each sentence
    def getDoc(self):
        li = []
        for s in self.sentences:
            try:
                li.append(s.nodes)
            except:
                pass
        return DocNode(li)
        
    # Return a list of syntactic heads--useful for machine learning tasks.    
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
        for s in self.sentences:
            try:
                headlist.extend(recurse_get(s.nodes))
            except:
                pass
        return headlist

    # Return a list of terminal syntactic heads, also useful for ML
    def getTermHeads(self):
        headList = []
        for s in self.sentences:
            try:
                for node in s.nodes.flat:
                    if node.isTerminal:
                        if node.pos.split('-')[0] in set([a.pos for a in self.allWords]):
                            if node.oneUp.head == node:
                                headList.append(node)
            except:
                pass
        return headList


    # Create sentence object for each .mrg-style parse
    def get_sentences(self, path):
        counter = 0
        lines = open(path).readlines()
        sent_list = '!!('.join(''.join(lines).split('\n(')).split('!!')
        sentences = []
        for s in sent_list:
            if s != '':
                sentences.append(Sentence(s, counter))
                counter += 1
        return sentences