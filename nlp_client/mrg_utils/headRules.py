#    headRules.py
#    Rules for collins-style head selection based on Marneffe et al (2006), useful functions
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


traces = ['-NONE-', '*T*-1', '*T*-2', '*T*', '*', '*-1', '*-2', '*?*', '*T*-3', '*RNR*-2', '*RNR*-1', '*ICH*-1', '*ICH*-2', '*ICH*-3', '*T*-4', '*T*-5', '*T*-224', '*ICH*']

semHeadRules = {\
"NP":[  ('RightUn', ["NN", "NNP", "NNPS", "NNS", "JJR", "FW"]), \
        ('Left', ["NP","PRP","QP"]),\
        ('RightUn', ["$","ADJP", "PRN"]),\
        ('Right', ["CD"]),\
        ('RightUn', ["JJ", "JJS", "NX", "RB", "QP", "VB", "DT", "WDT", "RBR", "ADVP"]),\
        ('Left', ["POS"]),\
        ('LeftIm', [])],\
\
"NX":[('LeftIm', [])],\
\
"WHNP":[('RightUn', ["NN", "NNP", "NNPS", "NNS", "NX", "POS", "JJR"]),\
        ('Left', ["NP"]),\
        ('RightUn', ["$","ADJP", "PRN"]),\
        ('Right', ["CD"]),\
        ('RightUn', ["JJ", "JJS", "RB", "QP"]),\
        ('Left', ["WHNP", "WHPP", "WHADJP", "WP$", "WP", "WDT"]),\
        ('LeftIm', [])],\
\
"ADJP":[('Right', ["$", "CD", "JJ", "NNS", "NN", "QP", "VBN", "VBG", "ADJP", "JJR", "NP", "JJS", "DT", "FW", "RBR", "RBS", "SBAR", "RB"]),\
        ('RightIm', [])],\
\
"INTJ":[('RightIm', [])],\
\
"LST":[('Left', ["LS",":"]),('LeftIm', [])],\
\
"NAC":[('Right', ["NN","NNS","NNP","NNPS","NP","NAC","EX","$","CD","QP", "PRP","VBG","JJ","JJS","JJR","ADJP","FW"]),('RightIm', [])],\
\
"PP":[('Left', ["IN","TO","VBG","VBN","RP","FW","X"]),('LeftIm',[])],\
\
"DOC":[('LeftUn', ["null","S","SINV","SQ"]),('LeftIm', [])], #(* special case for discourse processing *)\
\
"null":[('LeftUn', ["S","SINV","SQ","NP","UCP","VP","FRAG","SBARQ","SBAR"]),('LeftIm', [])],\
\
"NONE":[('RightIm',[])],
\
"PRN":[('Right', ["S", "SQ", "SINV", "SBAR", "FRAG", "NP", "WHNP", "WHPP", "WHADVP", "WHADJP", 
"IN", "DT", "VP", "PP", "ADVP", "ADJP", "SBARQ"]), ('RightIm', [])],\
\
"PRT":[('Left', ["RP"]),('LeftIm', [])],\
\
"RRC":[('Left', ["VP","NP","ADVP","ADJP","PP"]),('LeftIm', [])],\
\
"ADVP":[('Left', ["RB","RBR","RBS","FW","ADVP","TO","CD","JJR","JJ","IN","NP","JJS","NN"]),('LeftIm', [])],\
\
"QP":[('Right', ["$", "CD", "IN", "NNS", "NN", "JJ", "PDT", "DT", "RB", "NCD", "QP", "JJR", "JJS"]),('RightIm',[])],\
\
"S":[('Left', ["VP", "S", "FRAG", "SBAR", "ADJP", "UCP", "TO","SINV","SBARQ"]), ('Right', ["NP"]),('LeftIm', [])],\
\
"SQ":[('Left', ["VP", "SQ", "VB", "VBZ", "VBD", "VBP", "MD"]),('RightIm',[])],\
\
"SBAR":[('Left', ["S", "SQ", "SINV", "SBAR", "FRAG", "WHNP", "WHPP", "WHADVP", "WHADJP", "IN", "DT", "X"]),('RightIm',[])],\
\
"SBARQ":[('Left', ["SQ","S","SINV","SBARQ","FRAG"]),('RightIm',[])],\
\
"SINV":[('Right', ["VP","VBZ","VBD","VBP","VB","MD","S","SINV","ADJP","NP"]),('RightIm',[])],\
\
"TOP":[('Right', ["S","SBAR","SINV","SBARQ","SQ"]),('RightIm',[])],\
\
"VP":[('Left',["VBD","VBN","MD","VBZ","VB","VBG","VBP","AUX","AUXG","JJ","NN","NNS","VP","S","SBAR","SINV","ADJP","TO","NP","PP"]),('LeftIm',[])],\
\
"WHADJP":[('Right', ["CC","WRB","JJ","ADJP"]),('RightIm',[])],\
\
"WHADVP":[('Left', ["CC","WRB"]),('LeftIm',[])],\
\
"WHNP":[('Right', ["WDT","WP","WP$","WHADJP","WHPP","WHNP"]),('RightIm',[])],\
\
"WHPP":[('Left', ["IN","TO","FW"]),('LeftIm',[])],\
\
"UCP":[('Right', ["S", "SQ", "SINV", "SBAR", "FRAG", "WHNP", "WHPP", "WHADVP", "WHADJP", "IN", "DT"]), ('RightIm', [])],\
\
"CONJP":[('Left', ["TO", "RB", "IN", "CC"]),('LeftIm',[])],\
\
"X":[('RightIm',[])],\
\
":":[('RightIm',[])],\
\
"SYM":[('RightIm',[])],\
\
"FRAG":[('Right', ["S", "SQ", "SINV", "SBAR", "FRAG", "WHNP", "WHPP", "WHADVP", "WHADJP", "NP", 
"ADVP","ADJP","RB","WRB", "IN", "NN", "NNP",  "DT", "PP"]), ('RightIm', [])]\
}


def RightIm(aNode, aList):
    if aNode.children[-1].isTerminal:
        try:
            aNode.children[-1].depLink.index
            return aNode.children[-1]
        except:
            if len(aNode.children) == 1:
                return aNode.children[-1]
            return aNode.children[-2]
    try:
        traces.index(aNode.children[-1].string)
        try:
            traces.index(aNode.children[-2].string)
            try:
                traces.index(aNode.children[-3].string)
            except:
                return aNode.children[-3]
        except:
            return aNode.children[-2]
    except:
        return aNode.children[-1]

def LeftIm(aNode, aList):
    try:
        traces.index(aNode.children[0].string)
        try:
            traces.index(aNode.children[1].string)
            try:
                traces.index(aNode.children[2].string)
            except:
                return aNode.children[2]
        except:
            return aNode.children[1]
    except:
        return aNode.children[0]
    
def RightUn(aNode, aList):
    x = [y for y in aNode.children]
    x.reverse()
    for z in x:
        try:
            aList.index(z.pos.split('-')[0])
            try:
                traces.index(z.string)
                if len(z.children) == 1:
                    continue
                if z.isTerminal:
                    continue
                for b in [a.string for a in z.children]:
                    if b not in traces:
                        return z
            except:
                return z
        except:
            pass

def LeftUn(aNode, aList):
    for child in aNode.children:
        try:
            aList.index(child.pos.split('-')[0])
            return child
        except:
            pass

def Right(aNode, aList):
    x = [y for y in aNode.children]
    x.reverse()
    for pos in aList:
        for z in x:
            try:
                [pos].index(z.pos.split('-')[0])
                try:
                    traces.index(z.string)
                    if len(z.children) == 1:
                        continue
                    if z.isTerminal:
                        continue
                    for b in [a.string for a in z.children]:
                        if b not in traces:
                            return z
                except:
                    return z
            except:
                pass

def Left(aNode, aList):
    for pos in aList:
        for child in aNode.children:
            try:
                [pos].index(child.pos.split('-')[0])
                try:
                    traces.index(child.string.split(' ')[0])
                    if child.isTerminal:
                        continue
                    for b in [a.string for a in child.children]:
                        if b not in traces:
                            return child
                except:
                    return child
            except:
                pass

                

headRules = {'RightIm':RightIm, 'LeftIm':LeftIm, 'RightUn':RightUn, 'LeftUn':LeftUn, 'Left':Left, 'Right':Right}


copulas = ["be","am", "are", "is", "was", "were", "'m", "'re", "'s", "s", "seem", "seems", 
				   "seemed", "appear", "appears", "appeared", "stay", "stays", "stayed", "remain",
				   "remains", "remained", "resemble", "resembles", "resembled", "become", "became", "becomes"]
				   
auxiliaries = ["will", "wo", "shall", "may", "might", "should", "would", "can", "could", "ca", 
			       "must", "has", "have", "had", "having", "being", "been", "get", "gets", 
			       "getting", "got", "gotten", "do", "does", "did", "to", "'ve", "'d", "'ll"]

auxtags = ["TO", "MD", "VB", "VBD", "VBP", "VBZ", "VBG", "VBN", "AUX", "AUXG"]


auxCase = [('Left', ["VP", "ADJP","VBD","VBN","MD","VBZ","VB","VBG","VBP", "AUX", "AUXG", "JJ", "NN", "NNS", "VP", "S", "SBAR", "SINV"]), ('LeftIm',[])]

sqCop = [('Right', ["VP", "ADJP", "NP", "AHADJP", "WHNP"]), ('RightIm', [])]

vpCop = [('Left', ["VP", "ADJP", "NP", "AHADJP", "WHNP"]), ('LeftIm',[])]

npCoord = [('LeftUn', ["NN", "NNP", "NNPS", "NNS", "NX", "JJR","$","CD"]), ('LeftIm',[])]


def hasAux(node):
    for child in node.children:
        try:
            auxtags.index(child.pos)
            try:
                for chi in child.children:
                    try:
                        auxiliaries.index(chi.string.lower())
                        return True
                    except:
                        pass
            except:
                try:
                    auxiliaries.index(child.string.lower())
                    return True
                except:
                    pass
        except:
            pass
    return False


def hasCop(node):
    for child in node.children:
        try:
            auxtags.index(child.pos)
            try:
                for chi in child.children:
                    try:
                        copulas.index(chi.string.lower())
                        return True
                    except:
                        pass
            except:
                try:
                    copulas.index(child.string.lower())
                    return True
                except:
                    pass
        except:
            pass
    return False



def pickCoordHead(node):
    global npCoord
    for tup in npCoord:
        try:
           headRules[tup[0]](node, tup[1]).string
           return headRules[tup[0]](node, tup[1])
        except:
            pass


def isCoord(node):
    for child in node.children:
        try:
            ['CC', ','].index(child.pos.split('-')[0])
            for x in node.children:
                try:
                    ["NP", "VP", "SBAR"].index(x.pos.split('-')[0])
                    return False
                except:
                    pass
            return True
        except:
            pass
    return False
    
def getTermHead(aHead, aParent):
    if aHead.isTerminal:
        aHead.headOf.append(aParent)
        return aHead
    else:
        return getTermHead(aHead.head, aParent)
