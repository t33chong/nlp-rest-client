from __future__ import division
from preprocessing import preprocess
from preprocessing import build_dict_for_simple_lookup, build_dict_with_term_counts

class BinaryField(object):
    def __init__(self, terms_from_field):
        """Class whose main purpose is to build a simple lookup dictionary from
        a list of terms in a given field, and to save it in order to facilitate
        binary scoring.
        Applicable fields: hostname, sitename, headline, inferred_topic_score"""
        self.d = build_dict_for_simple_lookup(terms_from_field)

    def score(self, candidate):
        """Return a score based on the binary presence or absence of a given
        candidate in a simple lookup dictionary for a certain field. """
        return self.d.get(candidate, 0.0)

class TermFreqField(object):
    def __init__(self, terms_from_field):
        """Class whose main purpose is to build a term count dictionary from a
        list of terms in a given field, and to save the maximum term count in
        order to facilitate term frequency calculations.
        Applicable fields: domains, description, top titles, top categories"""
        self.d = build_dict_with_term_counts(terms_from_field)
        self.max_score = max(self.d.values())

    def score(self, candidate):
        """Return a score based on the term frequency of a given candidate in a
        term count dictionary for a certain field."""
        return self.d.get(candidate, 0.0) / self.max_score

if __name__ == '__main__':
    foo = BinaryField(['bar'])
    assert foo.score(('bar',)) == 1.0
    assert foo.score(('baz',)) == 0.0

    foo = TermFreqField(['bar', 'bar', 'baz'])
    assert foo.score(('bar',)) == 1.0
    assert foo.score(('baz',)) == 0.5
    assert foo.score(('luhrmann',)) == 0.0
