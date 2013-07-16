#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for Ranked Munkres Assignment module
"""

import unittest

from DAS.keywordsearch.rankers.munkres.munkres_ranked_list import MunkresRanked, \
    exhaustive_search

def parse_str_matrix(mstr):
    return [
            [float(cell.strip())
             for cell in l.split('\t')
             if cell.strip()  ]
             for l in mstr.split('\n')
             if l.strip()
    ]


matrix55_0 = parse_str_matrix(
    '''0.376573	0.012995	0.596978	0.609910	0.553592
    0.850493	0.621621	0.301854	0.502297	0.762826
    0.895802	0.871357	0.874034	0.505582	0.892734
    0.312408	0.597709	0.201495	0.866165	0.643917
    0.316172	0.243507	0.952605	0.262790	0.787490''')

matrix_33 = [[5, 9, 1],
            [10, 3, 2],
            [8, 7, 4]]


class Test_Munkres_Ranked(unittest.TestCase):
    """
    A test class for the DotDict module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        pass

    def _test_result_equality(self, matrix, maximize, K=50):


        mr = MunkresRanked(matrix, maximize=maximize)

        sols = []
        for sol in mr.k_best_solutions(k=K):
            #print 'sol detailed:', sol
            cells = sol['all_cells']
            #print 'cells:', cells
            cost = mr.get_total_cost(cells)
            #mr.print_solution(mr._matrix, cells)
            r = (cost, cells)
            sols.append(r)

        expected = exhaustive_search(matrix, k=None, maximize=maximize)

        keyfunc = lambda it: (it[0], str(it[1]))
        expected = sorted(expected, reverse=maximize, key=keyfunc)[:K]
        sols = sorted(sols, reverse=maximize, key=keyfunc)
        #print sols

        f_round = lambda (cost, cells): ('%.6f' % cost, cells)

        sols = map(f_round, sols)
        expected = map(f_round, expected)

        self.assertEqual(expected, sols)

    def test_ranked_assignment_float(self):
        self._test_result_equality(matrix = matrix55_0, maximize=False)

    def test_ranked_assignment_int(self):
        self._test_result_equality(matrix = matrix_33, maximize=False)

    def test_ranked_assignment_float_desc(self):
        self._test_result_equality(matrix = matrix55_0, maximize=True)

    def test_ranked_assignment_int_desc(self):
        self._test_result_equality(matrix = matrix_33, maximize=True)




#
# main
#
if __name__ == '__main__':
    unittest.main()

