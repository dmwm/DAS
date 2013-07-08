"""
Related implementations and documentation:
https://code.google.com/p/java-k-best/

Some info on Munkres Assignment: http://csclab.murraystate.edu/bob.pilgrim/445/munkres.html
"""


import sys

# from munkres import Munkres, print_matrix

from DAS.keywordsearch.rankers.munkres.munkres import Munkres

from itertools import imap

INCL_INDEX = 0
EXCL_INDEX = 1
COST_INDEX = 2

UGLY_DEBUG = False


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


class MunkresRanked:
    """
    This computes a Ranked List of K-best solutions for the Assignment Problem
    """

    def getf(self):
        maxfunc = min
        # initially Munkres is intended to minimize the total cost
        # if instead we want to maximize the scores, invert them
        if self._maximize:
            maxfunc = max
        maxfunc = min

        return maxfunc

    def __init__(self, matrix, maximize=False):
        self._matrix = self.cost_matrix = matrix
        self._maximize = maximize


        self.m = Munkres()
        self._partial_solutions = []


        self.maxfunc = min

        # initially Munkres is intended to minimize the total cost
        # if instead we want to maximize the scores, invert them
        if self._maximize:
            self.maxfunc = max
            self.cost_matrix = self.get_matrix_for_maximization(matrix)

    @staticmethod
    def print_solution(matrix, indexes):
        print indexes
        total = 0
        for row, column in indexes:
            value = matrix[row][column]
            total += value
            print '(%d, %d) -> %.2f' % (row, column, value)
        print 'total:', total


    def get_matrix_for_maximization(self, matrix):
        cost_matrix = []
        for row in matrix:
            cost_row = []
            for col in row:
                cost_row += [sys.maxint - col]
            cost_matrix += [cost_row]

        return cost_matrix


    def get_best_solution(self):
        indexes = self.m.compute(self.cost_matrix)
        return indexes

    def get_total_cost(self, indexes):
        return sum(self._matrix[row][col]
                    for row, col in indexes)


    def get_node_solution(self, node):
        """
        computes a solution for partial assignment problem defined by node
        """

        incl, excl, cost_so_far = node

        sol = self.m.compute(self.cost_matrix, included_cells=incl,
                             excluded_cells= excl)

        if UGLY_DEBUG: print 'GOT_SOL', sol

        if not sol:
            return False

        all_cells = set(sol) | incl

        # filter out invalid solutions
        # TODO: shouldn't this be moved into Munkres?
        # call: compute_partial() ?

        # TODO: non-rectangular matrix!!!
        if len(all_cells) < len(self._matrix):
            return False

        return {
            'cells': sol,
            # including the ones from node
            'all_cells':  all_cells,
            # total cost
            'cost': cost_so_far + self.get_total_cost(sol),
            'node': node
        }


    def empty_node(self):
        return set(), set(), 0


    def transform_node(self, old_node, incl=set(), excl=set()):
        """
        node - is a subset of all solutions that include and exclude
            specific cells (row, col)
        node = (incl, excl, cost_incl)
        """
        old_incl, old_excl, old_cost = old_node

        incl_or_excl = incl | excl
        upd_incl = old_incl | incl
        upd_excl = old_excl | excl

        # TODO: this can be faster, as we only need to add the new row
        # TODO: use old_cost
        if UGLY_DEBUG:
            for row, col in incl:
                print 'tranform_node, incl cell:', (row, col), 'cost:', \
                    self.cost_matrix[row][col]

        # new_cost = old_cost + self.cost_matrix[row][col] for cell in new_incl
        new_cost = sum(self._matrix[row][col]
                        for (row, col) in incl
                    ) + old_cost

        new_node = (upd_incl, upd_excl, new_cost)
        if UGLY_DEBUG:
            print 'tranform_node, input:', (old_node, incl, excl)
            print 'tranform_node, output_node:', new_node

        return new_node


    # TODO: or even just create a calc_incl_cost

    #def union_partial(self, p1, p2):

    def print_node(self, node):
        cost = node[2]
        #if self.maximize:
        #    cost = sys.maxint - cost

        return 'node(incl=%s, excl=%s, cost=%s)' % (
            str(list(node[0])), str(list(node[1])), str(cost))

    def print_nodes(self, nodes):
        return '\nNODE_LIST {\n    ' + \
               ';\n    '.join([self.print_node(node) for node in nodes]) + \
               '\n} '


    def k_best_solutions(self, k=10, DEBUG=False):
        """
        compute the K-best solutions
        """
        cur_node_M = self.empty_node()
        min_cost_sol = self.get_node_solution(cur_node_M)
        min_cost_sol['disabled'] = True
        self._partial_solutions.append(min_cost_sol)

        if DEBUG: MunkresRanked.print_solution(self._matrix, min_cost_sol['cells'])
        yield min_cost_sol

        # define partial nodes
        M_list = []
        for i in xrange(k-1):
            # TODO: no need to recalc old ones... store costs
            M_i = cur_node_M

            # TODO: check the paper, but it seem OK
            for cell in min_cost_sol['cells'][:-1]:


                cell = set([cell])

                # new node of M_i (required cells) + cell to exclude
                M_list.append(self.transform_node(M_i, excl=cell))

                # update M_i
                M_i = self.transform_node(M_i, incl=cell)

                if DEBUG: print 'icell', cell, ',  M_i:', self.print_node(
                    M_i), ',  M_list:', self.print_nodes(M_list)

            if DEBUG:  print 'created M_list:', self.print_nodes(M_list)

            # now solve each of the nodes
            new_solutions = [self.get_node_solution(node)
                         for node in M_list]
            if UGLY_DEBUG:
                print 'new solutions:'
                print '\n'.join(str(s) for s in new_solutions)

            # what if there is no solution (i.e. no cells available)
            # TODO: actually this we should know before-hand... as all nodes are supposed to be non-empty
            new_solutions = filter(None, new_solutions)

            if UGLY_DEBUG:
                print 'partial solutions before'
                print [self.print_node(s['node'])
                       for s in self._partial_solutions
                       if not s.get('disabled')]

            if True:
                # check for duplicates... TODO: shall this arrise?
                for sol in new_solutions:
                    if not any(set(s['all_cells']) == set(sol['all_cells'])
                               for s in self._partial_solutions):
                        self._partial_solutions.append(sol)
            else:
                self._partial_solutions.extend(new_solutions)

            if UGLY_DEBUG:
                print 'partial solutions in-between'
                print '\n'.join([str(self.print_node(s['node']))+':'+ str(s['cost'])
                       for s in self._partial_solutions
                       if not s.get('disabled')])


            # TODO: choose the best one, and add it's sub-nodes to node list

            if DEBUG: print 'NEW SOLUTIONS:', new_solutions



            # TODO: disabled could be more performant...
            not_used_sols = (s for s in self._partial_solutions
                             if not s.get('disabled', False))
            try:
                min_cost_sol = self.maxfunc(not_used_sols,
                                            key=lambda sol: sol['cost'])
            except ValueError, e:
                # expect: ValueError: min() arg is an empty sequence

                print 'Finished', e
                break

            if DEBUG: print 'Found one more min-cost solution:', min_cost_sol
            yield min_cost_sol


            # remove the current min-cost from partial solutions
            min_cost_sol['disabled'] = True

            if UGLY_DEBUG:
                print 'partial solutions after removing current best:'
                print '\n'.join([self.print_node(s['node'])
                       for s in self._partial_solutions
                       if not s.get('disabled')])


            # partition the M_d (currently best solution) by adding new node elements to it based on

            cur_node_M = min_cost_sol['node']




def permute(a, results):
    if len(a) == 1:
        results.insert(len(results), a)

    else:
        for i in range(0, len(a)):
            element = a[i]
            a_copy = [a[j] for j in range(0, len(a)) if j != i]
            subresults = []
            permute(a_copy, subresults)
            for subresult in subresults:
                result = [element] + subresult
                results.insert(len(results), result)


def exhaustive_search(matrix, k=10, maximize= False):

    permutations = []
    permute(range(len(matrix)), permutations) # [0, 1, 2] for a 3x3 matrix

    print permutations

    import heapq
    from heapq import heappushpop, heappush
    def cmp_gt(x, y):
    # Use __lt__ if available; otherwise, try __le__.
    # In Py3.x, only __lt__ will be called.
        return (x > y) if hasattr(x, '__gt__') else (not y >= x)

    if not maximize:
        heapq.cmp_lt = cmp_gt

    results = []
    # TODO: get proper permutations. itertools?
    for i, rows in enumerate(permutations):
        for cols in permutations:
            cells = zip(rows, cols)
            cost = sum(matrix[row][col]
                         for row, col in cells)
            r = (cost, tuple(set(cells)))

            # TODO: this method results in duplicates...
            if len(results) >= k*100:
                heappushpop(results, r)
            else:
                heappush(results, r)


    results = tuple(set(results))
    print sorted(results, reverse=maximize, key=lambda item: item[0])
    return results







if __name__ == '__main__':
    def test_munkres():
        matrix = matrix55_0
        m = Munkres()
        print matrix
        print('Best solution:')
        MunkresRanked.print_solution(matrix, m.compute(matrix))


        '''
        vidma@vidma-laptop:/storage/DAS/DAS_code/DAS/munkres/java-k-best-1.00/src$ java com.google.code.javakbest.Test
        Solution 0 (1.78051973502884):
        (0, 1)	(1, 2)	(2, 3)	(3, 4)	(4, 0)
        Solution 1 (1.916943031547661):
        (0, 4)	(1, 2)	(2, 3)	(3, 0)	(4, 1)
        Solution 2 (2.0714331273548563):
        (0, 0)	(1, 2)	(2, 3)	(3, 4)	(4, 1)
        Solution 3 (2.3604129886857486):
        (0, 3)	(1, 2)	(2, 4)	(3, 0)	(4, 1)
        Solution 4 (2.421300962484084):
        (0, 2)	(1, 4)	(2, 3)	(3, 0)	(4, 1)	'''


    def test_ranked_assignment_float():
        matrix = matrix55_0

        mr = MunkresRanked(matrix, maximize=False)

        print 'trying k-best'
        sols = []
        for sol in mr.k_best_solutions(k=30):
            print 'sol detailed:', sol
            cells = set(sol['cells']) | sol['node'][INCL_INDEX]
            #print 'cells:', cells
            cost = mr.get_total_cost(cells)
            mr.print_solution(mr._matrix, cells)
            sols.append(cost)


        # TODO: we now get repetitions...!

    def test():
        matrix = [[5, 9, 1],
                  [10, 3, 2],
                  [8, 7, 4]]

        mr = MunkresRanked(matrix, maximize=False)

        results = set()
        for sol in mr.k_best_solutions(k=300):
            print 'sol detailed:', sol
            cells = set(sol['cells']) | sol['node'][INCL_INDEX]
            cost = mr.get_total_cost(sol['cells'])
            r = set([(cost, tuple(cells))])
            results |= r
            #print 'cells:', cells
            mr.print_solution(mr._matrix, cells)

        #print results == set(exh_res)


    #test_munkres()

    test()
    test_ranked_assignment_float()

