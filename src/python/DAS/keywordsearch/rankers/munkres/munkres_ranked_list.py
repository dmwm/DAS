__author__ = 'vidma'

import sys

# from munkres import Munkres, print_matrix

from DAS.keywordsearch.rankers.munkres.munkres import Munkres

INCL_INDEX = 0
EXCL_INDEX = 1
COST_INDEX = 2

UGLY_DEBUG = False


class MunkresRanked:
    """
    Internal structures:


    """
    matrix = []
    cost_matrix = []
    m = Munkres()


    def __init__(self, matrix, maximize=True):
        self.matrix = self.cost_matrix = matrix
        self.maximize = maximize

        # if we want to maximize the scores, invert them
        if self.maximize:
            self.cost_matrix = self.get_matrix_for_maximization(matrix)


    def print_solution(self, matrix, indexes):
        total = 0
        for row, column in indexes:
            value = matrix[row][column]
            total += value
            print '(%d, %d) -> %d' % (row, column, value)
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
        return sum([self.matrix[col][row]
                for row, col in indexes])


    def get_node_solution(self, node):
        (incl, excl, cost_so_far) = node
        sol = self.m.compute(self.cost_matrix, included_cells=incl,
                             excluded_cells= excl)

        if UGLY_DEBUG: print 'GOT_SOL', sol



        if not sol:
            return False



        all_cells = set(sol) | incl


        # filter out invalid solutions

        if len(all_cells) < len(self.matrix):
            return False
        # TODO: probably exclude shall be handled differently, that shall be
        # certain cells excluded...


        return {
            'cells': sol,
            # including the ones from node
            'all_cells':  all_cells,
            # total cost
            'cost': cost_so_far + self.get_total_cost(sol),
            'node': node
        }


    def empty_node(self):
        return (set(), set(), 0)


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
            for (row, col) in incl:
                print 'tranform_node, incl cell:', (row, col), 'cost:', \
                    self.cost_matrix[row][col]

        # new_cost = old_cost + self.cost_matrix[row][col] for cell in new_incl
        new_cost = sum([self.matrix[row][col]
                        for (row, col) in incl
        ]) + old_cost

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
        M_d = self.empty_node()
        min_cost_sol =self.get_node_solution(M_d)
        if DEBUG: self.print_solution(self.matrix, min_cost_sol['cells'])
        yield min_cost_sol

        # define partial nodes
        M_list = []
        solutions_all = []
        for i in xrange(k-1):
            # TODO: no need to recalc old ones... store costs
            #M_list = []

            M_i = M_d


            for cell in min_cost_sol['cells'][:-1]:

                cell = set([cell])
                # add node by appending to exlude

                M_list.append(self.transform_node(M_i, excl=cell))

                # update M_i
                M_i = self.transform_node(M_i, incl=cell)

                if DEBUG: print 'icell', cell, ',  M_i:', self.print_node(
                    M_i), ',  M_list:', self.print_nodes(M_list)

            if DEBUG:
                print 'created M_list:', self.print_nodes(M_list)

            # now solve each of the nodes
            solutions = [self.get_node_solution(node)
                         for node in M_list]

            # what if there is no solution (i.e. no cells available)
            # TODO: actually this we should know before-hand... as all nodes are supposed to be non-empty
            solutions = filter(None, solutions)

            # TODO: choose the best one, and add it's sub-nodes to node list


            if DEBUG: print 'SOLUTIONS:', solutions

            if not solutions:
                break

            # TODO: how about maximization problem
            # our costs are inverted, but we are summing up the real cost
            # to avoid overflows (WTF, in python integer unlimited)

            if self.maximize:
                min_cost_sol = max(solutions, key=lambda sol: sol['cost'])
            else:
                min_cost_sol = min(solutions, key=lambda sol: sol['cost'])

            if DEBUG: print 'Found one more min-cost solution:', min_cost_sol
            yield min_cost_sol


            # TODO: I do not need to recalculate old ones....
            if DEBUG: print 'M_list before remove:', M_list
            # remove all values... TODO: there shall not be duplicates...
            while False:
                try:
                    M_list.remove(min_cost_sol['node']) # remove the min-cost
                except:
                    break

            M_list.remove(min_cost_sol['node']) # remove the min-cost


            if DEBUG: print 'M_list after remove:', M_list

            # partition the M_d (currently best solution) by adding new node elements to it based on
            # TODO: what is M_i now!?? shall I take all in M_list?

            M_d = min_cost_sol['node']




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
            cost = sum([ matrix[row][col]
                         for row, col in cells])
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
    def test():
        matrix = [[5, 9, 1],
                  [10, 3, 2],
                  [8, 7, 4]]




        mr = MunkresRanked(matrix, maximize=False)
        # to calculate profit (maximize)
        # TODO: can we use floats?!!!
        #cost_matrix = mr.get_matrix_for_maximization(matrix)
        #
        print 'basic solution'
        indexes = mr.m.compute(mr.cost_matrix, )
        mr.print_solution(matrix, indexes)
        print 'included_cells=[(0, 0)] solution'

        mr = MunkresRanked(matrix, maximize=False)
        indexes = mr.m.compute(mr.cost_matrix, included_cells=[(0, 0)])
        mr.print_solution(matrix, indexes)

        print 'excluded cells...'
        mr = MunkresRanked(matrix, maximize=False)
        # TODO: just simple excluding list this DO NOT WORK
        # exclude by setting cost!!!
        indexes = mr.m.compute(mr.cost_matrix,
                               included_cells=set([(0, 2), (1, 1)]),
                               excluded_cells=set([(2, 0), (0, 0)]))
        mr.print_solution(matrix, indexes)

        mr = MunkresRanked(matrix, maximize=True)

        print 'trying k-best'
        for sol in mr.k_best_solutions(k=30):
            print 'sol detailed:', sol
            cells = set(sol['cells']) | sol['node'][INCL_INDEX]
            #print 'cells:', cells
            mr.print_solution(mr.matrix, cells)


        exh_res = exhaustive_search(matrix, maximize=False)

        mr = MunkresRanked(matrix, maximize=False)

        results = set()
        for sol in mr.k_best_solutions(k=300):
            print 'sol detailed:', sol
            cells = set(sol['cells']) | sol['node'][INCL_INDEX]
            cost = mr.get_total_cost(sol['cells'])
            r = set([(cost, tuple(cells))])
            results |= r
            #print 'cells:', cells
            mr.print_solution(mr.matrix, cells)

        print results == set(exh_res)


    test()

