"""
Code based on Ranked-assignment paper (TODO: ref)
Ranked List of K-best solutions for the Assignment Problem

Modification to support contextualization partially based on KEYRY paper.


Related implementations and documentation:
https://code.google.com/p/java-k-best/

Some other info on Munkres Assignment:
http://csclab.murraystate.edu/bob.pilgrim/445/munkres.html
"""


from DAS.keywordsearch.rankers.munkres.munkres import Munkres


INCL_INDEX = 0
EXCL_INDEX = 1
COST_INDEX = 2



UGLY_DEBUG = False
DEBUG=False


class MunkresRanked:
    """
    This computes a Ranked List of K-best solutions for the Assignment Problem
    """
    #maxnum = 10000.0


    def getf(self):
        """
        initially Munkres is intended to minimize the total cost
        # if instead we want to maximize the scores, invert them
        """
        maxfunc = min
        if self._maximize:
            maxfunc = max
        maxfunc = min

        return maxfunc

    def __init__(self, matrix, maximize=False, maxnum=1000.0):
        #TODO: at the moment maximization works only if maxnum is much smaller than maxint

        self._matrix = self.cost_matrix = matrix
        self._maximize = maximize
        self.maxnum = maxnum #sys.maxint



        self.m = Munkres()
        self._partial_solutions = []


        self.minfunc = min

        # initially Munkres is intended to minimize the total cost
        # if instead we want to maximize the scores, invert them
        if self._maximize:
            self.minfunc = max
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
                cost_row += [self.maxnum - col]
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
                    self.cost_matrix[row][col], \
                    ' matrix:', self._matrix[row][col]

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


    def _debug_partial_solutions(self, txt):
        if UGLY_DEBUG:
            print 'partial solutions '+ txt
            print [str(self.print_node(s['node']))+':'+ str(s['cost'])
                   for s in self._partial_solutions
                   if not s.get('disabled')]

    def k_best_solutions(self, k=10):
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

            self._debug_partial_solutions('before')

            # TODO: for some reason we are getting duplicates, but if this filtering is active all works fine!
            if True:
                # check for duplicates... TODO: shall this arrise?
                for sol in new_solutions:
                    if not any(set(s['all_cells']) == set(sol['all_cells'])
                               for s in self._partial_solutions):
                        self._partial_solutions.append(sol)
            else:
                self._partial_solutions.extend(new_solutions)

            self._debug_partial_solutions('in between')


            # find the best solution, and add it's sub-nodes to node list

            if DEBUG: print 'NEW SOLUTIONS:', new_solutions


            # TODO: disabled could be more performant...
            not_used_sols = (s for s in self._partial_solutions
                             if not s.get('disabled', False))
            try:
                min_cost_sol = self.minfunc(not_used_sols,
                                            key=lambda sol: sol['cost'])
            except ValueError, e:
                # expect: ValueError: min() arg is an empty sequence

                if UGLY_DEBUG: print 'Finished', e
                break

            if DEBUG: print 'Found one more min-cost solution:', min_cost_sol
            yield min_cost_sol


            # remove the current min-cost from partial solutions
            min_cost_sol['disabled'] = True

            self._debug_partial_solutions('after')

            # partition the M_d (currently best solution) by adding new node elements to it based on

            cur_node_M = min_cost_sol['node']




if __name__ == '__main__':
    import munkres_list_test
    munkres_list_test.run_tests()
    # TODO: inverted direction do not allways work? problem with maxint?
    # well well, asssuming that values are between 0.0-1.0 we may easily overcome this...


