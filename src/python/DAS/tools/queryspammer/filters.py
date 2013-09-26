#pylint: disable-msg=R0903
"Text mutators"

import multiprocessing
import random
import string
LOG = multiprocessing.get_logger()

class Filter(object):
    "Base class for text mutators"
    def __init__(self, probability):
        self.probability = probability
        LOG.info('Initialised filter::%s (probability %s)'% \
                 (self.__class__.__name__,self.probability))
    def __call__(self, arg):
        if random.random() < self.probability:
            LOG.info('Activated %s'%self.__class__.__name__)
            return self.filter(arg)
        else:
            return arg
    def filter(self, arg):
        """Filter method"""
        return arg

class WordRearrangeFilter(Filter):
    "Switches two adjacent words"
    def filter(self, arg):
        """Filter method"""
        words = arg.split(' ')
        index = random.randint(0, len(words)-2)
        words[index] = (words[index], words[index+1])
        words[index+1] = words[index][0]
        words[index] = words[index][1]
        return ' '.join(words)

class RandomSpacesFilter(Filter):
    "Either doubles or removes a space"
    def filter(self, arg):
        """Filter method"""
        space_index = [i for i in xrange(len(arg)) if arg[i] == ' ']
        if len(space_index)>0:
            index = random.choice(space_index)
            if random.random()>0.5:
                return arg[:index] + ' ' + arg[index:]
            else:
                return arg[:index] + arg[index+1:]
        else:
            return arg

class WrongQuotationFilter(Filter):
    "Swaps single and double quotes"
    def filter(self, arg):
        """Filter method"""
        quote_index = [i for i in xrange(len(arg)) if arg[i] in ('"',"'")]
        if len(quote_index)>0:
            index = random.choice(quote_index)
            if arg[index] == '"':
                return arg[:index] + "'" + arg[index+1:]
            elif arg[index] == "'":
                return arg[:index] + '"' + arg[index+1:]
        else:
            return arg

class TruncationFilter(Filter):
    "Cuts off the last few characters"
    def filter(self, arg):
        """Filter method"""
        return arg[:-random.randint(1, 8)]

class BadSpellingFilter(Filter):
    "Switches alphabetic characters"
    def filter(self, arg):
        """Filter method"""
        char_index = [i for i in xrange(len(arg)) if arg[i] in string.letters]
        if len(char_index)>0:
            index = random.choice(char_index)
            return arg[:index] + random.choice(string.letters) + arg[index+1:]
        else:
            return arg

def list_filters():
    "List all filter classes"
    return [k
            for k, v in globals().items()
            if type(v)==type(type)
            and issubclass(v, Filter)
            and not v==Filter]
