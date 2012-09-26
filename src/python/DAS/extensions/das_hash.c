#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <Python.h>
#define SIZE 16
/*
 * C-version of das_hash function, see
 * http://www.cse.yorku.ca/~oz/hash.html
 * The both implementation sdbm and djb2 compute a polynomial function of
 * given power by use of Horner's rule, e.g. if power is 33 then formula is
 * h[i] = h[i-1]*33 + c, see
 * http://en.wikipedia.org/wiki/Horner's_method
 */

/* sdbm implementation
 * hash[i] = hash[i-1]*65599 + c
 * this formula can be represented as
 * hash = c + (hash<<6) + (hash<<16) - hash
 * which is eqivalent to
 * hash = c + hash*(2**6 + 2**16 - 1)
 */
/*
static unsigned long das_hash(unsigned char *str)
{
    unsigned long hash = 0;
    int c;
    while( (c = *str++) )
        hash = c + (hash << 6) + (hash << 16) - hash;
    return hash;
}
*/
/* djb2 implementation
 * hash[i] = hash[i-1]*33 + c
 * this formula can be represented as
 * hash = c + (hash+(hash<<5))
 * which is eqivalent to
 * hash = c + hash*(1+2**5)
 */
static unsigned long das_hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;
    while( (c = *str++) )
        hash = ((hash << 5) + hash) + c;
    return hash;
}

static PyObject*
_das_hash(PyObject *self, PyObject *args)
{
    unsigned char *user_input;
    /* parse input argument as string and store into user_input */
    if (!PyArg_ParseTuple(args, "s", &user_input))
            return NULL;

    /* create PyString object from das hash */
    unsigned long res = das_hash(user_input);
    char buf[SIZE+1];
    sprintf(buf, "%016lx", res); /* always get 16 digits for hex, fill with leading zeros */
    PyObject* data = PyString_FromStringAndSize(buf, SIZE);

    PyErr_Clear();
    return data;
}

static PyMethodDef _Methods[] = {
    {"_das_hash", _das_hash, METH_VARARGS, "DAS hash function."},
    {NULL,          NULL}           /* sentinel */
};

PyMODINIT_FUNC
initdas_hash(void)
{
    (void) Py_InitModule("das_hash", _Methods);
}

int
main(int argc, char *argv[])
{
    /* Pass argv[0] to the Python interpreter */
    Py_SetProgramName(argv[0]);

    /* Initialize the Python interpreter.  Required. */
    Py_Initialize();

    /* Add a static module */
    initdas_hash();

    return 0;
}
