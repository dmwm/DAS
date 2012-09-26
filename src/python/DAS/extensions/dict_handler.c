#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <Python.h>
/*
 * C-version of dict_helper from utils/utils.py
 * We need to perform mapping of parsed record into DAS notations
 * Can be used similar to dict_helper, e.g.
 * _dict_handler(idict, notations)
 * where first argument is data-service record and second is
 * notation map (dict).
 */
static PyObject*
_dict_handler(PyObject *self, PyObject *args)
{
    PyObject* dict;
    PyObject* map;
    PyObject* data = PyDict_New();
    if (!PyArg_ParseTuple(args, "OO", &dict, &map))
            return NULL;

    PyObject *key = NULL, *val = NULL;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &key, &val)) {
        PyObject *mkey = PyDict_GetItem(map, key);
        PyObject *res = NULL;
        if ( PyString_AsString(val) == NULL ) {
            /*case when val is not a string, e.g. int*/
            res = val;
        } else {
            char *dotch = strchr(PyString_AsString(val), '.'); /* search for . character */
            if ( dotch != NULL ) {
                /* case if val string contains dot, PyFloat may return none
                 * if unable to convert the value */
                res = PyFloat_FromString(val, NULL);
            } else {
                res = PyInt_FromString(PyString_AsString(val), NULL, 0);
            }
        }
        int decr = 1; /* reference counter */
        if (res == NULL) {
            res  = val;
            decr = 0; /* if we unable to convert decrement ref counter */
        }

        if  (mkey != NULL) {
            PyDict_SetItem(data, mkey, res);
            Py_XDECREF(mkey);
        } else {
            PyDict_SetItem(data, key, res);
        }
        if (decr == 1) {
            Py_XDECREF(res);
        }
    }
    PyErr_Clear();
    return data;
}

static PyMethodDef _Methods[] = {
    {"_dict_handler", _dict_handler, METH_VARARGS,
     "Create new dictionary from provided one and notation map."},
    {NULL,          NULL}           /* sentinel */
};

PyMODINIT_FUNC
initdas_speed_utils(void)
{
    (void) Py_InitModule("das_speed_utils", _Methods);
}

int
main(int argc, char *argv[])
{
    /* Pass argv[0] to the Python interpreter */
    Py_SetProgramName(argv[0]);

    /* Initialize the Python interpreter.  Required. */
    Py_Initialize();

    /* Add a static module */
    initdas_speed_utils();

    return 0;
}
