#include <stdio.h>
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

    PyObject *iter = NULL, *res = NULL, *key = NULL, *item = NULL;
    iter = PyObject_GetIter(dict);
    if (iter == NULL)
        return dict;

    while((item = PyIter_Next(iter))) {
        key = PyDict_GetItem(map, item);
        res = PyDict_GetItem(dict, item);
        if  (key!= NULL) {
            PyDict_SetItem(data, key, res);
        } else {
            PyDict_SetItem(data, item, res);
        }
        Py_XDECREF(item);
    }

    Py_XDECREF(iter);
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
}
