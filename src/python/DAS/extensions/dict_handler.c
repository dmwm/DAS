#include <stdio.h>
#include <Python.h>  /* Python header */
#include <regex.h>   /* Provides regular expression matching */

static int rematch(const char *re, const char *s)
{
    int result;
    regmatch_t match;
    regex_t compiled;
  
    /* compile the regular expression */
    result = regcomp (&compiled, re, REG_EXTENDED | REG_ICASE | REG_NOSUB);
    if (result) {
        char errorstring[128];
      
        regerror (result, &compiled, errorstring, sizeof(errorstring));
        regfree (&compiled);
        printf("Error %d \n", result);
        return result; // False
    }
  
    result = regexec (&compiled, s, 1, &match, 0);
    regfree (&compiled);
/*    printf("Match result=%d, input=%s, re=%s \n", result, s, re);*/
    return result; // 0 is True
}
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
    char *pat_int = "(^[0-9]$|^[0-9][0-9]*$)";
    char *pat_float = "(^[0-9]+.[0-9]*$|^[0-9]*.{1,1}[0-9]+$)";
    PyObject* dict;
    PyObject* map;
    PyObject* data = PyDict_New();
    if (!PyArg_ParseTuple(args, "OO", &dict, &map))
            return NULL;

    PyObject *iter = NULL, *res = NULL, *key = NULL, *item = NULL, *val = NULL;
    iter = PyObject_GetIter(dict);
    if (iter == NULL)
        return dict;

    const char* cstr;
    Py_ssize_t len;
/*    PyObject_AsReadBuffer(dict, &cstr, &len);*/
/*    printf("DIct object %s\n", cstr);*/
    while((item = PyIter_Next(iter))) {
        key = PyDict_GetItem(map, item);
        val = PyDict_GetItem(dict, item);

        if(PyObject_AsReadBuffer(val, &cstr, &len) == 0) {

            if  (rematch(pat_int, cstr) == 0) {
                res = PyInt_FromString(cstr, NULL, 10);
            } else if  (rematch(pat_float, cstr) == 0) {
                res = PyFloat_FromString(val, NULL);
                if  (res == NULL)
                    res = val;
            } else{
                res = val;
            }

        } else {
            PyErr_SetString(PyExc_ValueError, "could not convert value to buffer");
            break;
        }

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
