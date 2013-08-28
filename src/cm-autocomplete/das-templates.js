(function () {
    var templates = {"name": "xquery", "context": "xquery", "templates": [
        /*
        Currently in DASQL:

        lookup service-input | grep filter or selector, .. | unique | aggregator+

         */

        // operators
        {"name": "date last", "description": "filter by date (e.g. last 12 hours)\n Supported units for last operator are d (days), h (hours) and m(minutes).",
            "template": "date last ${type_a_number}h",
            "type": "service-input"},

       // {"name": "date", "description": "filter by date (between [start, end])",
       //     "template": "date BETWEEN [${start}, ${end}] hours"},

        {"name": "in", "description": "in filter [value1, value2, value3, ...], \n e.g. run IN [1, 2, 3]",
            "template": " in [ ${v1}, ${v2}, ${v3}]",
            "type": "service-input"},

        {"name": "between", "description": "between filter [start, end], e.g. run",
            "template": " between [${start}, ${end}] hours",
            "type": "service-input"},

        // aggregators
        {"name": "min", "description": "returns MIN value for the given field",
            "template": " min(${field})",
            "className": "CodeMirror-hint-function",
            "type": "aggregator"},

        {"name": "max", "description": "returns MAX value for the given field",
            "template": " max(${field})",
        "className": "CodeMirror-hint-function",
            "type": "aggregator"},

        {"name": "sum", "description": "return SUM of values for the given field",
            "template": " sum(${field})",
        "className": "CodeMirror-hint-function",
            "type": "aggregator"},

        {"name": "count", "description": "gives COUNT of *non null entries* for the given field",
            "template": " count(${field})",
        "className": "CodeMirror-hint-function",
            "type": "aggregator"},

        // TODO: preconditions
        // e.g. TODO: here precondition would be no |, and no other operators
        {"name": "unique", "description": "select only UNIQUE records\n (must go after a pipe sign |) ",
            "template": " unique",
        "className": "CodeMirror-hint-function",
            "type": "pipe"},


    ]};
    CodeMirror.templatesHint.addTemplates(templates);
})();