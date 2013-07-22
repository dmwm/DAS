(function () {
    var templates = {"name": "xquery", "context": "xquery", "templates": [

        // operators
        // TODO: what if there is no date yet, and vice-versa...
        {"name": "last", "description": "filter by date (e.g. last 12 hours)\n Supported units for last operator are d (days), h (hours) and m(minutes).",
            "template": " LAST ${type_a_number}h"},

       // {"name": "date", "description": "filter by date (between [start, end])",
       //     "template": "date BETWEEN [${start}, ${end}] hours"},

        {"name": "in", "description": "in filter [value1, value2, value3, ...], \n e.g. run IN [1, 2, 3]",
            "template": " in [ ${v1}, ${v2}, ${v3}]"},

        {"name": "between", "description": "between filter [start, end], e.g. run",
            "template": " between [${start}, ${end}]"},

        // aggregators
        {"name": "min", "description": "returns MIN value for the given field",
            "template": " min(${field})"},

        {"name": "max", "description": "returns MAX value for the given field",
            "template": " max(${field})"},

        {"name": "sum", "description": "return SUM of values for the given field",
            "template": " sum(${field})"},

        {"name": "count", "description": "gives COUNT of *non null entries* for the given field",
            "template": " count(${field})"},

        // TODO: preconditions
        // e.g. TODO: here precondition would be no |, and no other operators
        {"name": "unique", "description": "select only UNIQUE records\n (must go after a pipe sign |) ",
            "template": " unique"},


    ]};
    CodeMirror.templatesHint.addTemplates(templates);
})();