/*
TODO:  ajax hints e.g. primary_dataset, dataset
first provide the entity you are searching for (use-Tab)
or attr=value pairs for filtering

TODO: may value have a ","?
 */
(function () {
    Array.prototype.extend_array = function (array) {
        this.push.apply(this, array);
    };


    function forEach(arr, f) {
        for (var i = 0, e = arr.length; i < e; ++i) f(arr[i]);
    }

    function arrayContains(arr, item) {
        if (!Array.prototype.indexOf) {
            var i = arr.length;
            while (i--) {
                if (arr[i] === item) {
                    return true;
                }
            }
            return false;
        }
        return arr.indexOf(item) != -1;
    }


    function getStartsWith(cur, token, startIndex) {
        var length = cur.ch - token.start;
        if (!startIndex)
            startIndex = 0;
        var startsWith = token.string.substring(startIndex, length);
        return trim(startsWith);
    }

    function trim(str) {
        if (str.trim) {
            return str.trim();
        }
        return str.replace(/^\s+|\s+$/g, '');
    }


    function extractContext(token, editor, cur, getToken) {
        var tprop = token;

        //console.log('hint for token=', token, 'token.value_for=', token.state.value_for);


        console.log('tprop0:', tprop);

        // If it is a property, find out what it is a property of.
        if (tprop.className == "property") {
            tprop = getToken(editor, {line: cur.line, ch: tprop.start});
            console.log('tprop:', tprop);
            // TODO: is this needed?
            // if (tprop.string != "." && tprop.type != "daskey") return;
            if (!context) var context = [];
            context.push(tprop);
        } else if (!tprop.className) {
            parent = getToken(editor, {line: cur.line, ch: tprop.start});
            //we have two choices:
            // 1) whitespace after a value
            if (parent && /^[ ]+$/.test(parent.string)) {
                // make sure we don't catch properties from earlier entity
                // TODO: actually this shall not be there...
                parent.state.value_for = '';
                token.state.value_for = '';

                tprop = parent;
            }
            console.log('parent', parent);
            // 2) empty after an operator
            // no actions needed
        }
        return context;
    }

    function scriptHint(editor, keywords, getToken, callback) {
        // Find the token at the cursor
        var cur = editor.getCursor(), token = getToken(editor, cur);

        // If it's not a 'word-style' token, ignore the token.
        if (!CodeMirror.valueRE.test(token.string)) {
            token = {start: cur.ch, end: cur.ch, string: "", state: token.state,
                className: token.string == "." ? "property" : null};
        }


        context = extractContext(token, editor, cur, getToken);

        // check if to use async
        if (!checkAsyncCompletions(token, cur, context, callback)) {
            // beginning matches
            var found = getStaticCompletions(token, context, keywords);
            //append the infix matches
            found = getStaticCompletions(token, context, keywords, true, found);
            //found.extend_array(found2);

            console.log('found:', found);
            var s = getStartsWith(cur, token);
            console.log('getStartsWith results:', s);

            // templates
            // TODO: su[m] -> max
            if (CodeMirror.templatesHint) {
                console.log('getCompletions:template');
                console.log(token);
                 if (token.state && !token.state.value_for) {
                    CodeMirror.templatesHint.getCompletions(editor, found, s);
                 }
            }

            data = {list: found,
                from: {line: cur.line, ch: token.start},
                to: {line: cur.line, ch: token.end}};

            if (CodeMirror.attachContextInfo) {
                // if context info is available, attach it
                CodeMirror.attachContextInfo(data);
            }

            callback(data);


        }


    }


    CodeMirror.hint_values = {};
    CodeMirror.hint_fields = {};


    CodeMirror.dasHint = function (editor, callback) {
        return scriptHint(editor, [],
            function (e, cur) {
                return e.getTokenAt(cur);
            }, callback);
    };


    /* TODO: hints sort of work, but we have to take care of more general case, when it's not known what the value is - a field, a value or whateva */

    var datasetFields = 'status creation_time modification_time tag nfiles nblocks size modified_by name datatype created_by nevents';
    //var dasEntities = 'status group monitor parent config lumi site dataset release role user file child tier run summary primary_dataset block jobsummary'; // TODO: and more...
    var fileFields = 'adler32 block_name checksum created_by creation_time file_trigger_tag file_trigger_tag.nevents file_trigger_tag.trigger_tag id last_modified_by md5 modification_time name nevents original_node queryable_meta_data replica replica.creation_time replica.custodial replica.group replica.node_id replica.se replica.site replica.subscribed size';

    CodeMirror.hint_fields = {
        dataset: 'status creation_time modification_time tag nfiles nblocks size modified_by name datatype created_by nevents'.split(' '),
        file: 'adler32 block_name checksum created_by creation_time file_trigger_tag file_trigger_tag.nevents file_trigger_tag.trigger_tag id last_modified_by md5 modification_time name nevents original_node queryable_meta_data replica replica.creation_time replica.custodial replica.group replica.node_id replica.se replica.site replica.subscribed size'.split(' ')
    };

    CodeMirror.hint_daskeys = CodeMirror.hint_daskeys || 'dataset file site primary_dataset lumi'.split(' ');


    // TODO: file entity may actually return replica as well!!!


    var dasKeywords = ("between in grep").split(" ");


    /**
     * check whether async completion shall be used, e.g. values of high arity
     * */
    function checkAsyncCompletions(token, cur, context, callback) {

        // async completions
        if (!(context && context.length > 0)) {


            // TODO: values for high arity daskeys thru ajax, so far dataset
            // TODO: dataset wildcard live??!!

            // TODO: block also?

            if (token.state.value_for === "dataset") {
                var req = new Ajax.Request('/das/autocomplete',
                    {
                        method: 'get',
                        requestHeaders: {Accept: 'application/json'},

                        parameters: {
                            //dbs_instance: 'cms_dbs_prod_global',
                            'query': token.string || '/*/*/*'
                        },
                        onSuccess: function (resp) {
                            console.log('ajax succ', resp);

                            var json = resp.responseText.evalJSON(false);
                            console.log('ajax succ', json);

                            if (json) {
                                results = [];
                                forEach(json, function (v) {
                                    if (v.value.indexOf("dataset=") == 0)
                                        results.push(v.value.replace("dataset=", ''));
                                });
                                console.log('results=', results);
                                data = {
                                    list: results,
                                    from: {line: cur.line, ch: token.start},
                                    to: {line: cur.line, ch: token.end}};
                                var r = callback(data);

                                //console.log('callback done=', callback, r);
                            }
                        }
                    });
                return true;
            }

        }
        return false;
    }


    function getStaticCompletions(token, context, keywords, infix, old_found) {
        infix = infix || false;
        var found = old_found || [];

        // TODO: case match may be slightly better
        var start = token.string.toLowerCase();

        CodeMirror.hint_min_infix_len = 1;

        console.log('token:', token, 'context:', context, 'start:', start);


        function checkMatch(str) {
            str = str.toLowerCase();
            if (str.indexOf(start) == 0)
                return true;
            // partial match
            return infix && (start.length >= CodeMirror.hint_min_infix_len)
                && str.indexOf(start) !== -1;

        }

        function maybeAdd(str) {
            var NO_DUPLICATE_CHECK = false;
            var N = 500;

            if (found.length == N) {
                found.push('(showing only first 300 value matches...)');
                return;
            }
            if (found.length > N)
                return false;

            if (checkMatch(str) &&
                (NO_DUPLICATE_CHECK || !arrayContains(found, str)))
                found.push(str);
        }

        function maybeAddField(field_data) {
            var field_name = field_data.name;
            var field_title = field_data.title;
            if (field_title)
                field_title = '<b>'+field_title+'</b><br/>';

            var completion = {
                "moduleFunction": field_name,
                "text": "." + field_name,
                "className": "CodeMirror-hint-attribute"
            };

            completion.info = function (completion) {
                return '' +
                    field_title +
                    '<span class="note">Note: Have in mind, that ' +
                    'availability of fields in the results may depend' +
                    ' on the variation of your keywords and therefore' +
                    ' can not be known in advance.';
            };
            if (("." + field_name).indexOf(start) == 0 && !arrayContains(found, field_name)) found.push(completion);
        }


        if (context && context.length > 0) {
            // If this is a property, see if it belongs to some object we can
            // find in the current environment.
            //var obj = context.pop(), base;
            var obj = token;
            context = context.pop();
            console.log('context', context);


            // TODO: field selection is messed up! it do not include the value being typed...
            // TODO: property selection do not work within OPER(file.si)

            // TODO: it seems now I get: dataset.tag=dataset
            if (!obj.state.value_for) {
                field_data = CodeMirror.hint_fields[context.string];
                if (field_data !== undefined){
                    forEach(field_data, maybeAddField);
                }
                /*
                if (context.string == "dataset") {
                    forEach(datasetFields.split(" "), maybeAddField);
                }
                if (context.string == "file") {
                    forEach(fileFields.split(" "), maybeAddField);
                }
                // TODO: some value completions...
                if (context.string == "string") {
                    // forEach(coffeescriptKeywords.split(" "), maybeAdd);
                    // we could show the format for example...
                }
                */
            }
            else {
                // complete the attribute values, that we don't have...
            }

        }
        else {

            // If not, just look in the window object and any local scope
            // (reading into JS mode internals to get at the local variables)
            //for (var v = token.state.localVars; v; v = v.next) maybeAdd(v.name);

            if (!token.state.value_for) {
                // das keys
                forEach(CodeMirror.hint_daskeys, maybeAdd);
                forEach(keywords, maybeAdd);

            }

            // values for daskeys
            if (token.state.value_for) {
                daskey = token.state.value_for;
                if (CodeMirror.hint_values[daskey]) {
                    forEach(CodeMirror.hint_values[daskey], maybeAdd);
                }
            }

        }


        return found;
    }
})();
