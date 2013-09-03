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

    function forEachItem(arr, f) {
        for (var key in arr) {
            if (arr.hasOwnProperty(key)) {
                f(key, arr[key])
            }
        }
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


    function extractContext(token, editor, cur) {
        var tprop = token;

        getToken = function (e, cur) {
            return e.getTokenAt(cur);
        };

        //console.log('hint for token=', token, 'token.value_for=', token.state.value_for);


        //console.log('tprop0:', tprop);

        // If it is a property, find out what it is a property of.
        if (tprop.className == "property") {
            tprop = getToken(editor, {line: cur.line, ch: tprop.start});
            //console.log('tprop:', tprop);
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
            //console.log('parent', parent);
            // 2) empty after an operator
            // no actions needed
        }
        return context;
    }


    CodeMirror.dasHint = function (editor, callback) {
        // Find the token at the cursor
        var cur = editor.getCursor(), token = editor.getTokenAt(cur);

        console.log('dasHint, initial token:', token.string, token);
        // If it's not a 'word-style' token, ignore the token.
        if (!CodeMirror.valueRE.test(token.string)) {
            token = {start: cur.ch, end: cur.ch, string: "", state: token.state,
                className: token.string == "." ? "property" : null};
        }

        context = extractContext(token, editor, cur);

        // check if to use async
        if (!checkAsyncCompletions(token, cur, context, callback)) {
            // beginning matches
            var found = getStaticCompletions(token, context, []);
            //append the infix matches
            found = getStaticCompletions(token, context, [], true, found);
            callback(processHintResults(found, cur, token));
        }


        var dasKeywords = ("between in grep").split(" ");


        function checkMatch(str, token_value, infix) {
            infix = infix || true;

            str = str.toLowerCase();
            if (str.indexOf(token_value) == 0)
                return true;
            // partial match
            return infix && (token_value.length >= CodeMirror.hint_min_infix_len)
                && str.indexOf(token_value) !== -1;

        }


        function getDatasetsAsync(token, cur, callback) {
            var req = new Ajax.Request('/das/autocomplete',
                {
                    method: 'get',
                    requestHeaders: {Accept: 'application/json'},

                    parameters: {
                        //dbs_instance: 'cms_dbs_prod_global',
                        'query': token.string || '/*/*/*'
                    },
                    onSuccess: function (resp) {
                        //console.log('ajax succ', resp);

                        var json = resp.responseText.evalJSON(false);
                        //console.log('ajax succ', json);

                        if (json) {
                            results = [];
                            forEach(json, function (v) {
                                if (v.value.indexOf("dataset=") == 0)
                                    results.push(v.value.replace("dataset=", ''));
                            });

                            var r = callback(processHintResults(results, cur, token));

                            //console.log('callback done=', callback, r);
                        }
                    }
                });
        }

        function addResult(str, results){
            var type = "daskey";
            if (str.indexOf('=') !== -1)
                type = "suggestion";

            var completion = {
                "text": str,
                "displayText": str,
                "className": "CodeMirror-hint-"+type
            };
            var descr = CodeMirror.hint_daskey_descr[str];
            if (descr){
                completion.info = function(compl){
                    return descr;
                }
            }
            results.push(completion);
        }


        function processHintResults(results, cur, token) {
            var s = getStartsWith(cur, token);
            //console.log('getStartsWith results:', s);

            // templates
            // TODO: su[m] -> max
            if (CodeMirror.templatesHint) {
                //console.log('getCompletions:template');
                //console.log(token);
                if (token.state && !token.state.value_for) {
                    CodeMirror.templatesHint.getCompletions(editor, results, s);
                }
            }

            var data = {
                list: results,
                from: {line: cur.line, ch: token.start},
                to: {line: cur.line, ch: token.end}};

            if (CodeMirror.attachContextInfo) {
                // if context info is available, attach it
                CodeMirror.attachContextInfo(data);
            }
            return data;
        }

        function getAnyTokenMatchesAsync(token, cur, callback) {
            var token_value = token.string.toLowerCase();

            var req = new Ajax.Request('/das/autocomplete',
                {
                    method: 'get',
                    requestHeaders: {Accept: 'application/json'},

                    parameters: {
                        //dbs_instance: 'cms_dbs_prod_global',
                        'query': token.string || '/*/*/*'
                    },
                    onSuccess: function (resp) {
                        var results = [];

                        function maybeAdd(str) {
                            var NO_DUPLICATE_CHECK = false;
                            var N = 500;
                            //var results;

                            if (results.length == N) {
                                results.push('(showing only first 300 value matches...)');
                                return;
                            }
                            if (results.length > N) {
                                return false;
                            }


                            if (checkMatch(str, token_value)) {
                            //    (NO_DUPLICATE_CHECK || !arrayContains(results, str))) {
                                addResult(str, results);
                           }

                        }

                        //console.log('ajax succ', resp);

                        var json = resp.responseText.evalJSON(false);
                        //console.log('ajax succ', json);

                        if (json) {

                            // TODO: full matches first...

                            // das keys
                            forEach(CodeMirror.hint_lookup_keys, maybeAdd);

                            // locally stored values. hmm, for simplicity this can be done server side...
                            forEachItem(CodeMirror.hint_values, function (daskey, vals) {
                                var n = 0;
                                forEach(vals, function (v) {
                                    if (checkMatch(v, token_value, true) && (n < 5)) {
                                        addResult(daskey + "=" + v, results);
                                        n++;
                                    }
                                });
                            });


                            // dataset values
                            forEach(json, function (v) {
                                if (v.value.indexOf("dataset=") == 0)
                                    addResult(v.value, results);
                            });

                            // this will need a callback
                            // TODO: check for local value matches

                            // TODO: check for remote value match
                            //forEach(['dataset=*ZMM*', 'primary_dataset=ZMM', ], maybeAdd);


                            // TODO: check for local value match, e.g. primary_dataset...
                            //forEach(['dataset=*ZMM*', 'primary_dataset=ZMM' ], maybeAdd);

                            //console.log('results=', results);

                            var r = callback(processHintResults(results, cur, token));

                            //console.log('callback done=', callback, r);
                        }
                    }
                });
        }

        /**
         * check whether async completion shall be used, e.g. values of high arity
         * */
        function checkAsyncCompletions(token, cur, context, callback) {
            console.log('checkAsync; token=', token, 'context=', context);

            var token_value = token.string.toLowerCase();
            console.log('checkAsync; token_value=', token_value, 'context=', context);


            // async completions

            // if no dot in current token
            if (!(context && context.length > 0)) {


                // TODO: values for high arity daskeys thru ajax, so far dataset
                // TODO: dataset wildcard live??!!

                // TODO: block also?

                if (token.state.value_for === "dataset") {
                    getDatasetsAsync(token, cur, callback);
                    return true;
                }

                console.log('checkAsync; token=', token_value);
                // arbitrary token
                if (!token.state.value_for && token_value.length > 2) {
                    console.log('getAnyTokenMatchesAsync; token=', token_value);
                    getAnyTokenMatchesAsync(token, cur, callback);
                    return true;
                }

            }
            return false;
        }


        function getStaticCompletions(token, context, keywords, infix, old_found) {
            infix = infix || false;
            var found = old_found || [];

            // TODO: case match may be slightly better
            var token_value = token.string.toLowerCase();

            CodeMirror.hint_min_infix_len = 1;

            //console.log('token:', token, 'context:', context, 'start:', start);


            function maybeAddDasKey(str) {
                // TODO: some notes/hints for daskey...

            }

            /**
             * These will propose daskey=value given some value, e.g.
             * zmm --> [ dataset=*zmm*, primary_dataset=ZMM, ...]
             * @param str
             */
            function maybeAddSuggestion(str) {

            }


            function maybeAdd(str) {
                var NO_DUPLICATE_CHECK = false;
                var N = 500;

                if (found.length == N) {
                    found.push('(showing only first 300 value matches...)');
                    return;
                }
                if (found.length > N) {
                    return false;
                }

                var completion = {
                    "text": str,
                    "displayText": str,
                    "className": "CodeMirror-hint-daskey"
                };
                var descr = CodeMirror.hint_daskey_descr[str];
                if (descr){
                    completion.info = function(comp){
                        return descr;
                    }
                }

                if (checkMatch(str, token_value, infix) &&
                    (NO_DUPLICATE_CHECK || !arrayContains(found, str))) {
                    found.push(completion);
                }

            }

            function maybeAddField(field_data) {
                var field_name = field_data.name;
                var field_title = field_data.title;
                //if (field_title)
                field_title = '<b>' + (field_title || field_name) + '</b><br/>';

                var completion = {
                    "moduleFunction": field_name,
                    "text": "." + field_name,
                    "displayText": field_name,
                    "className": "CodeMirror-hint-attribute"
                };

                completion.info = function (completion) {
                    return '' +
                        field_title +
                        '<em>Example values:</em> 1, 2, mc<br />' +
                        '<br /><span class="hint-notice"><i>Note:</i> ' +
                        'exact list of fields available in results may depend ' +
                        ' on your input and therefore can not be known in advance.';
                };
                if (("." + field_name).indexOf(token_value) == 0 && !arrayContains(found, field_name)) found.push(completion);
            }


            if (context && context.length > 0) {

                var obj = token;
                context = context.pop();

                // 1) fieldnames for given entity
                if (!obj.state.value_for) {
                    field_data = CodeMirror.hint_fields[context.string];
                    if (field_data !== undefined) {
                        forEach(field_data, maybeAddField);
                    }

                }
                else {
                    // 1) field values

                    // complete the attribute values, that we don't have them now...
                }

            }
            else {

                // arbitrary token
                if (!token.state.value_for) {
                    // das keys
                    forEach(CodeMirror.hint_lookup_keys, maybeAdd);
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

    };

})();
