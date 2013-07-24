CodeMirror.defineMode('das', function (config) {

    CodeMirror.regexpOperator =  /[><=]/;
    CodeMirror.attributeRE = /(\w|\.)/;

    // todo: not sure value need ., but to be more careful
    CodeMirror.valueRE = /^[a-zA-Z0-9_\-*/.@:#]+/;
    CodeMirror.valueRE_1char = /[a-zA-Z0-9_\-*/.@:#]/;


    CodeMirror.attributeRE_strict = /^\w+\./;

    CodeMirror.wordRE = /\w/;
    CodeMirror.attributeSeparRE = /\./;


    var words = {};

    function define(style, string) {
        var split = string.split(' ');
        for (var i = 0; i < split.length; i++) {
            words[split[i]] = style;
        }
    }

    // Atoms
    //define('atom', 'true false');

    // Keywords
    // if then do else elif while until for in esac fi fin fil done exit set unset export function
    var das_keywords = 'last in between ';
    var das_builin = 'grep unique sum count min max avg |';
    das_builin += das_builin.toUpperCase();
    das_keywords += das_keywords.toUpperCase();


    var daskeys = 'status group monitor parent config lumi site dataset release role user file child tier run summary primary_dataset block jobsummary';

    define('keyword', das_keywords);

    // Commands
    define('builtin', das_builin);

    //daskeys
    define('daskey', daskeys);




    function testOperator(stream){
        var ch = stream.peek();
        return CodeMirror.regexpOperator.test(ch);
    }

    function testAttribute(stream){
        var ch = stream.peek();
        return CodeMirror.attributeSeparRE.test(cur);
    }


    /**
     * Tokenizes an operator, e.g. =, >=, <=, <
     *
     * @param stream
     * @param state
     */
    function tokenOperator(stream, state) {
        if (!testOperator(stream))
            return false;

        stream.eatWhile(CodeMirror.regexpOperator);
        var cur = stream.current();

        //state.within_value_for_operator = true;
        console.log('op:', cur, 'value_for:', state.value_for);

        // next is potentially a value
        state._eatAttr_next_step = 'value';
        return 'operator';
    }



    function is_ongoing_tokenEntityName(state){
        return state._eatAttr_next_step;
    }

    function cleanState(stream, state){
        state.attribute_name = '';
        state.value_for = '';
        state._eatAttr_next_step = false;
        state.completion_forbidden = false;
        state._clean_next = false;

    }

    function scheduleCleanState(state){
        state._clean_next = true;
        state._eatAttr_next_step = false;

    }

    function tokenEntityName(stream, state) {

        if (state.tokens.length > 0)
            state.tokens.shift();

        var step = is_ongoing_tokenEntityName(state);


        // TODO: shall I set value_for_attribute here?

        // TODO: just eat operators here, if any and it will be simpler...

        var label = false;

        switch (step) {
            case 'attr':
                // eat attr recursively to have separate tokens
                //stream.eat('.');
                //stream.eatWhile(/\w/);

                stream.eatWhile(CodeMirror.attributeRE);
                var cur = stream.current();
                state.attribute_name = cur;
                console.log('attr:', cur, 'next:', stream.peek());

                // TODO: append if needed...

                if (testOperator(stream)){
                    state._eatAttr_next_step = 'op';
                } else {
                    scheduleCleanState(state);
                }

                label = 'property';
                break;

            case 'op':
                console.log('processing operator, token=', state, state.value_for);

                if (testOperator(stream)){
                    label =  tokenOperator(stream, state);
                    state._eatAttr_next_step = 'value';

                    if (state.attribute_name) {
                        state.value_for = state.attribute_name;
                    }

                } else {
                    // process default
                    label = tokenBase(stream, state);
                    scheduleCleanState(stream, state);

                }
                console.log('processing operator, token=', state, state.value_for);

                break;

            case 'value':
                // next state shall be value
                stream.eat(' ');
                //stream.eatWhile(/[^ "']/);
                stream.eatWhile(CodeMirror.valueRE_1char);

                label =  'string';
                state._eatAttr_next_step = false;
                scheduleCleanState(state);
                break;

            default:
               // cleanState(stream, state);
                state._eatAttr_next_step = false;
                break;
        }
        return label;

    }

    function tokenBase(stream, state) {

        var ch = stream.peek();
        //console.log('tokenBase, ch:', ch);


        // TODO: this may be more fancy|| ch === '`''=
        //if (ch === '\'' || ch === '"') {
        //    state.tokens.unshift(tokenString(ch));
        //    return tokenize(stream, state);
        //}


        //console.log(state)

        if (state._clean_next)
            cleanState(stream, state);


        if (ch === ' '){
            stream.eat(' ');
            return 'aaa';
        }

        if (is_ongoing_tokenEntityName(state))
            return tokenEntityName(stream, state);




        // TODO: attribute may end without operator!!!

        if (ch === ',' || ch === '(' || ch === ')' ) {
            stream.next();
            return 'def';
        }

        if (ch === '[' || ch === ']') {
            stream.next();
            // this allows numbers only, so no completion needed
            state.completion_forbidden = true;
            return 'operator';
        }


        if (/\d/.test(ch)) {
            stream.eatWhile(/\d/);
            if (!/\w/.test(stream.peek())) {
                return 'number';
            }
        }

        // check for any regular strings (attribute, daskey, etc)
        //stream.eatWhile(CodeMirror.attributeRE);

        // do we have attribute that follows
        if (stream.match(CodeMirror.attributeRE_strict, false, true)) {
            console.log('attr re strict passed');
            stream.eatWhile(CodeMirror.wordRE);
            state._eatAttr_next_step = 'attr';

            // append a new task to process the attribute itself
            state.tokens.unshift(tokenEntityName);
            return 'daskey';

        }


        stream.eatWhile(CodeMirror.wordRE);
        var cur = stream.current();


        if (testOperator(stream) && /\w/.test(cur)){
            console.log('test op passed, cur: ', cur);
            state.value_for = cur;
            // tokenOperator(stream, state) will be called in next iteration
            state._eatAttr_next_step = 'op';
            return 'daskey';
        }




        // make sure to process any other unseen characters...
        // (as we do stream.peek not stream.next)
        if (!cur) {
            cur = stream.next();
            // TODO: check
            //cleanState(stream, state);
        }
        //if (testOperator(stream)){
        //    return tokenOperator(stream, state);
        //}

        return words.hasOwnProperty(cur) ? words[cur] : null;
    }




    function tokenize(stream, state) {
        return (state.tokens[0] || tokenBase)(stream, state);
    }

    return {
        startState: function () {
            return {tokens: []};
        },
        token: function (stream, state) {
            if (stream.eatSpace()) return null;
            return tokenize(stream, state);
        }
    };
});

CodeMirror.defineMIME('text/x-sh-das', 'das');
