
CodeMirror.commands.autocomplete = function (cm) {
    var opts = {
        completeSingle: false,
        closeOnUnfocus: true,
        async: true,
        closeCharacters: /[\s()><=,]/,  // default was: /[\s()\[\]{};:>,]/;
        customKeys: {
            // custom key handlers
            Home: function (cm, handle) {
                return CodeMirror.Pass
            },
            End: function (cm, handle) {
                return CodeMirror.Pass
            },
            Enter: function (cm, handle) {
                // if none selected, we shall propagate back the Enter
                console.log(handle, handle.is_none_selected());
                if (handle.is_none_selected())
                    return CodeMirror.Pass;
                return handle.pick()
            },

            // below are defaults...
            Up: function (cm, handle) {
                handle.moveFocus(-1);
            },
            Down: function (cm, handle) {
                handle.moveFocus(1);
            },
            PageUp: function (cm, handle) {
                handle.moveFocus(-handle.menuSize());
            },
            PageDown: function (cm, handle) {
                handle.moveFocus(handle.menuSize());
            },
            Tab: function (cm, handle) {
                return handle.pick()
            },
            Esc: function (cm, handle) {
                return handle.close()
            }
        }

    };
    if (cm.state.completionActive){
            console.log('hint already active/2');
            // TODO: shall we pass?
            return;
    } else {
          console.log('command:showHint');

    }
    CodeMirror.showHint(cm, CodeMirror.dasHint, opts);
    //active_hint.closeOnUnfocus
};

function passAndHint(cm) {
    setTimeout(function () {
        cm.execCommand("autocomplete");
    }, 100);
    return CodeMirror.Pass;
}

function submitQuery(cm) {
    setTimeout(function () {
        document.getElementById('das_search').submit();
    }, 100);
    //return CodeMirror.Pass;
}

var cm_editor = false;
var cm_is_completion_active = false;


function CM_disableAutocompletion() {
    if (cm_editor) {
        cm_editor.toTextArea();
        cm_editor = false;
    }
}

function CM_enableAutocompletion() {

    cm_editor = CodeMirror.fromTextArea(document.getElementById('input'), {
        mode: 'das',
        lineNumbers: false,
        matchBrackets: true,
        lineWrapping: true,
        extraKeys: {
            "Ctrl-Space": "autocomplete",
            "Tab": "autocomplete",
            "'.'": passAndHint,
            "'='": passAndHint,
            "Enter": submitQuery
        }
    });

    var timeout = false;

    function scheduleHint() {
        if (timeout) clearTimeout(timeout);
        timeout = setTimeout(function () {
            if (cm_editor.state.completionActive){
                console.log('hint already active/1');
                //return;
            }
            console.log('trying to show hint');
            cm_editor.execCommand("autocomplete");
        }, 100);
    }

    cm_editor.on("focus", function(){
        scheduleHint();
        return CodeMirror.Pass;
    });

    cm_editor.on("keyup", function (cm, event) {
        var popupKeyCodes = {
            "9": "tab",
            "13": "enter",
            "27": "escape",
            "33": "pageup",
            "34": "pagedown",
            "35": "end",
            "36": "home",
            "38": "up",
            "40": "down",
        };
        var keyCode = (event.keyCode || event.which).toString();
        console.log('keyCode=', keyCode);


        if (!popupKeyCodes[keyCode] && !cm_editor.state.completionActive) {
            scheduleHint();
        }
        return CodeMirror.Pass;
    });


    //document.getElementById('ac_hint').innerHTML = "Start typing " +
    //    "to activate autocompletion."

}