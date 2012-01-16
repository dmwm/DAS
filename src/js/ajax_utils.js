function ajaxCheckPid(base, method, pid, interval) {
    var limit = 30000; // in miliseconds
    var wait  = parseInt(interval);
    if (wait*2 < limit) {
        wait  = wait*2;
    } else { wait = limit; }
    new Ajax.Updater('response', base+'/'+method,
    { method: 'get' ,
      parameters : {'pid': pid},
      onException: function() {return;},
      onSuccess : function(transport) {
        var sec = wait/1000;
        var msg = ', next check in '+sec.toString()+' sec, please wait..., <a href="/das/">stop</a> request';
        // look at transport body and match its content,
        // if check_pid still working on request, call again
        if (transport.responseText.match(/processing PID/)) {
            transport.responseText += msg;
            setTimeout('ajaxCheckPid("'+base+'","'+method+'","'+pid+'","'+wait+'")', wait);
        }
      }
    });
}
