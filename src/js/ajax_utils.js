function ajaxCheckPid(base, pid, interval) {
    new Ajax.Updater('response', base+'/check_pid', 
    { method: 'get' ,
      parameters : {'pid': pid, 'interval': interval},
      evalScripts:true
    });
}
