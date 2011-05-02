function ajaxCheckPid(base, pid, next) {
    new Ajax.Updater('response', base+'/check_pid', 
    { method: 'get' ,
      parameters : {'pid': pid, 'next': next},
      evalScripts:true
    });
}
