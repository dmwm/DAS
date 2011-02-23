function ajaxStatus(base) {
    new Ajax.Updater('_response', base+'/status', 
    { method: 'get' ,
      parameters : UrlParams(),
      evalScripts:true
    });
}
function ajaxCheckPid(base, pid, next) {
    new Ajax.Updater('_response', base+'/check_pid', 
    { method: 'get' ,
      parameters : {'pid': pid, 'next': next},
      evalScripts:true
    });
}
function ajaxQueryInfo(base) {
    var q = document.getElementById('dasquery');
    new Ajax.Updater('_queryinfo', base+'/expert/query_info', 
    { method: 'get' ,
      parameters : {'dasquery':q.value},
    });
}
function ajaxCleanInfo(base) {
    var q = document.getElementById('dbcoll');
    new Ajax.Updater('_cleaninfo', base+'/expert/clean', 
    { method: 'get' ,
      parameters : {'dbcoll':q.value},
    });
}
