function ajaxStatus() {
    new Ajax.Updater('_response', '/das/status', 
    { method: 'get' ,
      parameters : UrlParams(),
      evalScripts:true
    });
}
function ajaxQueryInfo() {
    var q = document.getElementById('dasquery');
    new Ajax.Updater('_queryinfo', '/das/admin/query_info', 
    { method: 'get' ,
      parameters : {'dasquery':q.value},
    });
}
