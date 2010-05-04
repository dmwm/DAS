function ajaxStatus() {
    new Ajax.Updater('_response', '/das/status', 
    { method: 'get' ,
      parameters : UrlParams(),
      evalScripts:true
    });
}
function ajaxQueryInfo(query) {
    new Ajax.Updater('_query_info', '/das/admin/query_info', 
    { method: 'get' ,
      parameters : {'query':query},
    });
}
