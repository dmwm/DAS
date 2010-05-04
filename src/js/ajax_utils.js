function ajaxRequest() {
    new Ajax.Updater('_response', '/das/request', 
    { method: 'get' ,
      parameters : UrlParams(),
      evalScripts:true
    });
}
