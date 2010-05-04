function ajaxStatus() {
    new Ajax.Updater('_response', '/das/status', 
    { method: 'get' ,
      parameters : UrlParams(),
      evalScripts:true
    });
}
