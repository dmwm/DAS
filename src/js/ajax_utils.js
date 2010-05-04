function ajaxRequest_orig_version(query)
{
    wait();
    ajaxEngine.sendRequest('ajaxRequest', 'query='+query);
}
function ajaxRequest()
{
    wait();
    // Get passed parameters, create a string out of them and pass
    // to internal Rico method.
    var url=window.location.href;
    var arr = url.split('?');
    var first = arr.shift();
    var options = {parameters: arr.join('&')};
    ajaxEngine.sendRequestWithData('ajaxRequest', null, options);
}
/***** AJAX registrations *****/
// Requests
ajaxEngine.registerRequest('ajaxRequest','request');
// Objects
var ajaxUpdater = new Updater('_response');
ajaxEngine.registerAjaxObject('_response',ajaxUpdater);
