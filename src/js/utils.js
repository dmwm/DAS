
/*
 * General set of utilities used in DAS web interface
 * Author: Valentin Kuznetsov, 2009
 */
var Updater=Class.create();
Updater.prototype = {
    initialize: function(tab) {
       this.tab=tab
    },
    ajaxUpdate: function(ajaxResponse) {
       var responseHTML=RicoUtil.getContentAsString(ajaxResponse);
       var t=document.getElementById(this.tab);
       t.innerHTML=responseHTML;
       // parse response and search for any JavaScript code there, if found execute it.
       var jsCode = SearchForJSCode(responseHTML);
       if (jsCode) {
           eval(jsCode);
       }
    }
}
function SearchForCode(text,begPattern,endPattern) {
    var foundCode='';
    while( text && text.search(begPattern) != -1 ) {
        var p=text.split(begPattern);
        for(i=1;i<p.length;i++) {
            var n=p[i].split(endPattern);
            foundCode=foundCode+n[0]+';\n';
        }
        return foundCode;
    }
    return foundCode;
}
function SearchForJSCode(text) {
    var pattern1='<script type="application\/javascript">';
    var pattern2='<script type=\'application\/javascript\'>';
    var end='<\/script>';
    var foundCode=SearchForCode(text,pattern1,end);
    foundCode=foundCode+SearchForCode(text,pattern2,end);
    return foundCode;
}
function getTagValue(tag)
{
    return document.getElementById(tag).value;
}
function updateTag(tag, val) {
   var id = document.getElementById(tag);
   id.value=val;
}
function ClearTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.innerHTML="";
    }
}
function HideTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.className="hide";
    }
}
function ShowTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.className="show";
    }
}
function wait() {
    var id=document.getElementById('_response');
    id.innerHTML='<div><img src="/dascontrollers/images/loading.gif" alt="loading" /> please wait</div>';
}
function load(url) {
    window.location.href=url;
}
function reload() {
    var url = window.location.href;
    var newurl;
    if (url.search("ajax=1")>0) {
        newurl = url.replace(/ajax=1/g,'ajax=0');
    } else if (url.search("ajax=0")>0) {
        newurl = url;
    } else {
        newurl = window.location.href + '&ajax=0';
    }
    load(newurl);
}
function UrlParams() {
    var url=window.location.href;
    var arr=url.split('&');
    var first = arr[0].split('?');
    arr[0]=first[1];
//    return arr;

    var options = {};
    for (var i=0; i<arr.length; i++) {
        var params = arr[i].split('=');
        options[params[0]] = params[1];
    }
    return options;
}
function gup( name ) {
    // Courtesy of http://www.netlobo.com/url_query_string_javascript.html
    name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
    var regexS = "[\\?&]"+name+"=([^&#]*)";
    var regex = new RegExp( regexS );
    var results = regex.exec( window.location.href );
    if( results == null )
        return "";
    else
        // use unescape to properly show URL encoded input in search field form
        return unescape(results[1]);
}
