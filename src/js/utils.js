/*
 * DAS specific utilities
 * Author: Valentin Kuznetsov, 2009
 */ 
function updateInput(myinput) {
   if  (myinput) updateTag('input', myinput);
   else updateTag('input', '');

   var ulimit = gup('limit');
   if  (ulimit) updateTag('limit', ulimit);
   else updateTag('limit', '10');

   var uview = gup('view');
   if  (uview) updateTag('view', uview);
   else updateTag('view', 'list');

   var uinstance = gup('instance');
   if  (uinstance) updateTag('instance', uinstance);
   else updateTag('instance', 'cms_dbs_prod_global');
}
function getTagValue(tag)
{
    return document.getElementById(tag).value;
}
function updateTag(tag, val) {
   var id = document.getElementById(tag);
   if (id) {
       id.value=val;
   }
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
function FlipTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        if  (id.className == "show") {
            id.className="hide"; 
        } else {
            id.className="show"; 
        }
    }
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
    // see http://www.kamath.com/codelibrary/cl006_url.asp
    var lsRegExp = /\+/g;
    // Courtesy of http://www.netlobo.com/url_query_string_javascript.html
    name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
    var regexS = "[\\?&]"+name+"=([^&#]*)";
    var regex = new RegExp( regexS );
    var results = regex.exec( window.location.href );
    if( results == null )
        return "";
    else
        // use unescape to properly show URL encoded input in search field form
        return unescape(String(results[1]).replace(lsRegExp, " "));
}
function place_img(tag, img, txt) {
    var id=document.getElementById(tag);
    id.innerHTML='<div><img src="'+img+'" alt="loading" />'+txt+'</div>';
}
function AddFilters() {
    var uin=document.getElementById('input');
    var flt=document.getElementById('filters');
    var val=document.getElementById('das_keys');
    var newval = '';
    if (uin.value.indexOf('|') != -1) {
        if (uin.value.indexOf(' grep') != -1) {
            if (flt.value == 'grep') {
                newval = uin.value + ', ' + val.value;
            } else {
                alert('Cannot mix grep and aggregator function');
                newval = uin.value;
            }
        } else if (uin.value.indexOf('| unique') != -1 || uin.value.indexOf('|unique') != -1) {
            newval = uin.value + ' | grep ' + val.value;
        } else {
            if (flt.value == 'grep') {
                alert('Cannot mix grep and aggregator functions');
                newval = uin.value;
            } else {
                newval = uin.value + ', ' + flt.value + '(' +val.value +')';
            }
        }
    } else {
        if (flt.value == 'grep')
            newval = uin.value + ' | ' + flt.value+' ' + val.value;
        else
            newval = uin.value + ' | ' + flt.value + '(' + val.value+')';
    }
    updateTag('input', newval);
    load('/das/request?'+$('das_search').serialize());
}
function ClearFilters() {
    var uin=document.getElementById('input');
    if (uin.value.indexOf('|') != -1) {
        updateTag('input', uin.value.substring(0, uin.value.indexOf('|')));
    }
    load('/das/request?'+$('das_search').serialize());
}
