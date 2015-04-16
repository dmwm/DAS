/*
 * DAS specific utilities
 * Author: Valentin Kuznetsov, 2009
 */
function updateInput(myinput, dbs_global_inst) {
   if  (myinput) updateTag('input', myinput);
   else updateTag('input', '');

   var ulimit = gup('limit');
   if  (ulimit) updateTag('limit', ulimit);
   else updateTag('limit', '50');

   var uview = gup('view');
   if  (uview) updateTag('view', uview);
   else updateTag('view', 'list');

   var uinstance = gup('instance');
   if  (uinstance) updateTag('instance', uinstance);
   else updateTag('instance', dbs_global_inst);
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
function ToggleTag(tag, link_tag) {
    var id=document.getElementById(tag);
    if (id) {
        if  (id.className == "show") {
            id.className="hide";
        } else {
            id.className="show";
        }
    }
    var lid=document.getElementById(link_tag);
    if (lid) {
        if  (lid.innerHTML == "show") {
            lid.innerHTML="hide";
        } else {
            lid.innerHTML="show";
        }
    }
}
function ToggleTagNames(tag, link_tag) {
    var attrs=document.getElementsByName(tag);
    for(var i=0; i<attrs.length; ++i) {
        var val=attrs[i];
        if  (val.className == "show") {
            val.className="hide";
        } else {
            val.className="show";
        }
    }
    var lid=document.getElementById(link_tag);
    if (lid) {
        if  (lid.innerHTML == "show") {
            lid.innerHTML="hide";
        } else {
            lid.innerHTML="show";
        }
    }
}
function load(url) {
    window.location.href=url;
}
function reload() {
    load(window.location.href);
}
function UrlParams() {
    var url=window.location.href;
    var arr=url.split('&');
    var first = arr[0].split('?');
    arr[0]=first[1];
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
    if (uin.value.lastIndexOf('|') != -1) {
        if (uin.value.lastIndexOf('grep ') != -1) {
            if (flt.value == 'grep') {
                newval = uin.value + ', ' + val.value;
            } else if (flt.value == 'sort') {
                newval = uin.value + ' | sort ' + val.value;
            } else {
                alert('Cannot mix grep and aggregator function');
                newval = uin.value;
            }
        } else if (uin.value.lastIndexOf('sort ') != -1) {
            if (flt.value == 'grep') {
                newval = uin.value + ' | grep ' + val.value;
            } else if (flt.value == 'sort') {
                alert('Repeated sort filter');
                newval = uin.value;
            } else {
                newval = uin.value + ' | ' + flt.value + '(' +val.value +')';
            }
        } else if (uin.value.lastIndexOf('| unique') != -1 || uin.value.lastIndexOf('|unique') != -1) {
            if (flt.value == 'grep' || flt.value == 'sort') {
                newval = uin.value + ' | ' + flt.value + ' ' + val.value;
            } else {
                newval = uin.value + ' | ' + flt.value + '(' +val.value +')';
            }
        } else {
            if (flt.value == 'grep' || flt.value == 'sort') {
                alert('Cannot mix grep/sort filters with aggregator functions');
                newval = uin.value;
            } else {
                newval = uin.value + ' | ' + flt.value + '(' +val.value +')';
            }
        }
    } else {
        if (flt.value == 'grep' || flt.value == 'sort') {
            newval = uin.value + ' | ' + flt.value+' ' + val.value;
        } else {
            newval = uin.value + ' | ' + flt.value + '(' + val.value+')';
        }
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
