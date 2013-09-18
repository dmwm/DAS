// keyword search
//
// result type selector
//

var RTYPE_ANY = "any";

function hide(selector){
    //jQuery(selector).hide().;
}
function filterByResultType(rt, target){
    if (rt == RTYPE_ANY) {
        jQuery('.kws-result').show(300);
    } else {
        jQuery('.kws-result').hide();
        jQuery('.result-with-entity-'+rt).show(300);
    }

    jQuery('.rt-filters a').removeClass('rtype-selected');
    //console.log(target, jQuery(target));
    jQuery(target).addClass('rtype-selected')
}

// by default select the "any"
jQuery('.rt-'+RTYPE_ANY).addClass('rtype-selected');

function showAllRT(){
}


// limit the number of results shown

function showAllResults(){
    jQuery('.kws-result').show(300);
    jQuery('#results-only-top-k').html('');
}


function hideLowResults(){
    // hide all results lower than 10
    var limit = 10;
    var nimportant = 4;

    count = jQuery('.kws-result').length;

    if (count > limit) {
        jQuery('.kws-result:gt('+ (limit-1) +')').hide();
        var cont = jQuery('<div id="results-only-top-k">Showing only top ' +
                           limit + ' suggestions.&nbsp;</div>');
        var showBtn = jQuery('<a href="javascript:;">see more</a>');
        showBtn.click(showAllResults);
        cont.append(showBtn);

        jQuery('div#kws-results-listing').append(cont);
    }

    //if (count > nimportant) {
    //    jQuery('.kws-result:gt('+ (nimportant-1) +')').css('opacity', 0.7);
    //}
}

var initialize_kws_results = function() {
    // do not hide debug tooltip (TODO: it should be on click actually)
    jQuery(".debug").each(function(){
        jQuery(this).tooltip({
            content: jQuery(this).attr('title'),
            track: true,
            //close: true,
            hide: { effect: "fade", duration: 2000 }
            //function( event, ui ) { jQuery(this).tooltip('open'); },
        });
    });
    jQuery(".kws-link").each(function(){
        jQuery(this).tooltip({
            content: jQuery(this).attr('title')
        });
    });

    hideLowResults();
};