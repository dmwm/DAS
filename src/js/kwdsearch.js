// keyword search
//
// result type selector
//

var RTYPE_ANY = "any";

// Opentip Styles
Opentip.styles.drop = {
className: "drop",
borderWidth: 3,
stemLength: 12,
stemBase: 16,
borderRadius: 5,
borderColor: "#c3e0e6",
background: [ [ 0, "#f1f7f0" ], [ 1, "#d3f0f6" ] ],
target: true // improves stability in old IE
};

var initialize_kws_results = function() {
    // initialize opentip
    Opentip.findElements();

    // by default select the "any"
    $$('.rt-'+RTYPE_ANY).invoke('addClassName', 'rtype-selected');
};


function filterByResultType(rt, target){
    if (rt == RTYPE_ANY) {
        $$('.kws-result').invoke('show');
    } else {
        $$('.kws-result').invoke('hide');
        $$('.result-with-entity-'+rt).invoke('show');
    }

    $$('.rt-filters a').invoke('removeClassName', 'rtype-selected');
    //console.log(target, jQuery(target));
    target.addClassName('rtype-selected')
}
