/**
 * Created with PyCharm.
 * User: vidma
 * Date: 2/21/13
 * Time: 11:06 AM
 * To change this template use File | Settings | File Templates.
 */

(function($){
    var _DEBUG = true;

    VS.ui.SearchInput = VS.ui.SearchInput.extend({
        autocompleteValues: function(req, resp) {

            var searchTerm = req.term;
            if (_DEBUG) console.log(searchTerm, req);

            var lastWord   = searchTerm.match(/\w+\*?$/); // Autocomplete only last word.
            var re         = VS.utils.inflector.escapeRegExp(lastWord && lastWord[0] || '');
            this.app.options.callbacks.facetMatches(function(prefixes, options) {
                options = options || {};
                prefixes = prefixes || [];

                var prefix_matcher    = new RegExp('^' + re, 'i');

                // put prefix matches higher
                var matches = jQuery.grep(prefixes, function(item) {
                    return item && prefix_matcher.test(item.label || item);
                });

                // non prefix (infix) matches, e.g. datas->primary_dataset
                if (options.allowInfix){
                    var matcher    = new RegExp(re, 'i');
                    var matches_anywhere = jQuery.grep(prefixes, function(item) {
                        var r= item && matcher.test(item.label || item) && item;
                        //console.log(item.label || item, matches, jQuery.inArray(item.label || item, matches));
                        // only unique items
                        return r && jQuery.inArray(item.label || item, matches) == -1 && r;
                    });
                    matches = jQuery.merge(matches, matches_anywhere);
                    matches = jQuery.merge(matches, new Array({label: 'dataset=/Zmg69/*/*', type: 'value_match', 'facet': 'dataset'}));
                }

                if (options.preserveOrder) {
                    resp(matches);
                } else {
                    resp(_.sortBy(matches, function(match) {
                        if (match.label) return match.category + '-' + match.label;
                        else             return match;
                    }));
                }
            });
        },

        setupAutocomplete : function() {
            this.box.autocomplete({
                minLength : this.options.showFacets ? 0 : 1,
                delay     : 500,
                autoFocus : true,
                position  : {offset : "0 -1"},
                source    : _.bind(this.autocompleteValues, this),
                create    : _.bind(function(e, ui) {
                    $(this.el).find('.ui-autocomplete-input').css('z-index','auto');
                }, this),
                select    : _.bind(function(e, ui) {
                    e.preventDefault();
                    if (_DEBUG)  console.log('select:', e, ui)


                    if (!(ui.item instanceof String) && (ui.item.value.indexOf('=') !== -1)){
                        // special value match case
                        if (_DEBUG)  console.log('before adding value_match_facet:', ui.item);
                        var value = ui.item.value.split('=')[1];
                        var facet = ui.item.facet;
                        // TODO: get rid of initial entry
                        if (_DEBUG)  console.log('adding value_match_facet:', facet, value, this.options.position + 0);
                        //   addFacet : function(category, initialQuery, position)
                        this.app.searchBox.addFacet(facet, value, position);
                        //this.app.searchBox.setCursorAtEnd(1);
                        view = _.last(this.app.searchBox.inputViews);
                        if (_DEBUG)  console.log('probably last faced:', view, this.app.searchBox);
                        this.app.searchBox.focusNextFacet(view, 1, {startAtEnd: -1});
                        /*view.deselectFacet();
                        view.setCursorAtEnd(0);
                        view.enableEdit();*/
                    } else {
                        var remainder = this.addTextFacetRemainder(ui.item.value);
                        var position  = this.options.position + (remainder ? 1 : 0);
                        this.app.searchBox.addFacet(ui.item instanceof String ? ui.item : ui.item.value, '', position);
                    }
                    return false;
                }, this)
            });
        },

        addTextFacetRemainder : function(facetValue) {
            var boxValue = this.box.val();
            var lastWord = boxValue.match(/\b(\w+)$/);

            if (!lastWord) {
                return '';
            }

            var matcher = new RegExp(lastWord[0], "i");
            if (facetValue.search(matcher) == 0) {
                boxValue = boxValue.replace(/\b(\w+)$/, '');
            }
            boxValue = boxValue.replace('^\s+|\s+$', '');

            if (boxValue) {
                this.app.searchBox.addFacet(this.app.options.remainder, boxValue, this.options.position);
            }

            return boxValue;
        }
    });
})(jQuery);