/**
 * Created with PyCharm.
 * User: vidma
 * Date: 2/21/13
 * Time: 11:06 AM
 * To change this template use File | Settings | File Templates.
 */

(function($){
    var _DEBUG = true;


    /* Value matches */
    VS.ui.SearchFacet = VS.ui.SearchFacet.extend({
        autocompleteValues : function(req, resp) {
            var category = this.model.get('category');
            var value    = this.model.get('value');
            var searchTerm = req.term;

            this.options.app.options.callbacks.valueMatches(category, searchTerm, function(matches, options) {
                options = options || {};
                matches = matches || [];

                if (searchTerm && value != searchTerm) {
                    if (options.preserveMatches) {
                        resp(matches);
                    } else {
                        // TODO: beginning with higher score?
                        var re = VS.utils.inflector.escapeRegExp(searchTerm || '');
                        var matcher = new RegExp('(\\b|[_-])' + re, 'i');
                        matches = $.grep(matches, function(item) {
                            return matcher.test(item) ||
                                matcher.test(item.value) ||
                                matcher.test(item.label);
                        });
                    }
                }

                if (options.preserveOrder) {
                    resp(matches);
                } else {
                    resp(_.sortBy(matches, function(match) {
                        if (match == value || match.value == value) return '';
                        else return match;
                    }));
                }
            });

        },
        setupAutocomplete : function() {
            if (_DEBUG) console.log('model shall be', this.model);

            this.box.autocomplete({
                source    : _.bind(this.autocompleteValues, this),
                minLength : 0,
                delay     : 200,

                // MODIFICATION:
                // we do no want to auto-focus items that allow  wildcard
                // server-side autocompletion, otherwise the entered value would
                // be overriden by first selection on pressing enter

                autoFocus : (this.model.get('category') !== 'dataset'),

                position  : {offset : "0 5"},
                create    : _.bind(function(e, ui) {
                    $(this.el).find('.ui-autocomplete-input').css('z-index','auto');
                }, this),
                select    : _.bind(function(e, ui) {
                    e.preventDefault();
                    var originalValue = this.model.get('value');
                    this.set(ui.item.value);
                    if (originalValue != ui.item.value || this.box.val() != ui.item.value) {
                        if (this.options.app.options.autosearch) {
                            this.search(e);
                        } else {
                            this.options.app.searchBox.renderFacets();
                            this.options.app.searchBox.focusNextFacet(this, 1, {viewPosition: this.options.order});
                        }
                    }
                    return false;
                }, this),
                open      : _.bind(function(e, ui) {
                    var box = this.box;
                    this.box.autocomplete('widget').find('.ui-menu-item').each(function() {
                        var $value = $(this),
                            autoCompleteData = $value.data('item.autocomplete') || $value.data('ui-autocomplete-item');

                        if (autoCompleteData['value'] == box.val() && box.data('uiAutocomplete').menu.activate) {
                            box.data('uiAutocomplete').menu.activate(new $.Event("mouseover"), $value);
                        }
                    });
                }, this)
            });

            this.box.autocomplete('widget').addClass('VS-interface');
        }
    });


    /* Facet matches */
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
                    return item && prefix_matcher.test(item.label || item) && item;
                });

                // non prefix (infix) matches, e.g. datas->primary_dataset
                if (options.allowInfix){
                    var matcher    = new RegExp(re, 'i');
                    var matches_anywhere = jQuery.grep(prefixes, function(item) {
                        var r= item && matcher.test(item.label || item) && item;
                        //console.log(item.label || item, matches, jQuery.inArray(item.label || item, matches));
                        // only unique items
                        return r && jQuery.inArray(item, matches) == -1 && r;
                    });
                    matches = jQuery.merge(matches, matches_anywhere);
                    matches = jQuery.merge(matches, new Array(
                        {label: 'dataset=/Zmm/*/*', type: 'value_match', 'facet': 'dataset', category: 'value matches'},
                        {label: 'dataset=/*/Zmm/*', type: 'value_match', 'facet': 'dataset', category: 'value matches'},
                        {label: 'primary_dataset=Zmm*', type: 'value_match', 'facet': 'dataset', category: 'value matches'}
                    ));
                }

                if (_DEBUG) console.log('before end', matches);

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
                // TODO: larger delay (only) for server-side
                delay     : 50,

                // do not select the first option (it's annoying as prevents
                // user from typing his thing).
                // TODO: still could be useful in case of a good match (?)
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

                        // we get rid of initial entry (by not calling addTextFacetRemainder)
                        if (_DEBUG)  console.log('adding value_match_facet:', facet, value, this.options.position + 0);

                        //   addFacet : function(category, initialQuery, position)
                        this.app.searchBox.addFacet(facet, value, position);
                        view = _.last(this.app.searchBox.inputViews);
                        this.app.searchBox.focusNextFacet(view, 1, {startAtEnd: -1});

                    } else {
                        var remainder = this.addTextFacetRemainder(ui.item.value);
                        var position  = this.options.position + (remainder ? 1 : 0);
                        this.app.searchBox.addFacet(ui.item instanceof String ? ui.item : ui.item.value, '', position);
                    }
                    return false;
                }, this)
            });
            this.box.data('uiAutocomplete')._renderMenu = function(ul, items) {
                var category = '';
                _.each(items, _.bind(function(item, i) {
                    if (item.category && item.category != category) {
                        ul.append('<li class="ui-autocomplete-category">'+item.category+'</li>');
                        category = item.category;
                    }

                    if(this._renderItemData) {
                        this._renderItemData(ul, item);
                    } else {
                        this._renderItem(ul, item);
                    }

                }, this));
            };

            this.box.autocomplete('widget').addClass('VS-interface');
        },

        addTextFacetRemainder : function(facetValue) {
            var boxValue = this.box.val();
            var lastWord = boxValue.match(/\b(\w+)$/);

            //console.log('addTextFacetRemainder', boxValue);

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