#!/bin/bash
# a script used to initialize the default environment variables, e.g. for running unit tests

export DAS_ROOT=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
export DAS_CONFIG=$DAS_ROOT/etc/das.cfg
export DAS_JSPATH=$DAS_ROOT/web/js
export DAS_CSSPATH=$DAS_ROOT/web/css
export DAS_IMAGESPATH=$DAS_ROOT/web/images
# TODO: YUI is not yet installed automatically
export YUI_ROOT=$DAS_ROOT/web/js/yui
