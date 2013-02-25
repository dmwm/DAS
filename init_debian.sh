#!/bin/bash
export DAS_ROOT=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
export DAS_CONFIG=$DAS_ROOT/etc/das.cfg
export YUI_ROOT=$DAS_ROOT/third-party/yui
export DAS_JSPATH=/home/vidma/Desktop/DAS/DAS_code/DAS/src/js
export DAS_CSSPATH=/home/vidma/Desktop/DAS/DAS_code/DAS/src/css
export DAS_IMAGESPATH=/home/vidma/Desktop/DAS/DAS_code/DAS/src/images
export YUI_ROOT=/storage/DAS/DAS_code/DAS/external/yui
export DAS_KWS_IR_INDEX=/storage/DAS/kws_whoosh_idx
