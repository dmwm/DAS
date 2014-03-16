DAS and KWS maps
----------------

DAS relies on its maps to understand underlying data-services. The DAS tree
contains set of YML files located at services/{cms_maps,maps} areas. These YML
files contain information about underlying data-service APIs.

In order to generate DAS and KWS maps we provide series of tools which we'll
describe here.

Create DAS and KWS maps
-----------------------

DAS and KWS maps can be generated via `das_create_json_maps` and
`das_create_kws_maps`, respectively. KWS maps are generated via series of DAS
queries (please adjust script itself if you need to change KWS coverage).

All maps will be stored in associated JS (JavaScript) files defined in
aforementioned scripts. These files can be uploaded to github.com/dmwm/DASMaps
repository and/or used to upload them into MongoDB.

Validate DAS and KWS maps
-------------------------

It is important to validate DAS and KWS maps since they have pre-defined
structure. Moreover each map stores a hash of the map itself which is checked
during validation. To perform validation please use `das_js_validate` script.

Import DAS and KWS maps
-----------------------

DAS and KWS maps need to be imported into MongoDB. This can be done via
`das_js_import` script which accept location of DAS maps on local filesystem.

Fetch DAS and KWS maps
----------------------

DAS maps can be fetched from github.com/dmwm/DASMaps repository via
`das_js_fetch` script. These maps are good to go and uploaded to this
repository by developers.
