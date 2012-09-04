Gearman Client for nodejs 
========

I evaluated several existing gearman libraries:

* https://github.com/andris9/node-gearman
* https://github.com/andris9/gearnode
* https://github.com/cramerdev/gearman-node

None of these are implementations of the full protocol, though they do provide some good code snippets. As of the time of this writing, they haven't been updated in 6+ months. 

Benefits:
* full implementation of worker and client functionality
* very small (thanks to coffeescript goodness)
* fast
* fairly low level, lean abstractions over raw gearman protocol

TODO
* wire up the server responses for worker requests
* tests
* usage examples
