Gearman Client for nodejs 
========

Why Another Gearman Client?
--------
I evaluated several existing libraries, but none of them meet my needs. https://github.com/andris9/gearnode has a lot of gearman features, but is not stable and has implementation problems. https://github.com/andris9/node-gearman is more stable, but has a limited feature set. https://github.com/cramerdev/gearman-node has great networking code, but an extremely limited feature set. All of them haven't been updated in almost a year.

Use this one!

Benefits:
* full implementation of worker and client functionality
* very small (thanks to coffeescript goodness)
* fast
* fairly low level, lean abstractions over raw gearman protocol

Install
--------
```
git clone https://github.com/mreinstein/node-gearman.git
cd node-gearman
npm install
```

TODO
--------
* usage examples
* tests
* documentation

