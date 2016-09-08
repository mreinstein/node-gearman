# 2.0.2
* expose `connect()`
* remove all references to `this`

# 2.0.1
* removed unused dependency

# 2.0.0

* **breaking change** gearman is now a factory function. The new way to 
instantiate: 
```javascript
const gearman = require('gearman')
let client = gearman(...)
```
* **breaking change** requires node v6+
* ported from coffeescript to es6
* switched to strict mode
* added travis CI
* added this changelog