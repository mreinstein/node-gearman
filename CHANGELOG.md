# 2.0.0

* **breaking change** gearman is now a factory function. The new way to 
instantiate: 
```javascript
const gearman = require('gearman')
let client = gearman(...)
```
* ported from coffeescript to es6
* switched to strict mode
* added travis CI
* added this changelog