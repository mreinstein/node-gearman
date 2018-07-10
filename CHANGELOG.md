# 2.1.0
* move from nodeunit to tap
* style tweaks
* use const instead of let in more places

# 2.0.5
* move away from the deprecated Buffer constructor node API
* update examples, removing leftover bits of coffeescript

# 2.0.4
* updated nodeunit dependency
* updated package.json

# 2.0.3
* fixes https://github.com/mreinstein/node-gearman/issues/21

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