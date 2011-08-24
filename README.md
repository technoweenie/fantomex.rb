# Fantomex

Rough ruby port of https://github.com/technoweenie/fantomex

TBA

## USAGE

```ruby
require 'fantomex'
queue = Fantomex::Adapters::SequelAdapter.new "sqlite://"
queue.add 'some-message'
puts queue.peek.inspect
```
