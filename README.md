# dcrawl - Fast Ethereum Crawler

[![License: Apache](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Stability: experimental](https://img.shields.io/badge/stability-experimental-orange.svg)

## Introduction

Tool to crawl the Ethereum network, based on the nim language implementation of the Discovery v5 protocol,
part of the nim-eth library.

## Prerequisites

Thanks to the included nimbus-build-system, the library is almost self-contained,
requiring only `git` and a C compiler to build and run.

## Quick start

```
# download dependencies and build compiler
make -j4 update
# build the crawler
make -j4
# run the crawler with ethereum boot node
./run.sh
```

Configuration options:
```
./run.sh --help
```

## License

Licensed and distributed under either of

* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

or

* Apache License, Version 2.0, ([LICENSE-APACHEv2](LICENSE-APACHEv2) or http://www.apache.org/licenses/LICENSE-2.0)

at your option. This file may not be copied, modified, or distributed except according to those terms.
