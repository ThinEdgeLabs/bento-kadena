# Bento - 0.1.0

Bento is an indexing solution for [Kadena](https://kadena.io) blockchain written in Rust.

[![Build Status:](https://github.com/ThinEdgeLabs/bento/workflows/CI%20Tests/badge.svg)](https://github.com/ThinEdgeLabs/bento/actions?query=workflow%3A%22CI+Tests%22+branch%3Amain)

## Features

- out-of-the-box indexes blocks, transactions, events and token transfers
- automatically removes orphan blocks
- handles missed blocks
- HTTP API

## Coming soon

- an easy way to extend it and index any custom modules (eg. Marmalade)

## Setup

### Prerequisites

- [Chainweb Node](https://github.com/kadena-io/chainweb-node)
- [Docker] (optional)

#### Using Docker

The fastest way is to use Docker / Docker Compose as the repo already comes with a docker-compose configuration file.
Alternatively you would need to install PostgreSQL, [rust and cargo to build](Build with Cargo) and then execute the binaries.

**Important**: the `headerStream` chainweb node config needs to be set to `true` for the default streaming mode. You can check the [configuring the node](https://github.com/kadena-io/chainweb-data#configuring-the-node) section of `chainweb-data` for more details. Alternatively, the `poll` subcommand indexes new blocks by periodically polling the node and works without `headerStream`.

Once the node is setup and synced you can continue with the installation:

1. Clone the repository:

```
git clone git@github.com:ThinEdgeLabs/bento.git
```

2. Create a `.env` file, check the `.env-example` to see how it should look like.
3. Start the containers:

```
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

#### Build with Cargo

1. Install Rust and Cargo using [rustup].

2. Build

```
cargo build --release
```

The binaries are available at `./target/release/api` and `./target/release/indexer`.

## Usage

If you used Docker the _indexer_ and _api_ services should already be up and running. Additionally you can run one of the available indexer subcommands like this:

```bash
# docker compose -f docker-compose.yml -f docker-compose.prod.yml run indexer [subcommand]
docker compose -f docker-compose.yml -f docker-compose.prod.yml run indexer backfill
```

Manually starting the services:

**API**:

```bash
./target/release/api

[2023-09-10T08:49:14Z INFO  actix_server::builder] starting 8 workers
[2023-09-10T08:49:14Z INFO  actix_server::server] Actix runtime found; starting in Actix runtime
```

**Indexer**:

```bash
By default new blocks are indexed as they are mined. For backfilling and filling gaps use the subcommands
Usage: indexer [COMMAND]

Commands:
  backfill  Backfill blocks
  poll      Periodically poll the node for new blocks (alternative to the default SSE stream, does not require headerStream on the node)
  gaps      Index missed blocks
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

## API

Available endpoints:

- GET /tx/{request_key} - get tx result for given request key. If it's a multi-step tx, it will return all completed steps as well.
- POST /txs - get tx results for multiple request keys. Payload example:

```json
{ "request_keys": ["req-key-1", "req-key-2"] }
```

- GET /transfers?from={account_from}&to={account_to}&min_height={100}
- GET /balance/{account} - get balances of all tokens for given account
- GET /balance/{account}/{module} - get token balance on all chains for given account and module

## Development

### Setting up Bento locally

1. Install Rust using [rustup], which allows you to easily switch between Rust versions. Bento currently supports Rust Stable and Rust Beta.

2. Install [diesel](https://diesel.rs/guides/getting-started) using cargo:

```
cargo install diesel_cli --no-default-features --features postgres
```

3. Install [docker]. Alternatively, you can manually install PostgreSQL on your local machine.

4. Clone this repository and open it in your favorite editor.

5. Create a `.env` file in the project directory with database connection details, Chainweb node host and others. See [.env-sample](.env-sample) for an example how this should look like.

6. Start postgres and adminer containers:

```
docker compose up -d
```

7. Create the database used for tests:

```
diesel database setup --database-url=postgres://postgres:postgres@localhost/bento_test
```

8. To confirm everything is setup correctly, try running the test suite:

```bash
cargo test
```

9. Start the indexer using cargo:

```
cargo run --bin indexer
```

10. Start the api using cargo:

```
cargo run --bin api
```

### Coding Style

We follow the [Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md), enforced using [rustfmt](https://github.com/rust-lang/rustfmt).
To run rustfmt tests locally:

1. Use rustup to set rust toolchain to the version specified in the
   [rust-toolchain file](./rust-toolchain).

2. Install the rustfmt and clippy by running

   ```
   rustup component add rustfmt
   rustup component add clippy
   ```

3. Run clippy using cargo from the root of your bento repo.

   ```
   cargo clippy
   ```

   Each PR needs to compile without warning.

4. Run rustfmt using cargo from the root of your bento repo.

   To see changes that need to be made, run

   ```
   cargo fmt --all -- --check
   ```

   If all code is properly formatted (e.g. if you have not made any changes),
   this should run without error or output.
   If your code needs to be reformatted,
   you will see a diff between your code and properly formatted code.
   If you see code here that you didn't make any changes to
   then you are probably running the wrong version of rustfmt.
   Once you are ready to apply the formatting changes, run

   ```
   cargo fmt --all
   ```

   You won't see any output, but all your files will be corrected.

You can also use rustfmt to make corrections or highlight issues in your editor.
Check out [their README](https://github.com/rust-lang/rustfmt) for details.

[rustup]: https://rustup.rs/
[docker]: https://www.docker.com

## Contributing

If you want to contribute to Bento, please follow the [coding style rules](#coding-style).

## License

<!-- REUSE-IgnoreStart -->

Copyright 2023 ThinEdgeLabs LLC.

Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
[https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)> or the MIT license
<LICENSE-MIT or [https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT)>, at your
option. Files in the project may not be
copied, modified, or distributed except according to those terms.

<!-- REUSE-IgnoreEnd -->
