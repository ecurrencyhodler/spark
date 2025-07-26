# Spark

![spark](https://github.com/user-attachments/assets/f3d71a04-4027-42f2-b02a-e7a06616e33a)

## [mise](https://mise.jdx.dev/)

To install all of our protobuf, rust, and go toolchains install [mise](https://mise.jdx.dev/getting-started.html), then run:

```
mise trust
mise install
```

**Recommended**: Add [mise shell integration](https://mise.jdx.dev/getting-started.html#activate-mise) so that the mise environment will automatically activate when you are this repo, giving you access to all executables and environment variables. Otherwise you will need will need to either manually `mise activate [SHELL]` or run all commands with the `mise exec` prefix.

## pkg-config

We use `pkg-config` in the build process.
One way to install it is through brew:

```
brew install pkgconf
```

## [lefthook](https://lefthook.dev/) (optional)

Lefthook gives us an easy way to declare pre-commit hooks (as well as other git hooks) so you're
not pushing up PRs that immediately fail in CI.

You can either install it through `mise` (above, recommended) or brew:

```
brew install lefthook
```

Once it's installed, run `lefthook install` to install the git hooks, which will automatically run
when you did `git commit`. You can also run the hooks manually with `lefthook run pre-commit`.

## Generate proto files

After modifying the proto files, you can generate the Go files with the following command:

```
make
```

## Bitcoind

Our SO implementation uses ZMQ to listen for block updates from bitcoind. Install it with:

```
brew install zeromq
```

Note: whatever bitcoind you are running will also need to have been compiled with ZMQ.
The default installation via brew has ZMQ, but binaries downloaded from the bitcoin core
website do not.

```
brew install bitcoin
```

## DB Migrations

We use atlas to manage our database migrations. Install via `mise install`.

To make a migration, follow these steps:

- Make your change to the schema, run `make ent`
- Generate migration files by running `./scripts/gen-migration.sh <name>`:
- When running in minikube or `run-everything.sh`, the migration will be automatically
  applied to each operator's database. But if you want to apply a migration manually, you can run (e.g. DB name is `sparkoperator_0`):

```
atlas migrate apply --dir "file://so/ent/migrate/migrations" --url "postgresql://127.0.0.1:5432/sparkoperator_0?sslmode=disable"
```

- Commit the migration files, and submit a PR.

If you are adding atlas migrations for the first time to an existing DB, you will need to run the migration command with the `--baseline` flag.

```
atlas migrate apply --dir "file://so/ent/migrate/migrations" --url "postgresql://127.0.0.1:5432/sparkoperator_0?sslmode=disable" --baseline 20250228224813
```

## VSCode

If spark_frost.udl file has issue with VSCode, you can add the following to your settings.json file:

```
"files.associations": {
    "spark_frost.udl": "plaintext"
}
```

## Linting

Golang linting uses `golangci-lint`, installed with `mise install`.

To run the linters, use either of

```
mise lint

golangci-lint run
```

## Run tests

### Unit tests

```
mise test-go # works from any directory
mise test # works from the spark folder
```

or

In spark folder, run:

```
go test $(go list ./... | grep -v -E "so/grpc_test|so/tree")
```

## E2E tests

The E2E test environment can be run locally in minikube via `./scripts/local-test.sh` for hermetic testing (recommended) or run locally via `./run-everything.sh`.

#### Local Setup (`./run-everything.sh`)

```
brew install tmux
brew install sqlx-cli # required for LRC20 Node
brew install cargo # required for LRC20 Node
```

##### bitcoind

See bitcoin section above.

##### postgres

You also need to enable TCP/IP connections to the database.
You might need to edit the following files found in your `postgres` data directory. If you installed `postgres` via homebrew, it is probably in `/usr/local/var/postgres`. If you can connect to the database via `psql`, you can find the data directory by running `psql -U postgres -c "SHOW data_directory;"`.

A sample `postgresql.conf`:

```
hba_file = './pg_hba.conf'
ident_file = './pg_ident.conf'
listen_addresses = '*'
log_destination = 'stderr'
log_line_prefix = '[%p] '
port = 5432
```

A sample `pg_hba.conf`:

```
#type  database  user  address       method
local   all       all                trust
host    all       all   127.0.0.1/32 trust
host    all       all   ::1/128      trust
```

### Running tests

Golang integration tests are in the spark/so/grpc_test folder.
JS SDK integration tests are across the different JS packages, but can be run together in the js/sdks directory, or in each package's own directory, via `yarn test:integration`.

In the root folder, run:

```
# Hermetic/Minikube environment
#
# Usage:
#   ./scripts/local-test.sh [--dev-spark] [--dev-lrc20] [--no-spark-debug] [--keep-data]
#
# Options:
#   --dev-spark         - Sets USE_DEV_SPARK=true and SPARK_DEBUG=true to use the locally built dev spark image and enable headless dlv debugging. Requires the image to be built with the debug flag (which is the default from build-to-minikube.sh).
#   --dev-lrc20         - Sets USE_DEV_LRC20=true to use the locally built dev lrc20 image
#   --no-spark-debug    - Sets USE_DEV_SPARK=true and SPARK_DEBUG=false to configure the local dev spark container to run without headless dlv debugger. If you are using a local "production" image, you must use this flag.
#   --keep-data         - Sets RESET_DBS=false to preserve existing test data (databases and blockchain)
#   --operators|-o      - The number of Spark operators to deploy (default: 5)
#
# Environment Variables:
#   RESET_DBS           - Whether to reset operator databases and bitcoin blockchain (default: true)
#   USE_DEV_SPARK       - Whether to use the dev spark image built into minikube (default: false)
#   USE_DEV_LRC20       - Whether to use the dev lrc20 image built into minikube (default: false)
#   SPARK_DEBUG         - Whether to configure spark to run with headless dlv debugger. If you are using a local "production" image, this must be set to false. (default: false)
#   SPARK_TAG           - Image tag to use for both Spark operator and signer (default: latest)
#   LRC20_TAG           - Image tag to use for LRC20 (default: latest)
#   USE_LIGHTSPARK_HELM_REPO - Whether to fetch helm charts from remote repo (default: false)
#   OPS_DIR             - Path to the Lightspark ops repository which contains helm charts (auto-detected if not set)
#   LRC20_REPLICAS      - The number of LRC20 replicas to deploy (default: 1)
#   NUM_SPARK_OPERATORS - The number of Spark operators to deploy (default: 5)

./scripts/local-test.sh

# CTR-C when done to remove shut down port forwarding
```

OR

```
# Local environment
./run-everything.sh
```

then run your tests

```
mise test-grpc  # from anywhere in the repo

# OR

go test -failfast=false -p=2 ./so/grpc_test/...  # in the spark folder

# OR

gotestsum --format testname --rerun-fails ./so/grpc_test/...  # if you want prettier results and retries
```

In the sdks/js folder, you can run:

```
yarn install
yarn build
yarn test:integration
```

#### Troubleshooting

1. For local testing, operator (go) and signer (rust) logs are found in `_data/run_X/logs`. For minikube, logs are found via kubernetes. [k9s](https://k9scli.io/) is a great tool to investigate your minikube k8 cluster.
2. If you don't want to deal with `tmux` commands yourself, you can easily interact with tmux using the `iterm2` GUI and tmux control mode.
   From within `iterm2`, you can run:

`tmux -CC attach -t operator`

3. The first time you run `run-everything.sh` it will take a while to start up. You might actually need to run it a couple of times for everything to work properly. Attach to the `operator` session and check out the logs.

4. Having trouble with mise? You can always run `mise implode` and it will remove mise entirely so you can start over.

## Testing against local SSP changes

To run the server with local changes go to your sparkcore folder in the lightspark webdev project:

```
export QUART_APP=sparkcore.server
export QUART_ENV=development
export GRPC_DNS_RESOLVER=native
export QUART_CONFIG=core-dev.py

# run this line if accessed AWS PROD earlier, since PROD credentials supersede dev credentials.
unset AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_TOKEN_EXPIRATION

aws sso login
QUART_CONFIG=core-dev.py uv run quart -A sparkcore.server -E development run
```

Follow commented direction here: https://github.com/lightsparkdev/webdev/blob/150dd1ceecf85e122ec3ebd608d3c9f7c44f1969/sparkcore/sparkcore/spark/handlers/__tests__/test_transfer_v2.py#L35

Update the sspClientOptions for dev-regtest-config.json

```
"sspClientOptions": {
  "baseUrl": "http://127.0.0.1:5000",
  "schemaEndpoint": "graphql/spark/rc",
  "identityPublicKey": "028c094a432d46a0ac95349d792c2e3730bd60c29188db716f56a99e39b95338b4"
}
```

Running yarn cli:dev will now point to you local SSP server and will be connected to dev SOs and dev dbs.

## Releasing

Releases are triggered by creating a release, which kicks off the [Release](https://github.com/lightsparkdev/spark/actions/workflows/release.yaml)
workflow.

Every hour we build a [Release Candidate](https://github.com/lightsparkdev/spark/actions/workflows/rc.yaml).
The Release Candidate does the following:
- Chooses the most recent SO build that has successfully gone through CI.
- Runs all hermetic tests against the build.
- Restarts the SOs and LRC20 node in Loadtest with the build.
- (Soon) runs Artillery tests.

In regular circumstances (i.e. unless fixing a production issue), releasing from the release
candidate is greatly preferred. You can create a release from the release candidate [here](https://github.com/lightsparkdev/spark/releases/new?target=rc).

Each release has a name, which should follow `prod-spark-YYYY.MM.DD.N`, where `N` starts at 1 and
increments for each release on a given day.
