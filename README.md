# `tap-postgres`

Singer tap for Postgres.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| sqlalchemy_url      | True     | None    | Example postgresql://postgres:postgres@localhost:5432/postgres |
| ssh_tunnel          | False    | None    | SSH Tunnel Configuration, this is a json object. |
| ssh_tunnel.enable   | True (if ssh_tunnel set) | False   | Enable an ssh tunnel (also known as bastion host), see the other ssh_tunnel.* properties for more details.
| ssh_tunnel.host | True (if ssh_tunnel set) | False   | Host of the bastion host, this is the host we'll connect to via ssh
| ssh_tunnel.username | True (if ssh_tunnel set) | False   |Username to connect to bastion host
| ssh_tunnel.port | True (if ssh_tunnel set) | 22 | Port to connect to bastion host
| ssh_tunnel.private_key | True (if ssh_tunnel set) | None | Private Key for authentication to the bastion host
| ssh_tunnel.private_key_password | False | None | Private Key Password, leave None if no password is set
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-postgres --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.


## Installation

```bash
pipx install meltanolabs-tap-postgres
```

## Usage

You can easily run `tap-postgres` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-postgres --version
tap-postgres --help
tap-postgres --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry pre-commit
poetry install
pre-commit install
```

### Create and Run Tests

Create tests within the `tap_postgres/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-postgres` CLI interface directly using `poetry run`:

```bash
poetry run tap-postgres --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-postgres
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-postgres --version
# OR run a test `elt` pipeline:
meltano elt tap-postgres target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

# SSH Tunnels (Bastion Hosts)

This tap supports connecting to a Postgres database via an SSH tunnel (also known as a bastion host). This is useful if you need to connect to a database that is not publicly accessible. This is the same as using `ssh -L` and `ssh -R`, but this is done inside the tap itself.

## Explanation

An SSH tunnel is a method to securely forward network traffic. It uses the SSH protocol to encapsulate other protocols like HTTP, MySQL, Postgres, etc. This is particularly useful in scenarios where you need to access a service that is behind a firewall or in a network that you can't reach directly.

Here's a basic illustration of how an SSH tunnel works:
+-----------+             +-------------+             +-------------+
|   Local   |  SSH tunnel |   Bastion   |  Direct     |  Postgres   |
|  Machine  |  <========> |   Server    |  <========> |  DB         |
+-----------+ (encrypted) +-------------+ (unsecured) +-------------+

1. Local Machine: This is whereever this tap is running from, where you initiate the SSH tunnel. It's also referred to as the SSH client.
1. Bastion Server: This is a secure server that you have SSH access to, and that can connect to the remote server. All traffic in the SSH tunnel between your local machine and the bastion server is encrypted.
1. Remote Server: This is the server you want to connect to, like a PostgreSQL server. The connection between the bastion server and the remote server is a normal, potentially unsecured connection. However, because the bastion server is trusted, and because all traffic between your local machine and the bastion server is encrypted, you can safely transmit data to and from the remote server.

## Tutorial - How to setup the Keys needed for this to work

Steps
1. Bastian Host must be online (we're going to assume it's a linux box)
2. Bastian Host needs to be able to access your Postgres database
