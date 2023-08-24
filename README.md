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

| Setting                      | Required | Default | Description |
|:-----------------------------|:--------:|:-------:|:------------|
| host                         | False    | None    | Hostname for postgres instance. Note if sqlalchemy_url is set this will be ignored. |
| port                         | False    |    5432 | The port on which postgres is awaiting connection. Note if sqlalchemy_url is set this will be ignored. |
| user                         | False    | None    | User name used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| password                     | False    | None    | Password used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| database                     | False    | None    | Database name. Note if sqlalchemy_url is set this will be ignored. |
| sqlalchemy_url               | False    | None    | Example postgresql://[username]:[password]@localhost:5432/[db_name] |
| filter_schemas               | False    | None    | If an array of schema names is provided, the tap will only process the specified Postgres schemas and ignore others. If left blank, the tap automatically determines ALL available Postgres schemas. |
| ssh_tunnel                   | False    | None    | SSH Tunnel Configuration, this is a json object |
| ssh_tunnel.enable   | True (if ssh_tunnel set) | False   | Enable an ssh tunnel (also known as bastion host), see the other ssh_tunnel.* properties for more details.
| ssh_tunnel.host | True (if ssh_tunnel set) | False   | Host of the bastion host, this is the host we'll connect to via ssh
| ssh_tunnel.username | True (if ssh_tunnel set) | False   |Username to connect to bastion host
| ssh_tunnel.port | True (if ssh_tunnel set) | 22 | Port to connect to bastion host
| ssh_tunnel.private_key | True (if ssh_tunnel set) | None | Private Key for authentication to the bastion host
| ssh_tunnel.private_key_password | False | None | Private Key Password, leave None if no password is set
| ssl_enable                   | False    |       0 | Whether or not to use ssl to verify the server's identity. Use ssl_certificate_authority and ssl_mode for further customization. To use a client certificate to authenticate yourself to the server, use ssl_client_certificate_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_certificate_enable| False    |       0 | Whether or not to provide client-side certificates as a method of authentication to the server. Use ssl_client_certificate and ssl_client_private_key for further customization. To use SSL to verify the server's identity, use ssl_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_mode                     | False    | verify-full | SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) for more information. Must be one of disable, allow, prefer, require, verify-ca, or verify-full. Note if sqlalchemy_url is set this will be ignored. |
| ssl_certificate_authority    | False    | ~/.postgresql/root.crl | The certificate authority that should be used to verify the server's identity. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_certificate       | False    | ~/.postgresql/postgresql.crt | The certificate that should be used to verify your identity to the server. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_private_key       | False    | ~/.postgresql/postgresql.key | The private key for the certificate you provided. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_storage_directory        | False    | .secrets | The folder in which to store SSL certificates provided as raw values. When a certificate/key is provided as a raw value instead of as a filepath, it must be written to a file before it can be used. This configuration option determines where that file is created. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-postgres --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### SSH Tunnels (Bastion Hosts)

This tap supports connecting to a Postgres database via an SSH tunnel (also known as a bastion host). This is useful if you need to connect to a database that is not publicly accessible. This is the same as using `ssh -L` and `ssh -R`, but this is done inside the tap itself.

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
