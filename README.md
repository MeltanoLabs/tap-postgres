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
| ssh_tunnel.enable   | True (if ssh_tunnel set) | False   | Enable an ssh tunnel (also known as bastion server), see the other ssh_tunnel.* properties for more details.
| ssh_tunnel.host | True (if ssh_tunnel set) | False   | Host of the bastion server, this is the host we'll connect to via ssh
| ssh_tunnel.username | True (if ssh_tunnel set) | False   |Username to connect to bastion server
| ssh_tunnel.port | True (if ssh_tunnel set) | 22 | Port to connect to bastion server
| ssh_tunnel.private_key | True (if ssh_tunnel set) | None | Private Key for authentication to the bastion server
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

## SSH Tunnels

This tap supports connecting to a Postgres database via an SSH tunnel (also known as a bastion server). This is useful if you need to connect to a database that is not publicly accessible. This is the same as using `ssh -L`, but this is done inside the tap itself.

## What is an SSH Tunnel?

An SSH tunnel is a method to securely forward network traffic. It uses the SSH protocol to encapsulate other protocols like HTTP, MySQL, Postgres, etc. This is particularly useful in scenarios where you need to access a service that is behind a firewall or in a network that you can't reach directly. In the context of this tap, you can use an SSH tunnel to access a Postgres database that's not accessible to the wider internet.

Here's a basic illustration of how an SSH tunnel works:
```
+-------------+             +-------------+             +-------------+
|    Local    | SSH tunnel  |   Bastion   |   Direct    |  Postgres   |
|   Machine   | <=========> |   Server    | <=========> |     DB      |
+-------------+ (encrypted) +-------------+ (unsecured) +-------------+
```

1. Local Machine: This is wherever this tap is running from, where you initiate the SSH tunnel. It's also referred to as the SSH client.
1. Bastion Server: This is a secure server that you have SSH access to, and that can connect to the remote server. All traffic in the SSH tunnel between your local machine and the bastion server is encrypted.
1. Remote Server: This is the server you want to connect to, in this case a PostgreSQL server. The connection between the bastion server and the remote server is a normal, potentially unsecured connection. However, because the bastion server is trusted, and because all traffic between your local machine and the bastion server is encrypted, you can safely transmit data to and from the remote server.

### Obtaining Keys

Setup
1. Ensure your bastion server is online.
1. Ensure you bastion server can access your Postgres database.
1. Have some method of accessing your bastion server. This could either be through password-based SSH authentication or through a hardwired connection to the server.
1. Install `ssh-keygen` on your client machine if it is not already installed.

Creating Keys
1. Run the command `ssh-keygen`.
    1. Enter the directory where you would like to save your key. If you're in doubt, the default directory is probably fine.
        - If you get a message similar to the one below asking if you wish to overwrite a previous key, enter `n`, then rerun `ssh-keygen` and manually specify the output_keyfile using the `-f` flag.
            ```
            /root/.ssh/id_rsa already exists.
            Overwrite (y/n)?
            ```
    1. If you wish, enter a passphrase to provide additional protection to your private key. SSH-based authentication is generally considered secure even without a passphrase, but a passphrase can provide an additional layer of security.
    1. You should now be presented with a message similar to the following, as well as a key fingerprint and ascii randomart image.
        ```
        Your identification has been saved in /root/.ssh/id_rsa
        Your public key has been saved in /root/.ssh/id_rsa.pub
        ```
1. Navigate to the indicated directory and find the two keys that were just generated. The file named `id_rsa` is your private key. Keep it safe. The file named `id_rsa.pub` is your public key, and needs to be transferred to your bastion server for your private key to be used.

Copying Keys
1. Now that you have a pair of keys, the public key needs to be transferred to your bastion server.
1. If you already have password-based SSH authentication configured, you can use the command `ssh-copy-id [user]@[host]` to copy your public key to the bastion server. Then you can move on to [using your keys](#using-your-keys)
1. If not, you'll need some other way to access your bastion server. Once you've accessed it, copy the `id_rsa.pub` file onto the bastion server in the `~/.ssh/authorized_keys` file. You could do this using a tool such as `rsync` or with a cloud-based service.
    - Keep in mind: it's okay if your public key is exposed to the internet through a file-share or something similar. It is useless without your private key.

### Using Your Keys

To connect through SSH, you will need to determine the following pieces of information. If you're missing something, go back to [the section on Obtaining Keys](#obtaining-keys) to gather all the relevant information.
 - The connection details for your Postgres database, the same as any other tap-postgres run. This includes host, port, username, password and database.
   - Alternatively, provide an sqlalchemy url. Keep in mind that many other configuration options are ignored when an sqlalchemy url is set, and ideally you should be able to accomplish everything through other configuration options. Consider making an issue in the [tap-postrges repository](https://github.com/MeltanoLabs/tap-postgres) if you find a reasonable use-case that is unsupported by current configuration options.
   - Note that when your connection details are used, it will be from the perspective of the bastion server. This could change the meaning of local IP address or keywords such as "localhost".
 - The hostname or ip address of the bastion server, provided in the `ssh.host` configuration option.
 - The port for use with the bastion server, provided in the `ssh.port` configuration option.
 - The username for authentication with the bastion server, provided in the `ssh.username` configuration option. This will require you to have setup an SSH login with the bastion server.
 - The private key you use for authentication with the bastion server, provided in the `ssh.private_key` configuration option. If your private key is protected by a password (alternatively called a "private key passphrase"), provide it in the `ssh.private_key_password` configuration option. If your private key doesn't have a password, you can safely leave this field blank.

After everything has been configured, be sure to indicate your use of an ssh tunnel to the tap by configuring the `ssh.enable` configuration option to be `True`. Then, you should be able to connect to your privately accessible Postgres database through the bastion server.
