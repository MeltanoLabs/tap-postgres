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

| Setting                           | Required | Default                      | Description                                                                                                                                                                                                                                                                                              |
| :-------------------------------- | :------- | :--------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| host                              | False    | None                         | Hostname for postgres instance. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                      |
| port                              | False    | 5432                         | The port on which postgres is awaiting connection. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                   |
| user                              | False    | None                         | User name used to authenticate. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                      |
| password                          | False    | None                         | Password used to authenticate. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                       |
| database                          | False    | None                         | Database name. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                                       |
| max_record_count                  | False    | None                         | Optional. The maximum number of records to return in a single stream.                                                                                                                                                                                                                                    |
| sqlalchemy_url                    | False    | None                         | Example postgresql://[username]:[password]@localhost:5432/[db_name]                                                                                                                                                                                                                                      |
| filter_schemas                    | False    | None                         | If an array of schema names is provided, the tap will only process the specified Postgres schemas and ignore others. If left blank, the tap automatically determines ALL available Postgres schemas.                                                                                                     |
| dates_as_string                   | False    | 0                            | Defaults to false, if true, date, and timestamp fields will be Strings. If you see ValueError: Year is out of range, try setting this to True.                                                                                                                                                           |
| ssh_tunnel                        | False    | None                         | SSH Tunnel Configuration, this is a json object                                                                                                                                                                                                                                                          |
| ssh_tunnel.enable                 | False    | 0                            | Enable an ssh tunnel (also known as bastion server), see the other ssh_tunnel.* properties for more details                                                                                                                                                                                              |
| ssh_tunnel.host                   | False    | None                         | Host of the bastion server, this is the host we'll connect to via ssh                                                                                                                                                                                                                                    |
| ssh_tunnel.username               | False    | None                         | Username to connect to bastion server                                                                                                                                                                                                                                                                    |
| ssh_tunnel.port                   | False    | 22                           | Port to connect to bastion server                                                                                                                                                                                                                                                                        |
| ssh_tunnel.private_key            | False    | None                         | Private Key for authentication to the bastion server                                                                                                                                                                                                                                                     |
| ssh_tunnel.private_key_password   | False    | None                         | Private Key Password, leave None if no password is set                                                                                                                                                                                                                                                   |
| ssl_enable                        | False    | 0                            | Whether or not to use ssl to verify the server's identity. Use ssl_certificate_authority and ssl_mode for further customization. To use a client certificate to authenticate yourself to the server, use ssl_client_certificate_enable instead. Note if sqlalchemy_url is set this will be ignored.      |
| ssl_client_certificate_enable     | False    | 0                            | Whether or not to provide client-side certificates as a method of authentication to the server. Use ssl_client_certificate and ssl_client_private_key for further customization. To use SSL to verify the server's identity, use ssl_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_mode                          | False    | verify-full                  | SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) for more information. Must be one of disable, allow, prefer, require, verify-ca, or verify-full. Note if sqlalchemy_url is set this will be ignored.                    |
| ssl_certificate_authority         | False    | ~/.postgresql/root.crl       | The certificate authority that should be used to verify the server's identity. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored.                                                                       |
| ssl_client_certificate            | False    | ~/.postgresql/postgresql.crt | The certificate that should be used to verify your identity to the server. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored.                                                                           |
| ssl_client_private_key            | False    | ~/.postgresql/postgresql.key | The private key for the certificate you provided. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored.                                                                                                    |
| ssl_storage_directory             | False    | .secrets                     | The folder in which to store SSL certificates provided as raw values. When a certificate/key is provided as a raw value instead of as a filepath, it must be written to a file before it can be used. This configuration option determines where that file is created.                                   |
| default_replication_method        | False    | FULL_TABLE                   | Replication method to use if there is not a catalog entry to override this choice. One of `FULL_TABLE`, `INCREMENTAL`, or `LOG_BASED`.                                                                                                                                                                   |
| stream_maps                       | False    | None                         | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html).                                                                                                                                                              |
| stream_map_config                 | False    | None                         | User-defined config values to be used within map expressions.                                                                                                                                                                                                                                            |
| faker_config                      | False    | None                         | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly).                                                 |
| faker_config.seed                 | False    | None                         | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator                                                                                                                                                                                |
| faker_config.locale               | False    | None                         | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization                                                                                                                                                                                    |
| flattening_enabled                | False    | None                         | 'True' to enable schema flattening and automatically expand nested properties.                                                                                                                                                                                                                           |
| flattening_max_depth              | False    | None                         | The max depth to flatten schemas.                                                                                                                                                                                                                                                                        |
| batch_config                      | False    | None                         |                                                                                                                                                                                                                                                                                                          |
| batch_config.encoding             | False    | None                         | Specifies the format and compression of the batch files.                                                                                                                                                                                                                                                 |
| batch_config.encoding.format      | False    | None                         | Format to use for batch files.                                                                                                                                                                                                                                                                           |
| batch_config.encoding.compression | False    | None                         | Compression format to use for batch files.                                                                                                                                                                                                                                                               |
| batch_config.storage              | False    | None                         | Defines the storage layer to use when writing batch files                                                                                                                                                                                                                                                |
| batch_config.storage.root         | False    | None                         | Root path to use when writing batch files.                                                                                                                                                                                                                                                               |
| batch_config.storage.prefix       | False    | None                         | Prefix to use when writing batch files.                                                                                                                                                                                                                                                                  |

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

### Setting Up SSL Files


We have set the provided keys in the .ssl directory to be valid for multiple centuries. However, we have also provided configuration instructions below to create all of the necessary files for testing SSL.

A list of each file and its purpose:
1. `ca.crt`: CA for client's certificate (stored on the server)
1. `cert.crt`: Client's certificate (stored on the client)
1. `pkey.key`: Client's private key (stored on the client)
1. `public_pkey.key`: Client's private key with incorrect file permissions (stored on the client)
1. `root.crt`: CA for server's certificate (stored on the client)
1. `server.crt`: Server's certificate (stored on the server)
1. `server.key`: Server's private key (stored on the server)

Run the following command to generate all relevant SSL files, with certificates valid for two centuries (73048 days).

```bash
chmod 0600 ssl/*.key
openssl req -new -x509 -days 73048 -nodes -out ssl/server.crt -keyout ssl/server.key -subj "/CN=localhost" &&
openssl req -new -x509 -days 73048 -nodes -out ssl/cert.crt -keyout ssl/pkey.key -subj "/CN=postgres" &&
cp ssl/server.crt ssl/root.crt &&
cp ssl/cert.crt ssl/ca.crt &&
cp ssl/pkey.key ssl/public_pkey.key &&
chown 999:999 ssl/server.key &&
chmod 600 ssl/server.key &&
chmod 600 ssl/pkey.key &&
chmod 644 ssl/public_pkey.key
```

Now that all of the SSL files have been set up, you're ready to set up tests with pytest.

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
   - Alternatively, provide an sqlalchemy url. Keep in mind that many other configuration options are ignored when an sqlalchemy url is set, and ideally you should be able to accomplish everything through other configuration options. Consider making an issue in the [tap-postrges repository](https://github.com/MeltanoLabs/tap-postgres) if you find a reasonable use case that is unsupported by current configuration options.
   - Note that when your connection details are used, it will be from the perspective of the bastion server. This could change the meaning of local IP address or keywords such as "localhost".
 - The hostname or ip address of the bastion server, provided in the `ssh.host` configuration option.
 - The port for use with the bastion server, provided in the `ssh.port` configuration option.
 - The username for authentication with the bastion server, provided in the `ssh.username` configuration option. This will require you to have setup an SSH login with the Bastion server.
 - The private key you use for authentication with the bastion server, provided in the `ssh.private_key` configuration option. If your private key is protected by a password (alternatively called a "private key passphrase"), provide it in the `ssh.private_key_password` configuration option. If your private key doesn't have a password, you can safely leave this field blank.

After everything has been configured, be sure to indicate your use of an ssh tunnel to the tap by configuring the `ssh.enable` configuration option to be `True`. Then, you should be able to connect to your privately accessible Postgres database through the bastion server.

## Log-Based Replication

Log-based replication is an alternative to full-table and incremental syncs and syncs all changes to the database, including deletes. This feature is built based on [postgres replication slots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS).

### Negatives of Log-Based Replication

1. Managing replication slots - Log-based replication has to be set up and maintained on the database. This tap attempts to abstract away as much complexity as possible, but there's still potentially manual effort needed
2. Log Files - When a replication slot is setup the file that holds these logs will continue to grow until consumed, this can cause issues if the tap doesn't ingest these quickly enough due to outages, etc.

If and when someone finds more, please add them to this list!

### Implementation Details
Log-based replication will modify the schemas output by the tap. Specifically, all fields will be made nullable and non-required. The reason for this is that when the tap sends a message indicating that a record has been deleted, that message will leave all fields for that record (except primary keys) as null. The stream's schema must be capable of accommodating these messages, even if a source field in the database is not nullable. As a result, log-based schemas will have all fields nullable.

Note that changing what streams are selected after already beginning log-based replication can have unexpected consequences. To ensure consistent output, it is best to keep selected streams the same across invocations of the tap.

Note also that using log-based replication will cause the replication key for all streams to be set to "_sdc_lsn", which is the Postgres LSN for the record in question.

### How to Set Up Log-Based Replication

1. Ensure you are using  PostgresSQL 9.4 or higher.
1. Need to access the master postgres instance
1. Install the wal2json plugin for your database. Example instructions are given below for a Postgres 15.0 database running on Ubuntu 22.04. For more information, or for alternative versions/operating systems, refer to the [wal2json documentation](https://github.com/eulerto/wal2json)
    - Update and upgrade apt if necessary.
      ```bash
      sudo apt update
      sudo apt upgrade -y
      ```
    - Prepare by making prerequisite installations.
      ```bash
      sudo apt install curl ca-certificates
      sudo install -d /usr/share/postgresql-common/pgdg
      ```
    - Import the repository keys for the Postgres Apt repository
      ```bash
      sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
      ```
    - Create the pgdg.list file.
      ```bash
      sudo sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
      ```
    - Use the Postgres Apt repository to install wal2json
      ```bash
      sudo apt update
      sudo apt-get install postgresql-server-dev-15
      export PATH=/usr/lib/postgresql/15/bin:$PATH
      sudo apt-get install postgresql-15-wal2json
      ```
1. Configure your database with wal2json enabled.
    - Edit your `postgresql.conf` configuration file so the following parameters are appropriately set.
      ```
      wal_level = logical
      max_replication_slots = 10
      max_wal_senders = 10
      ```
    - Restart PostgresSQL
    - Create a replication slot for tap-postgres.
      ```sql
      SELECT * FROM pg_create_logical_replication_slot('tappostgres', 'wal2json');
      ```
1. Ensure your configuration for tap-postgres specifies host, port, user, password, and database manually, without relying on an sqlalchemy url.
1. Use the following metadata modification in your `meltano.yml` for the streams you wish to have as log-based. Note that during log-based replication, we do not support any replication key other than `_sdc_lsn`.
    ```yml
    metadata:
      "*":
        replication_method: LOG_BASED
        replication_key: _sdc_lsn
    ```
