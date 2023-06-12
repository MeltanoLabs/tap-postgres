"""Tests standard tap features using the built-in SDK tests library."""
import json

import pendulum
from singer_sdk.testing.templates import TapTestTemplate

from tap_postgres.tap import TapPostgres

TABLE_NAME = "test_replication_key"
SAMPLE_CONFIG_DSA = {
    "sqlalchemy_url": "postgresql://postgres:postgres@10.5.0.5:5432/main",
    "ssh_tunnel": {
        "enable": True,
        "host": "127.0.0.1",
        "port": 2223,
        "username": "melty",
        "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABsQAAAAdzc2gtZH\nNzAAAAgQDSZjQVKBCj57wXTZTFusc/Amp5wet2ugo/Mh+86+v2WDbluFztNZXTA3EtX8p6\nzZtoLZJ/+VCtLqZD7MjJIt4/bPhOjyXOlbtIwL7w80drTxMFBOvuBQkD+TqIzaONwzsN5b\nGcQNACpyz4C2eSUP4KOmOrKXovFI6pMQ22lbqrrQAAABUAv4qw6qJkET1T4J8o0RgzoxNI\nTFkAAACAQQ5w7+2rPlC/GP9ScUCQZTicgzAYlTNOCvIcO4pRj7E1NwNMuafl6xNRjrIYBp\nOqMhDLIBx15Yob0J/6PpE65oeQ8Lq8QboZxO8bio0FGt4qE6mXB4vJq2oOwQkWHzH64x9l\nfmFQNe8KRpd0G/daXBgeF+FEqV2vVsjsjKXxwncAAACAFRvMwvnkzX/c2MaWx78+HJEjjf\nATYt2acoLAH2YRwnhavQyEScNQDiZnBbIr2J21ccvGvFyZT2dtcz83pwFDa9o7Y41EWQG7\nifRPYrj9aHd3TyxeiSGSZlna9ekcfXbIF7+aRHSyEie/YIYUGm73jCW+TDcXK1nQHu7tGL\n1KkBQAAAHox++oGsfvqBoAAAAHc3NoLWRzcwAAAIEA0mY0FSgQo+e8F02UxbrHPwJqecHr\ndroKPzIfvOvr9lg25bhc7TWV0wNxLV/Kes2baC2Sf/lQrS6mQ+zIySLeP2z4To8lzpW7SM\nC+8PNHa08TBQTr7gUJA/k6iM2jjcM7DeWxnEDQAqcs+AtnklD+Cjpjqyl6LxSOqTENtpW6\nq60AAAAVAL+KsOqiZBE9U+CfKNEYM6MTSExZAAAAgEEOcO/tqz5Qvxj/UnFAkGU4nIMwGJ\nUzTgryHDuKUY+xNTcDTLmn5esTUY6yGAaTqjIQyyAcdeWKG9Cf+j6ROuaHkPC6vEG6GcTv\nG4qNBRreKhOplweLyatqDsEJFh8x+uMfZX5hUDXvCkaXdBv3WlwYHhfhRKldr1bI7Iyl8c\nJ3AAAAgBUbzML55M1/3NjGlse/PhyRI43wE2LdmnKCwB9mEcJ4Wr0MhEnDUA4mZwWyK9id\ntXHLxrxcmU9nbXM/N6cBQ2vaO2ONRFkBu4n0T2K4/Wh3d08sXokhkmZZ2vXpHH12yBe/mk\nR0shInv2CGFBpu94wlvkw3FytZ0B7u7Ri9SpAUAAAAFAZscEj14jPPE+Znbk4FflEe6t2r\nAAAAE3Jvb3RAb3BlbnNzaC1zZXJ2ZXI=\n-----END OPENSSH PRIVATE KEY-----",
    },
}
SAMPLE_CONFIG_ECDSA = {
    "sqlalchemy_url": "postgresql://postgres:postgres@10.5.0.5:5432/main",
    "ssh_tunnel": {
        "enable": True,
        "host": "127.0.0.1",
        "port": 2223,
        "username": "melty",
        "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS\n1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQS9T8ajwjHV1Xl705ShFqry77rS7wrh\nlCN0a4Hf33yapuCBKCRDr+/Y7gISoiER3rez56TQCvIFuKUEgCUsTMSWAAAAsFXWKa1V1i\nmtAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBL1PxqPCMdXVeXvT\nlKEWqvLvutLvCuGUI3Rrgd/ffJqm4IEoJEOv79juAhKiIRHet7PnpNAK8gW4pQSAJSxMxJ\nYAAAAhAOe2L6cmoGh4gZ++o+GiqMQ2WQ3RUfle/gc0G1nhLPhWAAAAE3Jvb3RAb3BlbnNz\naC1zZXJ2ZXIBAgME\n-----END OPENSSH PRIVATE KEY-----",
    },
}
SAMPLE_CONFIG_ED25519 = {
    "sqlalchemy_url": "postgresql://postgres:postgres@10.5.0.5:5432/main",
    "ssh_tunnel": {
        "enable": True,
        "host": "127.0.0.1",
        "port": 2223,
        "username": "melty",
        "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\nQyNTUxOQAAACCjD/qc+T3Cc/k2pbjOUFW0OobeLnoWUAoaBhTQchI26wAAAJgismUyIrJl\nMgAAAAtzc2gtZWQyNTUxOQAAACCjD/qc+T3Cc/k2pbjOUFW0OobeLnoWUAoaBhTQchI26w\nAAAECV2axXFduGtP3RS1f97smocwVLphmETzpWdwi89jWrJaMP+pz5PcJz+TaluM5QVbQ6\nht4uehZQChoGFNByEjbrAAAAE3Jvb3RAb3BlbnNzaC1zZXJ2ZXIBAg==\n-----END OPENSSH PRIVATE KEY-----",
    },
}
SAMPLE_CONFIG_RSA = {
    "sqlalchemy_url": "postgresql://postgres:postgres@10.5.0.5:5432/main",
    "ssh_tunnel": {
        "enable": True,
        "host": "127.0.0.1",
        "port": 2223,
        "username": "melty",
        "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n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\n-----END OPENSSH PRIVATE KEY-----",
    },
}


def test_ssh_tunnel():
    """We expect the SSH environment to already be up"""
    tap = TapPostgres(config=SAMPLE_CONFIG)
    tap.sync_all()
