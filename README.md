# sample_study_notes_14
KYC Stuides

delivery service

```python
import errno
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Protocol

import paramiko
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.core.logging import log


class DeliveryConfigError(Exception):
    """Raised when required SFTP settings (host/credentials/known_hosts) are missing
    or invalid - a configuration problem, not a transient delivery failure."""


class DeliveryError(Exception):
    """Raised when an SFTP delivery attempt fails in a way that's potentially
    transient - e.g. the uploaded file's remote size doesn't match the local one."""


class DeliveryAuthenticationError(DeliveryError):
    """Raised when the SFTP server rejects the configured credentials. Not retried -
    a bad key/username won't fix itself on a second attempt."""


class DeliveryUnavailableError(DeliveryError):
    """Raised when the SFTP server can't be reached after exhausting retries (connection
    refused, timeout, DNS failure, etc.)."""


class DeliveryCollisionError(DeliveryError):
    """Raised when the final remote filename already exists with a DIFFERENT size than
    what was just uploaded - a real, non-transient naming collision (not the same file
    redelivered - see the idempotent-redelivery handling in _attempt_delivery). Not
    retried: retrying a genuine collision just fails the same way every time."""


class DeliveryRemoteDirectoryNotFoundError(DeliveryError):
    """Raised when the SFTP server rejects an upload because the destination directory
    doesn't exist yet on the server (surfaces from paramiko as a plain OSError with
    errno.ENOENT - identical to a missing local file, see docs/decisions.md 2026-08-27,
    which is why this is only detected at the upload call specifically, not via the
    generic OSError/SSHException catch in deliver()). A permanent misconfiguration, not
    a transient failure, so not retried - the landing-zone folder must be created on
    the server first; delivery_service.py never creates remote directories itself."""


class SFTPClientLike(Protocol):
    def put(self, localpath: str, remotepath: str, confirm: bool = True) -> object: ...
    def stat(self, remotepath: str) -> object: ...
    def rename(self, oldpath: str, newpath: str) -> None: ...
    def remove(self, remotepath: str) -> None: ...
    def close(self) -> None: ...


@dataclass
class DeliveryResult:
    remote_path: str
    size_bytes: int
    delivered_at: datetime


@dataclass
class SFTPConnectionSettings:
    """Resolved, ready-to-use connection details for one named SFTP destination -
    lives here (not in sftp_connection_service.py) so that module can import
    DeliveryConfigError from here without a circular import; this is the shape
    deliver()'s optional `connection` argument expects."""

    host: str
    port: int
    username: str
    private_key_path: str
    known_hosts_path: Path
    remote_folder: str


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(
        exc, (paramiko.AuthenticationException, paramiko.BadHostKeyException)
    ):
        return False
    if isinstance(exc, (DeliveryCollisionError, DeliveryRemoteDirectoryNotFoundError)):
        # Checked before the general DeliveryError case below, since both are
        # subclasses - neither a genuine collision nor a missing remote directory
        # can resolve itself on retry.
        return False
    if isinstance(exc, DeliveryError):
        return True
    return isinstance(exc, (paramiko.SSHException, OSError))


class DeliveryService:
    """Delivers a local file to an enterprise SFTP landing zone via paramiko. Does NOT
    transform, poll, or call Fenergo's own API - see CLAUDE.md service boundaries.
    """

    def __init__(self, client_factory: Optional[Callable[[], SFTPClientLike]] = None):
        self._test_client_factory = client_factory

    def _require_setting(self, name: str) -> str:
        value = getattr(settings, name)
        if not value:
            raise DeliveryConfigError(f"{name} must be set to use SFTP delivery")
        return value

    def _connect(
        self, connection: Optional[SFTPConnectionSettings] = None
    ) -> SFTPClientLike:
        if self._test_client_factory is not None:
            return self._test_client_factory()

        if connection is not None:
            host = connection.host
            port = connection.port
            username = connection.username
            key_path = connection.private_key_path
            known_hosts_path = connection.known_hosts_path
        else:
            host = self._require_setting("SFTP_HOST")
            username = self._require_setting("SFTP_USERNAME")
            key_path = self._require_setting("SFTP_PRIVATE_KEY_PATH")
            port = settings.SFTP_PORT
            known_hosts_path = settings.known_hosts_path

        if not Path(key_path).exists():
            raise DeliveryConfigError(
                f"SFTP private key file not found at {key_path} - check "
                "SFTP_PRIVATE_KEY_PATH (or the connection's own "
                "*_PRIVATE_KEY_PATH env var, see sftp_connection_service.py)"
            )

        if not known_hosts_path.exists():
            raise DeliveryConfigError(
                f"SFTP known_hosts file not found at {known_hosts_path} - refusing to "
                "connect without host-key verification (see docs/decisions.md)"
            )

        client = paramiko.SSHClient()
        client.load_host_keys(str(known_hosts_path))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(
            hostname=host,
            port=port,
            username=username,
            key_filename=key_path,
            timeout=settings.SFTP_CONNECT_TIMEOUT_SECONDS,
        )
        return client.open_sftp()

    def deliver(
        self,
        local_path: Path,
        remote_directory: str,
        remote_filename: Optional[str] = None,
        connection: Optional[SFTPConnectionSettings] = None,
    ) -> DeliveryResult:
        """connection (optional, net new 2026-08-24): which named SFTP destination
        to use (see sftp_connection_service.py) - report-driven flows resolve this
        from ReportDefinition.sftp_connection. None (default) keeps the legacy
        behavior: the single global SFTP_HOST/SFTP_PORT/SFTP_USERNAME/
        SFTP_PRIVATE_KEY_PATH settings, unchanged - used by deliver_file() (the
        raw `deliver` CLI command), which has no report/connection context."""
        local_size = local_path.stat().st_size

        remote_filename = remote_filename or local_path.name
        final_remote_path = f"{remote_directory.rstrip('/')}/{remote_filename}"
        temp_remote_path = f"{final_remote_path}.part"

        log_host = connection.host if connection is not None else settings.SFTP_HOST
        log_username = (
            connection.username if connection is not None else settings.SFTP_USERNAME
        )
        log_port = connection.port if connection is not None else settings.SFTP_PORT

        try:
            for attempt in Retrying(
                retry=retry_if_exception(_is_retryable),
                stop=stop_after_attempt(settings.SFTP_RETRY_COUNT),
                wait=wait_exponential(multiplier=1, min=1, max=10),
                reraise=True,
            ):
                with attempt:
                    self._attempt_delivery(
                        local_path,
                        temp_remote_path,
                        final_remote_path,
                        local_size,
                        connection,
                    )
        except paramiko.AuthenticationException as exc:
            raise DeliveryAuthenticationError(
                f"SFTP authentication failed for {log_username}@{log_host}: {exc}"
            ) from exc
        except paramiko.BadHostKeyException:
            # Security-relevant server-identity mismatch - a distinct concern from bad
            # credentials or an unreachable server, surfaced as-is rather than wrapped.
            raise
        except (OSError, paramiko.SSHException) as exc:
            raise DeliveryUnavailableError(
                f"SFTP server {log_host}:{log_port} unavailable after "
                f"{settings.SFTP_RETRY_COUNT} attempt(s): {exc}"
            ) from exc

        delivered_at = datetime.now(timezone.utc)
        log.info(f"Delivered {local_path} to {final_remote_path} ({local_size} bytes)")
        return DeliveryResult(
            remote_path=final_remote_path,
            size_bytes=local_size,
            delivered_at=delivered_at,
        )

    def _attempt_delivery(
        self,
        local_path: Path,
        temp_remote_path: str,
        final_remote_path: str,
        local_size: int,
        connection: Optional[SFTPConnectionSettings] = None,
    ) -> None:
        sftp = self._connect(connection)
        try:
            try:
                sftp.put(str(local_path), temp_remote_path, confirm=True)
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    # Indistinguishable from a missing local file by errno/message
                    # alone (both are "[Errno 2] No such file") - but this one is
                    # raised specifically from the upload call itself, after
                    # _connect() already succeeded, so it can only be the remote
                    # side. See docs/decisions.md 2026-08-27.
                    raise DeliveryRemoteDirectoryNotFoundError(
                        f"Remote directory not found while uploading to "
                        f"{temp_remote_path} - the destination folder doesn't "
                        "exist yet on the SFTP server and must be created there "
                        "first (delivery_service.py never creates it)"
                    ) from exc
                raise
            remote_size = sftp.stat(temp_remote_path).st_size
            if remote_size != local_size:
                raise DeliveryError(
                    f"Uploaded size {remote_size} does not match local size {local_size} "
                    f"for {temp_remote_path}"
                )
            try:
                sftp.rename(temp_remote_path, final_remote_path)
            except OSError as rename_exc:
                # Real SFTP servers (including atmoz/sftp's OpenSSH-based
                # sftp-server, confirmed live) refuse SSH_FXP_RENAME onto an
                # existing destination. If it's the exact same content already
                # delivered - e.g. an Airflow task retry after the first attempt
                # actually succeeded - treat this as done, not a failure. Only
                # compares size, not a checksum: proportionate for "the same
                # upload retried," not a general integrity guarantee - a true
                # collision between two different files of the exact same byte
                # count would be misclassified as already-delivered. Downstream
                # consumers already have the .mrk SHA256 sidecar for that case.
                # See docs/decisions.md 2026-08-04.
                try:
                    existing_size = sftp.stat(final_remote_path).st_size
                except OSError:
                    raise rename_exc  # destination doesn't exist either - unrelated failure
                if existing_size != local_size:
                    raise DeliveryCollisionError(
                        f"{final_remote_path} already exists with a different size "
                        f"({existing_size} bytes) than this upload ({local_size} bytes)"
                    ) from rename_exc
                log.info(
                    f"{final_remote_path} already exists with matching size "
                    f"({local_size} bytes) - treating as already delivered, not "
                    "re-uploading"
                )
                sftp.remove(temp_remote_path)
        finally:
            sftp.close()

```

tests/services/test_delivery_service.py:

```python
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import paramiko
import pytest

from app.core.config import settings
from app.services.delivery_service import (
    DeliveryAuthenticationError,
    DeliveryCollisionError,
    DeliveryConfigError,
    DeliveryError,
    DeliveryRemoteDirectoryNotFoundError,
    DeliveryService,
    DeliveryUnavailableError,
)


class FakeSFTPClient:
    """In-memory stand-in for paramiko.SFTPClient - same shape as the calls
    DeliveryService actually makes (put/stat/rename/remove/close)."""

    def __init__(
        self,
        fail_puts_before_success: int = 0,
        corrupt_upload_size: int | None = None,
        preexisting_remote_files: dict[str, int] | None = None,
        raise_missing_remote_directory: bool = False,
    ):
        self.remote_files: dict[str, int] = dict(preexisting_remote_files or {})
        self._fail_puts_before_success = fail_puts_before_success
        self._corrupt_upload_size = corrupt_upload_size
        self._raise_missing_remote_directory = raise_missing_remote_directory
        self.put_calls: list[str] = []
        self.remove_calls: list[str] = []
        self.closed = False

    def put(self, localpath: str, remotepath: str, confirm: bool = True):
        self.put_calls.append(remotepath)
        if self._raise_missing_remote_directory:
            raise OSError(2, "No such file or directory")
        if self._fail_puts_before_success > 0:
            self._fail_puts_before_success -= 1
            raise OSError("connection reset during upload")
        size = Path(localpath).stat().st_size
        self.remote_files[remotepath] = (
            self._corrupt_upload_size if self._corrupt_upload_size is not None else size
        )

    def stat(self, remotepath: str):
        return SimpleNamespace(st_size=self.remote_files[remotepath])

    def rename(self, oldpath: str, newpath: str):
        # Real SFTP servers (confirmed live against atmoz/sftp) refuse to rename
        # onto an existing destination - mirror that here instead of silently
        # overwriting, so the idempotent-redelivery handling has something real
        # to catch.
        if newpath in self.remote_files:
            raise OSError("Failure")
        self.remote_files[newpath] = self.remote_files.pop(oldpath)

    def remove(self, remotepath: str):
        self.remove_calls.append(remotepath)
        del self.remote_files[remotepath]

    def close(self):
        self.closed = True


@pytest.fixture
def local_file(tmp_path) -> Path:
    path = tmp_path / "output.csv"
    path.write_text("id,name\n1,Acme\n")
    return path


def test_successful_delivery_uploads_and_renames_to_final_path(local_file):
    fake = FakeSFTPClient()
    service = DeliveryService(client_factory=lambda: fake)

    result = service.deliver(local_file, remote_directory="/outbound/reports")

    assert result.remote_path == "/outbound/reports/output.csv"
    assert result.size_bytes == local_file.stat().st_size
    assert "/outbound/reports/output.csv" in fake.remote_files
    assert "/outbound/reports/output.csv.part" not in fake.remote_files
    assert fake.put_calls == ["/outbound/reports/output.csv.part"]


def test_custom_remote_filename_is_honored(local_file):
    fake = FakeSFTPClient()
    service = DeliveryService(client_factory=lambda: fake)

    result = service.deliver(
        local_file,
        remote_directory="/outbound/reports",
        remote_filename="CANDER_20260723.csv",
    )

    assert result.remote_path == "/outbound/reports/CANDER_20260723.csv"


def test_retries_then_succeeds_on_transient_failure(local_file):
    fake = FakeSFTPClient(fail_puts_before_success=1)
    factory_calls = []

    def factory():
        factory_calls.append(1)
        return fake

    service = DeliveryService(client_factory=factory)

    result = service.deliver(local_file, remote_directory="/outbound/reports")

    assert result.remote_path == "/outbound/reports/output.csv"
    assert len(factory_calls) == 2


def test_size_mismatch_after_upload_is_retried_then_raises(local_file):
    fake = FakeSFTPClient(corrupt_upload_size=999999)
    service = DeliveryService(client_factory=lambda: fake)

    with pytest.raises(DeliveryError):
        service.deliver(local_file, remote_directory="/outbound/reports")


def test_successful_delivery_result_includes_delivered_at_timestamp(local_file):
    fake = FakeSFTPClient()
    service = DeliveryService(client_factory=lambda: fake)

    before = datetime.now(timezone.utc)
    result = service.deliver(local_file, remote_directory="/outbound/reports")
    after = datetime.now(timezone.utc)

    assert before <= result.delivered_at <= after


def test_authentication_failure_raises_structured_error_and_is_not_retried(local_file):
    calls = []

    def factory():
        calls.append(1)
        raise paramiko.AuthenticationException("bad key")

    service = DeliveryService(client_factory=factory)

    with pytest.raises(DeliveryAuthenticationError) as exc_info:
        service.deliver(local_file, remote_directory="/outbound/reports")

    assert len(calls) == 1
    assert isinstance(exc_info.value.__cause__, paramiko.AuthenticationException)


def test_server_unavailable_after_retries_raises_structured_error(
    local_file, monkeypatch
):
    monkeypatch.setattr(settings, "SFTP_RETRY_COUNT", 2)
    calls = []

    def factory():
        calls.append(1)
        raise OSError("Connection refused")

    service = DeliveryService(client_factory=factory)

    with pytest.raises(DeliveryUnavailableError) as exc_info:
        service.deliver(local_file, remote_directory="/outbound/reports")

    assert len(calls) == 2
    assert isinstance(exc_info.value.__cause__, OSError)


def test_bad_host_key_is_not_wrapped_as_unavailable(local_file):
    """Host-key mismatch is a distinct, security-relevant concern from AC3/AC4 - kept as
    the raw paramiko type rather than folded into DeliveryUnavailableError."""
    calls = []

    fake_key = SimpleNamespace(get_base64=lambda: "fakekeybase64")

    def factory():
        calls.append(1)
        raise paramiko.BadHostKeyException("host", fake_key, fake_key)

    service = DeliveryService(client_factory=factory)

    with pytest.raises(paramiko.BadHostKeyException):
        service.deliver(local_file, remote_directory="/outbound/reports")

    assert len(calls) == 1


def test_missing_local_file_raises_without_connecting(tmp_path):
    calls = []

    def factory():
        calls.append(1)
        return FakeSFTPClient()

    service = DeliveryService(client_factory=factory)

    with pytest.raises(FileNotFoundError):
        service.deliver(
            tmp_path / "does_not_exist.csv", remote_directory="/outbound/reports"
        )

    assert calls == []


def test_missing_required_settings_raises_config_error(local_file, monkeypatch):
    monkeypatch.setattr(settings, "SFTP_HOST", None)
    service = DeliveryService()

    with pytest.raises(DeliveryConfigError):
        service.deliver(local_file, remote_directory="/outbound/reports")


def test_redelivery_with_matching_size_at_destination_succeeds_idempotently(local_file):
    local_size = local_file.stat().st_size
    fake = FakeSFTPClient(
        preexisting_remote_files={"/outbound/reports/output.csv": local_size}
    )
    service = DeliveryService(client_factory=lambda: fake)

    result = service.deliver(local_file, remote_directory="/outbound/reports")

    assert result.remote_path == "/outbound/reports/output.csv"
    assert result.size_bytes == local_size
    # temp .part file cleaned up via sftp.remove(), not left dangling
    assert "/outbound/reports/output.csv.part" not in fake.remote_files
    assert fake.remove_calls == ["/outbound/reports/output.csv.part"]


def test_redelivery_with_different_size_at_destination_raises_collision_error_not_retried(
    local_file,
):
    fake = FakeSFTPClient(
        preexisting_remote_files={"/outbound/reports/output.csv": 999999}
    )
    service = DeliveryService(client_factory=lambda: fake)

    with pytest.raises(DeliveryCollisionError):
        service.deliver(local_file, remote_directory="/outbound/reports")

    # not retried - exactly one put attempt, not settings.SFTP_RETRY_COUNT of them
    assert fake.put_calls == ["/outbound/reports/output.csv.part"]


def test_missing_known_hosts_file_raises_config_error(
    local_file, monkeypatch, tmp_path
):
    key_path = tmp_path / "id_rsa"
    key_path.write_text("fake key content")
    monkeypatch.setattr(settings, "SFTP_HOST", "lvappi01908.cloud.bns")
    monkeypatch.setattr(settings, "SFTP_USERNAME", "svc-account")
    monkeypatch.setattr(settings, "SFTP_PRIVATE_KEY_PATH", str(key_path))
    monkeypatch.setattr(settings, "SFTP_KNOWN_HOSTS_PATH", "does-not-exist-known-hosts")
    service = DeliveryService()

    with pytest.raises(DeliveryConfigError):
        service.deliver(local_file, remote_directory="/outbound/reports")


def test_missing_local_private_key_raises_config_error_and_is_not_retried(
    local_file, monkeypatch, tmp_path
):
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("")
    monkeypatch.setattr(settings, "SFTP_HOST", "lvappi01908.cloud.bns")
    monkeypatch.setattr(settings, "SFTP_USERNAME", "svc-account")
    monkeypatch.setattr(
        settings, "SFTP_PRIVATE_KEY_PATH", str(tmp_path / "does-not-exist")
    )
    monkeypatch.setattr(
        type(settings), "known_hosts_path", property(lambda self: known_hosts)
    )

    def _unexpected_ssh_client():
        raise AssertionError(
            "should never reach paramiko.SSHClient() - the private key check "
            "should have short-circuited first"
        )

    # No client_factory - exercises _connect()'s own body, same pattern as
    # test_connect_uses_legacy_global_settings_when_no_connection_given. Guard
    # against a real network attempt if the check fails to short-circuit.
    monkeypatch.setattr("paramiko.SSHClient", _unexpected_ssh_client)
    service = DeliveryService()

    with pytest.raises(DeliveryConfigError, match="does-not-exist"):
        service.deliver(local_file, remote_directory="/outbound/reports")


def test_remote_directory_missing_raises_specific_error_and_is_not_retried(local_file):
    fake = FakeSFTPClient(raise_missing_remote_directory=True)
    service = DeliveryService(client_factory=lambda: fake)

    with pytest.raises(DeliveryRemoteDirectoryNotFoundError, match="/outbound/reports"):
        service.deliver(local_file, remote_directory="/outbound/reports")

    # not retried - exactly one put attempt, not settings.SFTP_RETRY_COUNT of them
    assert fake.put_calls == ["/outbound/reports/output.csv.part"]


class _RecordingSSHClient:
    """Stands in for paramiko.SSHClient itself (not the SFTP client returned by
    open_sftp()) - used specifically to verify _connect()'s own connection-detail
    resolution logic, which the client_factory seam above bypasses entirely."""

    calls: list[dict] = []

    def __init__(self):
        self.loaded_host_keys = None
        self.missing_host_key_policy = None

    def load_host_keys(self, path):
        self.loaded_host_keys = path

    def set_missing_host_key_policy(self, policy):
        self.missing_host_key_policy = policy

    def connect(self, hostname, port, username, key_filename, timeout):
        _RecordingSSHClient.calls.append(
            {
                "hostname": hostname,
                "port": port,
                "username": username,
                "key_filename": key_filename,
            }
        )

    def open_sftp(self):
        return FakeSFTPClient()


def test_connect_uses_legacy_global_settings_when_no_connection_given(
    local_file, monkeypatch, tmp_path
):
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("")
    key_path = tmp_path / "legacy_key"
    key_path.write_text("fake key content")
    monkeypatch.setattr(settings, "SFTP_HOST", "legacy-host")
    monkeypatch.setattr(settings, "SFTP_PORT", 22)
    monkeypatch.setattr(settings, "SFTP_USERNAME", "legacy-user")
    monkeypatch.setattr(settings, "SFTP_PRIVATE_KEY_PATH", str(key_path))
    monkeypatch.setattr(
        type(settings), "known_hosts_path", property(lambda self: known_hosts)
    )
    monkeypatch.setattr("paramiko.SSHClient", _RecordingSSHClient)
    _RecordingSSHClient.calls = []

    service = DeliveryService()
    service.deliver(local_file, remote_directory="/outbound/reports")

    assert _RecordingSSHClient.calls == [
        {
            "hostname": "legacy-host",
            "port": 22,
            "username": "legacy-user",
            "key_filename": str(key_path),
        }
    ]


def test_connect_uses_passed_connection_details_when_given(
    local_file, monkeypatch, tmp_path
):
    from app.services.sftp_connection_service import SFTPConnectionSettings

    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("")
    key_path = tmp_path / "id_ed25519"
    key_path.write_text("fake key content")
    monkeypatch.setattr("paramiko.SSHClient", _RecordingSSHClient)
    _RecordingSSHClient.calls = []

    connection = SFTPConnectionSettings(
        host="lvappi01908.cloud.bns",
        port=2222,
        username="sftpuser",
        private_key_path=str(key_path),
        known_hosts_path=known_hosts,
        remote_folder="./ClientCentralDataFenergo",
    )
    service = DeliveryService()
    service.deliver(
        local_file, remote_directory="/outbound/reports", connection=connection
    )

    assert _RecordingSSHClient.calls == [
        {
            "hostname": "lvappi01908.cloud.bns",
            "port": 2222,
            "username": "sftpuser",
            "key_filename": str(key_path),
        }
    ]

```
