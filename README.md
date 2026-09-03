# sample_study_notes_14
KYC Stuides

About the app:

Simple answer: Application-local, embedded runtime — not a globally-installed Python. In production/Docker, the exact Python interpreter (3.12.3) ships baked into the container image itself (FROM python:3.12.3-slim), self-contained and isolated from whatever Python (if any) exists on the host machine or Kubernetes node. Locally, it uses a per-project virtual environment (.venv), which similarly isolates all dependencies from any system-wide Python install.

Is this better? Yes, and it's the standard/expected practice, not just a preference — for concrete reasons a technical interviewer would want to hear:

Reproducibility — the exact interpreter version and every dependency's exact version are pinned per project, so it behaves identically on any machine, instead of drifting based on whatever's globally installed.
Isolation — no version conflicts with other Python tools/apps on the same machine.
Portability — the same container runs the same way on a laptop, in CI, or on a production K8s cluster, since nothing depends on the host's own Python.
Safety — global Python installs are often relied on by the OS itself; installing/upgrading packages into it risks breaking unrelated system tooling. Isolating avoids that entirely.
Relying on a globally-installed Python directly is generally considered a legacy/fragile pattern outside of quick personal scripts — worth stating plainly if asked, not hedging.


## dag

```python
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

REPORT_NAME = "ChinaGTTReport"  # <- change this per report


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def run_report_full():
    # DO NOT set retries= on this task. `run-report` submits a NEW request to
    # Fenergo every time it runs - if Airflow retries it after a failure, it may
    # resubmit even though the first attempt already succeeded (a real duplicate
    # Fenergo submission, not just a duplicate log line). No idempotency guard
    # exists for this yet - see docs/architecture.md Sec 4.
    #
    # To retry a failed run safely, use airflow_dag_stage_level.py's
    # poll/download/transform tasks instead (each resumable via execution_id),
    # or fix it manually:
    #   python -m app.cli poll <execution_id>
    #   python -m app.cli download <execution_id> <presigned_url>
    #   python -m app.cli transform <execution_id>
    BashOperator(
        task_id="run_report",
        bash_command=f"python -m app.cli run-report {REPORT_NAME}",
    )


run_report_full()
```

## Jira

Title: Register finalized GTT reports (Product, UK Product) and rename China/CANDER assets

Description:
As the platform, I need the five finalized GTT onboarding-status reports registered so they can be run via run-report.

Renamed China_GTT_Report → China_OnboardingStatus_YYYYMMDD and CANDER_Report_ExtractTemplate → CanadianDerivatives_OnboardingStatus_YYYYMMDD (SQL + template assets), keeping existing ChinaGTTReport/CANDERReport CLI names unchanged.
Registered two new reports: ProductReport (GTT/Product) and UKProductReport (GTT/UKProduct), with real column templates for UK Product (Product's headers not yet available).
Added real column templates (Source=Target) for China and Canadian Derivatives from their actual Fenergo CSV exports.
Standardized output/marker filenames to date-only (YYYYMMDD), replacing the previous timestamp format, across all reports.

## Changes

app/services/reporting_orchestrator.py:

```python
#
    @staticmethod
    def _timestamp() -> str:
        """Date-only (YYYYMMDD), matching the finalized reports' naming standard"""
        return datetime.now(timezone.utc).strftime("%Y%m%d")

```

app/services/report_definition_service.py:

```python
from app.core.config import settings
from app.services.fenergo_service import ReportSource
from report_definitions import (
    cander_report,
    china_gtt_report,
    product_report,
    singapore_report,
    uk_product_report,
)
# ...

        for module in (
            cander_report,
            china_gtt_report,
            product_report,
            singapore_report,
            uk_product_report,
        )
```

new report_definitions/cander_report.py:

```python
REPORT_NAME = "CANDERReport"
SQL_FILE = "CanadianDerivatives_OnboardingStatus_YYYYMMDD.sql"
TEMPLATE_FILE = "CanadianDerivatives_OnboardingStatus.xml"
ARCHIVE_PATH = "GTT/Cander"
SFTP_CONNECTION = "ClientCentralData"

```

report_definitions/china_gtt_report.py:

```python
REPORT_NAME = "ChinaGTTReport"
# Renamed 2026-09-01 to match the finalized report name saved on the Fenergo platform
# itself - "YYYYMMDD" is literal text in the filename, not a real date (see
# docs/decisions.md). Starts empty - real SQL not yet provided.
SQL_FILE = "China_OnboardingStatus_YYYYMMDD.sql"
TEMPLATE_FILE = "China_OnboardingStatus.xml"
ARCHIVE_PATH = "GTT/China"
SFTP_CONNECTION = "RegCentral"

```

report_definitions/product_report.py:

```python
REPORT_NAME = "ProductReport"
# "YYYYMMDD" is literal text in the filename, matching the report name saved on the
# Fenergo platform itself (see docs/decisions.md). Empty - real SQL not yet provided.
SQL_FILE = "Product_OnboardingStatus_YYYYMMDD.sql"
# Template has zero columns - real CSV column headers for this report don't exist yet
# (see docs/decisions.md 2026-09-01).
TEMPLATE_FILE = "Product_OnboardingStatus.xml"
ARCHIVE_PATH = "GTT/Product"
# Assumed - only one real connection exists as of 2026-08-24 (docs/decisions.md).
# Confirm/correct once this report has real executable SQL.
SFTP_CONNECTION = "ClientCentralData"

```

report_definitions/uk_product_report.py:

```python
REPORT_NAME = "UKProductReport"
# "YYYYMMDD" is literal text in the filename, matching the report name saved on the
# Fenergo platform itself (see docs/decisions.md). Empty - real SQL not yet provided.
SQL_FILE = "UK_Product_OnboardingStatus_YYYYMMDD.sql"
TEMPLATE_FILE = "UK_Product_OnboardingStatus.xml"
ARCHIVE_PATH = "GTT/UKProduct"
# Assumed - only one real connection exists as of 2026-08-24 (docs/decisions.md).
# Confirm/correct once this report has real executable SQL.
SFTP_CONNECTION = "ClientCentralData"

```

tests/services/test_report_definition_service.py:

```python
import pytest

from app.core.config import Settings, settings
from app.services.fenergo_service import ReportSource
from app.services.report_definition_service import (
    ReportDefinition,
    ReportDefinitionService,
)


def test_get_returns_known_report_definition():
    definition = ReportDefinitionService.get("CANDERReport")
    assert definition == ReportDefinition(
        sql_file="CanadianDerivatives_OnboardingStatus_YYYYMMDD.sql",
        template_file="CanadianDerivatives_OnboardingStatus.xml",
        archive_path="GTT/Cander",
        sftp_connection="ClientCentralData",
    )
    assert definition.generate_marker is True


def test_generate_marker_defaults_to_true_when_omitted():
    definition = ReportDefinition(
        template_file="x.xml", sql_file="x.sql", archive_path="Test/Path"
    )
    assert definition.generate_marker is True


def test_generate_marker_can_be_explicitly_disabled():
    definition = ReportDefinition(
        template_file="x.xml",
        sql_file="x.sql",
        archive_path="Test/Path",
        generate_marker=False,
    )
    assert definition.generate_marker is False


def test_get_raises_for_unknown_report_name():
    with pytest.raises(KeyError):
        ReportDefinitionService.get("DoesNotExist")


def test_get_returns_china_gtt_report_definition():
    definition = ReportDefinitionService.get("ChinaGTTReport")
    assert definition == ReportDefinition(
        sql_file="China_OnboardingStatus_YYYYMMDD.sql",
        template_file="China_OnboardingStatus.xml",
        archive_path="GTT/China",
        sftp_connection="RegCentral",
    )


def test_get_returns_product_report_definition():
    definition = ReportDefinitionService.get("ProductReport")
    assert definition == ReportDefinition(
        sql_file="Product_OnboardingStatus_YYYYMMDD.sql",
        template_file="Product_OnboardingStatus.xml",
        archive_path="GTT/Product",
        sftp_connection="ClientCentralData",
    )


def test_get_returns_uk_product_report_definition():
    definition = ReportDefinitionService.get("UKProductReport")
    assert definition == ReportDefinition(
        sql_file="UK_Product_OnboardingStatus_YYYYMMDD.sql",
        template_file="UK_Product_OnboardingStatus.xml",
        archive_path="GTT/UKProduct",
        sftp_connection="ClientCentralData",
    )


def test_archive_path_is_required():
    with pytest.raises(TypeError):
        ReportDefinition(template_file="x.xml", sql_file="x.sql")


def test_sftp_connection_defaults_to_none_when_omitted():
    definition = ReportDefinition(
        template_file="x.xml", sql_file="x.sql", archive_path="Test/Path"
    )
    assert definition.sftp_connection is None


def test_sql_and_template_file_paths_resolve_under_settings_paths():
    sql_path = ReportDefinitionService.sql_file_path("CANDERReport")
    template_path = ReportDefinitionService.template_file_path("CANDERReport")

    assert sql_path.name == "CanadianDerivatives_OnboardingStatus_YYYYMMDD.sql"
    assert sql_path.parent == settings.sql_query_path
    assert template_path.name == "CanadianDerivatives_OnboardingStatus.xml"
    assert template_path.parent == settings.template_path


def test_extract_templates_removed_from_settings():
    """Hard rule (CLAUDE.md #5): the registry belongs in report_definitions/ files,
    not hardcoded inside Settings. Settings is for env/secrets only."""
    assert "EXTRACT_TEMPLATES" not in Settings.model_fields


def test_report_definition_rejects_both_sql_file_and_saved_query_id():
    with pytest.raises(ValueError):
        ReportDefinition(
            template_file="x.xml",
            sql_file="x.sql",
            saved_query_id="abc-123",
            archive_path="Test/Path",
        )


def test_report_definition_rejects_neither_sql_file_nor_saved_query_id():
    with pytest.raises(ValueError):
        ReportDefinition(template_file="x.xml", archive_path="Test/Path")


def test_report_source_reads_sql_file_text_for_sql_file_based_report():
    source = ReportDefinitionService.report_source("ChinaGTTReport")

    assert source == ReportSource(
        sql_query=ReportDefinitionService.sql_file_path("ChinaGTTReport").read_text()
    )


def test_report_source_wraps_saved_query_id_for_saved_query_based_report(monkeypatch):
    saved_query_definition = ReportDefinition(
        template_file="China_GTT_Report_ExtractTemplate.xml",
        saved_query_id="saved-query-abc-123",
        archive_path="GTT/China",
    )
    monkeypatch.setitem(
        ReportDefinitionService._registry,
        "SavedQueryTestReport",
        saved_query_definition,
    )

    source = ReportDefinitionService.report_source("SavedQueryTestReport")

    assert source == ReportSource(saved_query_id="saved-query-abc-123")


def test_sql_file_path_raises_clearly_for_saved_query_based_report(monkeypatch):
    saved_query_definition = ReportDefinition(
        template_file="China_GTT_Report_ExtractTemplate.xml",
        saved_query_id="saved-query-abc-123",
        archive_path="GTT/China",
    )
    monkeypatch.setitem(
        ReportDefinitionService._registry,
        "SavedQueryTestReport",
        saved_query_definition,
    )

    with pytest.raises(ValueError):
        ReportDefinitionService.sql_file_path("SavedQueryTestReport")

```


tests/services/test_reporting_orchestrator.py:

```python
import dataclasses
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.config import settings
from app.models.execution import ExecutionStatus
from app.services import execution_service
from app.services.delivery_service import (
    DeliveryConfigError,
    DeliveryError,
    DeliveryResult,
)
from app.services.download_service import DownloadResult
from app.services.fenergo_service import StatusResult, SubmitResult
from app.services.report_definition_service import ReportDefinitionService
from app.services.reporting_orchestrator import (
    OrchestrationError,
    ReportingOrchestrator,
    UtilityOperationResult,
)

CHINA_GTT_CSV = (
    "Fenergo ID,Client Name,LEI,Global Risk Rating,Scheduled Review Date,"
    "China Risk Rating,China Risk Rating (Override),China Next Review Date,"
    "China Next Review Date (Override),China Comments,Country of Incorporation,"
    "Product Category,Product Type,Booking Entity,Arranging Entity,Product ID\n"
    "1,Acme Corp,LEI123,High,2026-01-01,Medium,,2026-06-01,,,China,FamilyA,"
    "TypeA,EntityA,EntityB,PROD-1\n"
)


class FakeFenergoService:
    def __init__(
        self, status="Completed", presigned_url="https://example.test/report.csv"
    ):
        self.status = status
        self.presigned_url = presigned_url
        self.submit_calls = []

    async def submit(self, source, description):
        self.submit_calls.append((source, description))
        return SubmitResult(report_id="FEN-123")

    async def check_status(self, report_id):
        return StatusResult(status=self.status, presigned_url=self.presigned_url)


class FakeDownloadService:
    def __init__(self, csv_content=CHINA_GTT_CSV):
        self._csv_content = csv_content
        self.download_calls = []

    async def download(self, presigned_url, destination_filename):
        self.download_calls.append((presigned_url, destination_filename))
        path = settings.download_path / destination_filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self._csv_content)
        return DownloadResult(local_path=path, size_bytes=len(self._csv_content))


class FailingDownloadService:
    async def download(self, presigned_url, destination_filename):
        raise RuntimeError("simulated download failure")


class FakeDeliveryService:
    def __init__(self, fail: bool = False):
        self.deliver_calls = []
        self._fail = fail

    def deliver(
        self, local_path, remote_directory, remote_filename=None, connection=None
    ):
        self.deliver_calls.append((local_path, remote_directory, connection))
        if self._fail:
            raise DeliveryError("simulated delivery failure")
        return DeliveryResult(
            remote_path=f"{remote_directory}/{local_path.name}",
            size_bytes=local_path.stat().st_size,
            delivered_at=datetime.now(timezone.utc),
        )


@pytest.fixture(autouse=True)
def _isolated_download_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DOWNLOAD_FOLDER", str(tmp_path / "downloads"))
    monkeypatch.setattr(settings, "SFTP_SUCCESS_DIRECTORY", "success")
    monkeypatch.setattr(settings, "AUDIT_ROOT", None)
    # ChinaGTTReport declares sftp_connection="RegCentral" - required env vars for
    # SFTPConnectionService.get() to resolve without raising.
    monkeypatch.setenv("SFTP_REGCENTRAL_HOST", "test-host")
    monkeypatch.setenv("SFTP_REGCENTRAL_USERNAME", "test-user")
    monkeypatch.setenv("SFTP_REGCENTRAL_PRIVATE_KEY_PATH", "/test/key")
    monkeypatch.setenv("SFTP_REGCENTRAL_REMOTE_FOLDER", "./RegCentralTest")


def _seed_full_run_execution(report_name="ChinaGTTReport", downloaded_file_path=None):
    """Seeds a real FULL_RUN execution the way submit->download would leave it -
    the shape every linked utility-flow test needs to attach to."""
    execution = execution_service.create_execution(report_name)
    execution_service.mark_submitted(execution.execution_id, "FEN-123")
    if downloaded_file_path is not None:
        execution_service.mark_downloaded(
            execution.execution_id, str(downloaded_file_path)
        )
    return execution


async def test_run_report_full_success_sequences_all_steps_and_completes(db):
    fenergo = FakeFenergoService()
    download = FakeDownloadService()
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(
        fenergo_service=fenergo, download_service=download, delivery_service=delivery
    )

    execution = await orchestrator.run_report("ChinaGTTReport")

    assert execution.status == ExecutionStatus.SFTP_COMPLETED.value
    assert execution.fenergo_report_id == "FEN-123"
    assert execution.output_file_path is not None

    output_path = Path(execution.output_file_path)
    assert output_path.exists()
    content = output_path.read_text()
    assert "Fenergo ID" in content
    assert "TypeA" in content

    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert marker_path.exists()

    assert (
        marker_path.read_text().strip()
        == hashlib.sha256(output_path.read_bytes()).hexdigest()
    )

    assert len(delivery.deliver_calls) == 2
    delivered_paths = {call[0] for call in delivery.deliver_calls}
    assert delivered_paths == {output_path, marker_path}
    # ChinaGTTReport declares sftp_connection="RegCentral" - delivery goes to
    # that connection's own remote_folder, not the legacy global SFTP_SUCCESS_DIRECTORY.
    assert all(call[1] == "./RegCentralTest" for call in delivery.deliver_calls)
    assert all(
        call[2] is not None and call[2].host == "test-host"
        for call in delivery.deliver_calls
    )


async def test_run_report_output_lands_under_reports_archive_path_locally(db):
    """No AUDIT_ROOT set: settings.download_path/<archive_path>/, no Archive/Failed
    split - matches the user's own description of local behavior."""
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    execution = await orchestrator.run_report("ChinaGTTReport")

    output_path = Path(execution.output_file_path)
    assert output_path.parent == settings.download_path / "GTT" / "China"


async def test_run_report_output_lands_under_audit_root_archive_when_set(
    db, monkeypatch, tmp_path
):
    audit_root = tmp_path / "Audit"
    monkeypatch.setattr(settings, "AUDIT_ROOT", str(audit_root))
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    execution = await orchestrator.run_report("ChinaGTTReport")

    output_path = Path(execution.output_file_path)
    assert output_path.parent == audit_root / "Archive" / "GTT" / "China"


async def test_run_report_moves_output_to_failed_when_delivery_fails_and_audit_root_set(
    db, monkeypatch, tmp_path
):
    audit_root = tmp_path / "Audit"
    monkeypatch.setattr(settings, "AUDIT_ROOT", str(audit_root))
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(fail=True),
    )

    with pytest.raises(DeliveryError):
        await orchestrator.run_report("ChinaGTTReport")

    archive_dir = audit_root / "Archive" / "GTT" / "China"
    failed_dir = audit_root / "Failed" / "GTT" / "China"
    assert list(archive_dir.glob("*.csv")) == []
    failed_files = list(failed_dir.glob("*.csv"))
    assert len(failed_files) == 1
    assert "Fenergo ID" in failed_files[0].read_text()


async def test_run_report_leaves_output_in_place_when_delivery_fails_and_no_audit_root(
    db,
):
    """Locally (no AUDIT_ROOT), Archive/Failed are the same directory - moving is a
    harmless no-op, not an error."""
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(fail=True),
    )

    with pytest.raises(DeliveryError):
        await orchestrator.run_report("ChinaGTTReport")

    output_dir = settings.download_path / "GTT" / "China"
    files = list(output_dir.glob("*.csv"))
    assert len(files) == 1
    assert "Fenergo ID" in files[0].read_text()


async def test_run_report_full_success_records_seven_file_processing_rows_one_per_artifact(
    db,
):
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    execution = await orchestrator.run_report("ChinaGTTReport")

    records = execution_service.list_file_processing(execution.execution_id)
    steps = [r.processing_step for r in records]
    assert steps == [
        "SUBMIT",
        "POLL",
        "DOWNLOAD",
        "TRANSFORM",
        "MARKER",
        "DELIVER",
        "DELIVER",
    ]
    assert all(r.status == "COMPLETED" for r in records)

    marker_record = records[4]
    output_path = Path(execution.output_file_path)
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert marker_record.checksum_value == marker_path.read_text().strip()

    deliver_file_names = {
        r.file_name for r in records if r.processing_step == "DELIVER"
    }
    assert deliver_file_names == {output_path.name, marker_path.name}


async def test_run_report_skips_marker_records_five_file_processing_rows(
    db, monkeypatch
):
    definition = ReportDefinitionService.get("ChinaGTTReport")
    monkeypatch.setitem(
        ReportDefinitionService._registry,
        "ChinaGTTReport",
        dataclasses.replace(definition, generate_marker=False),
    )
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    execution = await orchestrator.run_report("ChinaGTTReport")

    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == [
        "SUBMIT",
        "POLL",
        "DOWNLOAD",
        "TRANSFORM",
        "DELIVER",
    ]
    assert all(r.status == "COMPLETED" for r in records)


async def test_run_report_marks_failed_when_polling_reports_failed(db):
    fenergo = FakeFenergoService(status="Failed")
    orchestrator = ReportingOrchestrator(
        fenergo_service=fenergo,
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    with pytest.raises(OrchestrationError):
        await orchestrator.run_report("ChinaGTTReport")

    executions = [e for e in _all_executions() if e.report_name == "ChinaGTTReport"]
    execution = executions[-1]
    assert execution.status == ExecutionStatus.FAILED.value
    assert execution.failure_stage == "POLL"


async def test_run_report_marks_failed_when_download_raises(db):
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FailingDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    with pytest.raises(RuntimeError):
        await orchestrator.run_report("ChinaGTTReport")

    execution = _all_executions()[-1]
    assert execution.status == ExecutionStatus.FAILED.value
    assert execution.failure_stage == "DOWNLOAD"
    assert "simulated download failure" in execution.error_message

    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == ["SUBMIT", "POLL", "DOWNLOAD"]
    assert records[-1].status == "FAILED"


async def test_run_report_marks_failed_when_marker_generation_raises(db):
    class FailingMarkerService:
        @staticmethod
        def create_marker_for_file(path):
            raise OSError("simulated marker generation failure")

    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=delivery,
        marker_service=FailingMarkerService,
    )

    with pytest.raises(OSError):
        await orchestrator.run_report("ChinaGTTReport")

    assert delivery.deliver_calls == []
    execution = _all_executions()[-1]
    assert execution.status == ExecutionStatus.FAILED.value
    assert execution.failure_stage == "MARKER"

    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == [
        "SUBMIT",
        "POLL",
        "DOWNLOAD",
        "TRANSFORM",
        "MARKER",
    ]
    assert records[-1].status == "FAILED"
    assert "simulated marker generation failure" in records[-1].error_message


async def test_run_report_raises_delivery_config_error_when_success_directory_unset(
    db, monkeypatch
):
    # ChinaGTTReport now delivers via sftp_connection="RegCentral" (see
    # SFTPConnectionService), not the legacy global SFTP_SUCCESS_DIRECTORY - the
    # connection's own required env var is what needs to be missing to reproduce
    # this config error for this report now.
    monkeypatch.delenv("SFTP_REGCENTRAL_HOST", raising=False)
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=delivery,
    )

    with pytest.raises(DeliveryConfigError):
        await orchestrator.run_report("ChinaGTTReport")

    assert delivery.deliver_calls == []
    execution = _all_executions()[-1]
    assert execution.status == ExecutionStatus.FAILED.value
    assert execution.failure_stage == "DELIVER"

    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == [
        "SUBMIT",
        "POLL",
        "DOWNLOAD",
        "TRANSFORM",
        "MARKER",
        "DELIVER",
    ]
    assert [r.status for r in records] == [
        "COMPLETED",
        "COMPLETED",
        "COMPLETED",
        "COMPLETED",
        "COMPLETED",
        "FAILED",
    ]


async def test_run_report_explicit_true_override_forces_marker_over_false_default(
    db, monkeypatch
):
    definition = ReportDefinitionService.get("ChinaGTTReport")
    monkeypatch.setitem(
        ReportDefinitionService._registry,
        "ChinaGTTReport",
        dataclasses.replace(definition, generate_marker=False),
    )
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=delivery,
    )

    execution = await orchestrator.run_report("ChinaGTTReport", generate_marker=True)

    output_path = Path(execution.output_file_path)
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert marker_path.exists()
    assert len(delivery.deliver_calls) == 2


async def test_run_report_explicit_false_override_suppresses_marker_over_true_default(
    db,
):
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=delivery,
    )

    execution = await orchestrator.run_report("ChinaGTTReport", generate_marker=False)

    output_path = Path(execution.output_file_path)
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert not marker_path.exists()
    assert len(delivery.deliver_calls) == 1


async def test_run_report_no_sftp_no_output_path_skips_delivery_stays_transformed(db):
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=delivery,
    )

    execution = await orchestrator.run_report("ChinaGTTReport", sftp=False)

    assert execution.status == ExecutionStatus.TRANSFORMED.value
    assert delivery.deliver_calls == []
    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == [
        "SUBMIT",
        "POLL",
        "DOWNLOAD",
        "TRANSFORM",
        "MARKER",
    ]


async def test_run_report_output_path_copies_locally_and_completes(db, tmp_path):
    destination = tmp_path / "local_drop"
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    execution = await orchestrator.run_report(
        "ChinaGTTReport", local_destination=destination
    )

    assert execution.status == ExecutionStatus.SFTP_COMPLETED.value
    output_path = Path(execution.output_file_path)
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert (destination / output_path.name).exists()
    assert (destination / marker_path.name).exists()
    assert (destination / output_path.name).read_bytes() == output_path.read_bytes()

    records = execution_service.list_file_processing(execution.execution_id)
    deliver_records = [r for r in records if r.processing_step == "DELIVER"]
    assert len(deliver_records) == 2
    assert {r.output_file_path for r in deliver_records} == {
        str(destination / output_path.name),
        str(destination / marker_path.name),
    }


async def test_run_report_sftp_true_and_output_path_together_raises(db, tmp_path):
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    with pytest.raises(ValueError):
        await orchestrator.run_report(
            "ChinaGTTReport", sftp=True, local_destination=tmp_path / "out"
        )


async def test_submit_report_success_creates_and_marks_submitted(db):
    orchestrator = ReportingOrchestrator(fenergo_service=FakeFenergoService())

    execution = await orchestrator.submit_report("ChinaGTTReport")

    assert execution.status == ExecutionStatus.REQUEST_SUBMITTED.value
    assert execution.report_name == "ChinaGTTReport"
    assert execution.fenergo_report_id == "FEN-123"

    records = execution_service.list_file_processing(execution.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "SUBMIT"
    assert records[0].status == "COMPLETED"


async def test_submit_report_marks_failed_when_fenergo_raises(db):
    class FailingFenergoService:
        async def submit(self, source, description):
            raise RuntimeError("simulated submit failure")

    orchestrator = ReportingOrchestrator(fenergo_service=FailingFenergoService())

    with pytest.raises(RuntimeError):
        await orchestrator.submit_report("ChinaGTTReport")

    execution = _all_executions()[-1]
    assert execution.status == ExecutionStatus.FAILED.value
    assert execution.failure_stage == "SUBMIT"

    records = execution_service.list_file_processing(execution.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "SUBMIT"
    assert records[0].status == "FAILED"


async def test_poll_execution_success_returns_presigned_url_and_marks_url_received(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(fenergo_service=FakeFenergoService())

    poll_result = await orchestrator.poll_execution(created.execution_id)

    assert poll_result.status == "Completed"
    assert poll_result.presigned_url == "https://example.test/report.csv"
    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.REPORT_READY.value

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "POLL"
    assert records[0].status == "COMPLETED"


async def test_poll_execution_marks_failed_when_status_is_failed(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(status="Failed")
    )

    with pytest.raises(OrchestrationError):
        await orchestrator.poll_execution(created.execution_id)

    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.FAILED.value
    assert updated.failure_stage == "POLL"

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "POLL"
    assert records[0].status == "FAILED"


async def test_poll_execution_raises_lookuperror_directly_for_unknown_execution_id(db):
    orchestrator = ReportingOrchestrator(fenergo_service=FakeFenergoService())

    with pytest.raises(LookupError):
        await orchestrator.poll_execution("does-not-exist")


async def test_poll_execution_once_true_completes_immediately_without_looping(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(fenergo_service=FakeFenergoService())

    poll_result = await orchestrator.poll_execution(created.execution_id, once=True)

    assert poll_result.status == "Completed"
    assert poll_result.presigned_url == "https://example.test/report.csv"
    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.REPORT_READY.value
    assert updated.poll_attempts == 1

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "POLL"


async def test_poll_execution_once_true_marks_failed_when_status_is_failed(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(status="Failed")
    )

    with pytest.raises(OrchestrationError):
        await orchestrator.poll_execution(created.execution_id, once=True)

    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.FAILED.value
    assert updated.failure_stage == "POLL"

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].status == "FAILED"


async def test_poll_execution_once_true_returns_pending_without_raising_or_marking_failed(
    db,
):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(status="Pending")
    )

    poll_result = await orchestrator.poll_execution(created.execution_id, once=True)

    assert poll_result.status == "Pending"
    updated = execution_service.get_execution(created.execution_id)
    # increment_poll_attempt() sets status=POLLING as a side effect - correct,
    # not FAILED, which is the actual thing being asserted here.
    assert updated.status == ExecutionStatus.POLLING.value
    assert updated.failure_stage is None
    assert updated.poll_attempts == 1

    # "still pending" writes no file_processing row at all - not an event worth
    # recording, same treatment as "not a failure".
    assert execution_service.list_file_processing(created.execution_id) == []


async def test_poll_execution_once_true_increments_poll_attempts_across_calls(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(status="Pending")
    )

    await orchestrator.poll_execution(created.execution_id, once=True)
    await orchestrator.poll_execution(created.execution_id, once=True)

    updated = execution_service.get_execution(created.execution_id)
    assert updated.poll_attempts == 2


async def test_poll_execution_marks_failed_when_no_fenergo_report_id(db):
    created = execution_service.create_execution("ChinaGTTReport")
    orchestrator = ReportingOrchestrator(fenergo_service=FakeFenergoService())

    with pytest.raises(ValueError):
        await orchestrator.poll_execution(created.execution_id)

    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.FAILED.value
    assert updated.failure_stage == "POLL"


def test_poll_execution_no_fenergo_report_id_does_not_record_file_processing(db):
    import asyncio

    created = execution_service.create_execution("ChinaGTTReport")
    orchestrator = ReportingOrchestrator(fenergo_service=FakeFenergoService())

    with pytest.raises(ValueError):
        asyncio.run(orchestrator.poll_execution(created.execution_id))

    assert execution_service.list_file_processing(created.execution_id) == []


async def test_download_execution_success(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    download = FakeDownloadService()
    orchestrator = ReportingOrchestrator(download_service=download)

    execution = await orchestrator.download_execution(
        created.execution_id, "https://example.test/report.csv"
    )

    assert execution.status == ExecutionStatus.DOWNLOADED.value
    assert execution.downloaded_file_path is not None
    assert download.download_calls == [
        ("https://example.test/report.csv", "FEN-123.csv")
    ]

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "DOWNLOAD"
    assert records[0].status == "COMPLETED"


async def test_download_execution_marks_failed_when_download_raises(db):
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_submitted(created.execution_id, "FEN-123")
    orchestrator = ReportingOrchestrator(download_service=FailingDownloadService())

    with pytest.raises(RuntimeError):
        await orchestrator.download_execution(created.execution_id, "https://x/y.csv")

    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.FAILED.value
    assert updated.failure_stage == "DOWNLOAD"

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].status == "FAILED"


async def test_download_execution_no_fenergo_report_id_does_not_record_file_processing(
    db,
):
    created = execution_service.create_execution("ChinaGTTReport")
    orchestrator = ReportingOrchestrator(download_service=FakeDownloadService())

    with pytest.raises(ValueError):
        await orchestrator.download_execution(created.execution_id, "https://x/y.csv")

    assert execution_service.list_file_processing(created.execution_id) == []


async def test_transform_execution_success(db, tmp_path):
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    created = execution_service.create_execution("ChinaGTTReport")
    execution_service.mark_downloaded(created.execution_id, str(input_path))
    orchestrator = ReportingOrchestrator()

    execution = await orchestrator.transform_execution(created.execution_id)

    assert execution.status == ExecutionStatus.TRANSFORMED.value
    output_path = Path(execution.output_file_path)
    assert output_path.exists()
    assert "Fenergo ID" in output_path.read_text()

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "TRANSFORM"
    assert records[0].status == "COMPLETED"
    assert records[0].output_file_path == str(output_path)


async def test_transform_execution_marks_failed_when_no_downloaded_file_path(db):
    created = execution_service.create_execution("ChinaGTTReport")
    orchestrator = ReportingOrchestrator()

    with pytest.raises(ValueError):
        await orchestrator.transform_execution(created.execution_id)

    updated = execution_service.get_execution(created.execution_id)
    assert updated.status == ExecutionStatus.FAILED.value
    assert updated.failure_stage == "TRANSFORM"

    records = execution_service.list_file_processing(created.execution_id)
    assert len(records) == 1
    assert records[0].status == "FAILED"


# --- Utility flows: deliver_file (Flow: raw SFTP delivery, no report) ---


def test_deliver_file_unlinked_returns_untracked_result_and_writes_no_db_rows(
    db, tmp_path
):
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = orchestrator.deliver_file(source_file)

    assert isinstance(result, UtilityOperationResult)
    assert result.execution_id is None
    assert result.output_path == "success/arbitrary.csv"
    assert len(delivery.deliver_calls) == 1
    assert _all_executions() == []


def test_deliver_file_linked_writes_deliver_file_processing_row(db, tmp_path):
    execution = _seed_full_run_execution()
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = orchestrator.deliver_file(source_file, execution_id=execution.execution_id)

    assert result.execution_id == execution.execution_id
    assert result.output_path == "success/arbitrary.csv"
    records = execution_service.list_file_processing(execution.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "DELIVER"
    assert records[0].status == "COMPLETED"
    assert records[0].file_name == "arbitrary.csv"
    # The linked execution's own report_execution row is untouched.
    unchanged = execution_service.get_execution(execution.execution_id)
    assert unchanged.status == ExecutionStatus.REQUEST_SUBMITTED.value


def test_deliver_file_to_error_target(db, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "SFTP_ERROR_DIRECTORY", "error")
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    orchestrator.deliver_file(source_file, target="error")

    assert delivery.deliver_calls[0][1] == "error"


def test_deliver_file_linked_marks_failed_file_processing_on_error(
    db, tmp_path, monkeypatch
):
    execution = _seed_full_run_execution()
    monkeypatch.setattr(settings, "SFTP_ERROR_DIRECTORY", None)
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(DeliveryConfigError):
        orchestrator.deliver_file(
            source_file, target="error", execution_id=execution.execution_id
        )

    records = execution_service.list_file_processing(execution.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "DELIVER"
    assert records[0].status == "FAILED"


def test_deliver_file_unlinked_writes_no_db_rows_on_error(db, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "SFTP_ERROR_DIRECTORY", None)
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(DeliveryConfigError):
        orchestrator.deliver_file(source_file, target="error")

    assert _all_executions() == []


def test_deliver_file_raises_for_unknown_target(db, tmp_path):
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(ValueError):
        orchestrator.deliver_file(source_file, target="nonsense")


def test_deliver_file_raises_lookuperror_for_unknown_execution_id(db, tmp_path):
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("a,b\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(LookupError):
        orchestrator.deliver_file(source_file, execution_id="does-not-exist")


# --- Utility flows: generate_marker_for_file (Flow 3) ---


def test_generate_marker_for_file_unlinked_returns_untracked_result_and_writes_no_db_rows(
    db, tmp_path
):
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = orchestrator.generate_marker_for_file(source_file)

    assert isinstance(result, UtilityOperationResult)
    assert result.execution_id is None
    marker_path = source_file.with_suffix(source_file.suffix + ".mrk")
    assert result.output_path == str(marker_path)
    assert (
        marker_path.read_text().strip()
        == hashlib.sha256(source_file.read_bytes()).hexdigest()
    )
    assert len(delivery.deliver_calls) == 1
    assert _all_executions() == []


def test_generate_marker_for_file_linked_writes_marker_and_deliver_file_processing_rows(
    db, tmp_path
):
    execution = _seed_full_run_execution()
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = orchestrator.generate_marker_for_file(
        source_file, execution_id=execution.execution_id
    )

    assert result.execution_id == execution.execution_id
    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == ["MARKER", "DELIVER"]
    assert all(r.status == "COMPLETED" for r in records)
    marker_path = source_file.with_suffix(source_file.suffix + ".mrk")
    assert records[0].checksum_value == marker_path.read_text().strip()


def test_generate_marker_for_file_no_sftp_no_output_path_skips_delivery(db, tmp_path):
    execution = _seed_full_run_execution()
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = orchestrator.generate_marker_for_file(
        source_file, execution_id=execution.execution_id, sftp=False
    )

    assert delivery.deliver_calls == []
    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == ["MARKER"]
    marker_path = source_file.with_suffix(source_file.suffix + ".mrk")
    assert result.output_path == str(marker_path)


def test_generate_marker_for_file_output_path_copies_locally(db, tmp_path):
    execution = _seed_full_run_execution()
    destination = tmp_path / "local_drop"
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    orchestrator.generate_marker_for_file(
        source_file, execution_id=execution.execution_id, local_destination=destination
    )

    assert delivery.deliver_calls == []
    marker_path = source_file.with_suffix(source_file.suffix + ".mrk")
    assert (destination / marker_path.name).exists()
    records = execution_service.list_file_processing(execution.execution_id)
    deliver_record = next(r for r in records if r.processing_step == "DELIVER")
    assert deliver_record.output_file_path == str(destination / marker_path.name)


def test_generate_marker_for_file_sftp_true_and_output_path_together_raises(
    db, tmp_path
):
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(ValueError):
        orchestrator.generate_marker_for_file(
            source_file, sftp=True, local_destination=tmp_path / "out"
        )


def test_generate_marker_for_file_linked_writes_failed_file_processing_row_on_error(
    db, tmp_path, monkeypatch
):
    execution = _seed_full_run_execution()
    monkeypatch.setattr(settings, "SFTP_SUCCESS_DIRECTORY", None)
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(DeliveryConfigError):
        orchestrator.generate_marker_for_file(
            source_file, execution_id=execution.execution_id
        )

    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == ["MARKER", "DELIVER"]
    assert [r.status for r in records] == ["COMPLETED", "FAILED"]


def test_generate_marker_for_file_unlinked_writes_no_db_rows_on_error(
    db, tmp_path, monkeypatch
):
    monkeypatch.setattr(settings, "SFTP_SUCCESS_DIRECTORY", None)
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(DeliveryConfigError):
        orchestrator.generate_marker_for_file(source_file)

    assert _all_executions() == []


def test_generate_marker_for_file_raises_lookuperror_for_unknown_execution_id(
    db, tmp_path
):
    source_file = tmp_path / "arbitrary.csv"
    source_file.write_text("some,content\n1,2\n")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(LookupError):
        orchestrator.generate_marker_for_file(
            source_file, execution_id="does-not-exist"
        )


# --- Utility flows: transform_existing_csv (Flow 2) ---


async def test_transform_existing_csv_unlinked_returns_untracked_result_and_writes_no_db_rows(
    db, tmp_path
):
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = await orchestrator.transform_existing_csv("ChinaGTTReport", input_path)

    assert isinstance(result, UtilityOperationResult)
    assert result.execution_id is None
    output_path = Path(result.output_path)
    assert output_path.exists()
    assert "Fenergo ID" in output_path.read_text()
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert marker_path.exists()
    assert len(delivery.deliver_calls) == 2
    assert _all_executions() == []


async def test_transform_existing_csv_unlinked_respects_generate_marker_override_false(
    db, tmp_path
):
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = await orchestrator.transform_existing_csv(
        "ChinaGTTReport", input_path, generate_marker=False
    )

    output_path = Path(result.output_path)
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert not marker_path.exists()
    assert len(delivery.deliver_calls) == 1


async def test_transform_existing_csv_unlinked_marker_file_missing_input_raises(
    db, tmp_path
):
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(FileNotFoundError):
        await orchestrator.transform_existing_csv(
            "ChinaGTTReport", tmp_path / "does-not-exist.csv"
        )

    assert _all_executions() == []


async def test_transform_existing_csv_linked_writes_file_processing_rows(db, tmp_path):
    execution = _seed_full_run_execution(report_name="ChinaGTTReport")
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = await orchestrator.transform_existing_csv(
        "ChinaGTTReport", input_path, execution_id=execution.execution_id
    )

    assert result.execution_id == execution.execution_id
    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == [
        "TRANSFORM",
        "MARKER",
        "DELIVER",
        "DELIVER",
    ]
    assert all(r.status == "COMPLETED" for r in records)
    # The linked execution's own report_execution row is untouched - re-processing
    # is a file-level event, not a change to the original run's outcome.
    unchanged = execution_service.get_execution(execution.execution_id)
    assert unchanged.status == ExecutionStatus.REQUEST_SUBMITTED.value
    assert unchanged.output_file_path is None


async def test_transform_existing_csv_no_sftp_no_output_path_skips_delivery(
    db, tmp_path
):
    execution = _seed_full_run_execution(report_name="ChinaGTTReport")
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    await orchestrator.transform_existing_csv(
        "ChinaGTTReport", input_path, execution_id=execution.execution_id, sftp=False
    )

    assert delivery.deliver_calls == []
    records = execution_service.list_file_processing(execution.execution_id)
    assert [r.processing_step for r in records] == ["TRANSFORM", "MARKER"]


async def test_transform_existing_csv_output_path_copies_locally(db, tmp_path):
    execution = _seed_full_run_execution(report_name="ChinaGTTReport")
    destination = tmp_path / "local_drop"
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    delivery = FakeDeliveryService()
    orchestrator = ReportingOrchestrator(delivery_service=delivery)

    result = await orchestrator.transform_existing_csv(
        "ChinaGTTReport",
        input_path,
        execution_id=execution.execution_id,
        local_destination=destination,
    )

    assert delivery.deliver_calls == []
    output_path = Path(result.output_path)
    marker_path = output_path.with_suffix(output_path.suffix + ".mrk")
    assert (destination / output_path.name).exists()
    assert (destination / marker_path.name).exists()
    records = execution_service.list_file_processing(execution.execution_id)
    deliver_records = [r for r in records if r.processing_step == "DELIVER"]
    assert len(deliver_records) == 2


async def test_transform_existing_csv_sftp_true_and_output_path_together_raises(
    db, tmp_path
):
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(ValueError):
        await orchestrator.transform_existing_csv(
            "ChinaGTTReport", input_path, sftp=True, local_destination=tmp_path / "out"
        )


async def test_transform_existing_csv_linked_raises_on_report_name_mismatch(
    db, tmp_path
):
    execution = _seed_full_run_execution(report_name="CANDERReport")
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(ValueError):
        await orchestrator.transform_existing_csv(
            "ChinaGTTReport", input_path, execution_id=execution.execution_id
        )

    assert execution_service.list_file_processing(execution.execution_id) == []


async def test_transform_existing_csv_linked_writes_failed_file_processing_row_on_error(
    db, tmp_path
):
    execution = _seed_full_run_execution(report_name="ChinaGTTReport")
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(FileNotFoundError):
        await orchestrator.transform_existing_csv(
            "ChinaGTTReport",
            tmp_path / "does-not-exist.csv",
            execution_id=execution.execution_id,
        )

    records = execution_service.list_file_processing(execution.execution_id)
    assert len(records) == 1
    assert records[0].processing_step == "TRANSFORM"
    assert records[0].status == "FAILED"


async def test_transform_existing_csv_raises_lookuperror_for_unknown_execution_id(
    db, tmp_path
):
    input_path = tmp_path / "existing.csv"
    input_path.write_text(CHINA_GTT_CSV)
    orchestrator = ReportingOrchestrator(delivery_service=FakeDeliveryService())

    with pytest.raises(LookupError):
        await orchestrator.transform_existing_csv(
            "ChinaGTTReport", input_path, execution_id="does-not-exist"
        )


def test_timestamp_is_date_only_no_time_component():
    """2026-09-01: output/marker filenames switched to a date-only YYYYMMDD stamp,
    matching the finalized reports' naming standard - see docs/decisions.md."""
    value = ReportingOrchestrator._timestamp()
    assert len(value) == 8
    assert value.isdigit()
    assert datetime.strptime(value, "%Y%m%d")


async def test_output_filename_uses_date_only_timestamp(db):
    orchestrator = ReportingOrchestrator(
        fenergo_service=FakeFenergoService(),
        download_service=FakeDownloadService(),
        delivery_service=FakeDeliveryService(),
    )

    execution = await orchestrator.run_report("ChinaGTTReport")

    output_path = Path(execution.output_file_path)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    assert output_path.name == f"ChinaGTTReport_{today}.csv"


def _all_executions():
    return execution_service.list_executions()

```

assets/templates/CanadianDerivatives_OnboardingStatus.xml:

```xml
<?xml version="1.0" encoding="utf-8" ?>
<!-- Source=Target deliberately identical for now - real Target renames come later,
     once transform is actually needed (see docs/decisions.md). -->
<ExtractConfig ExtractType="CSV" DateFormat="M/d/yyyy h:mm:ss tt" MaxRetry="5">
    <RecordTemplate>
        <Column Assignment="1" Source="Fenergo ID" Target="Fenergo ID" DataType="String" />
        <Column Assignment="2" Source="Legal Entity Type" Target="Legal Entity Type" DataType="String" />
        <Column Assignment="3" Source="Legal Entity Name" Target="Legal Entity Name" DataType="String" />
        <Column Assignment="4" Source="LEI" Target="LEI" DataType="String" />
        <Column Assignment="5" Source="Associated Asset Manager" Target="Associated Asset Manager" DataType="String" />
        <Column Assignment="6" Source="Regulatory Status" Target="Regulatory Status" DataType="String" />
        <Column Assignment="7" Source="Canadian Reporting Requirements Completed" Target="Canadian Reporting Requirements Completed" DataType="String" />
        <Column Assignment="8" Source="Canadian Person Representation" Target="Canadian Person Representation" DataType="String" />
        <Column Assignment="9" Source="Country of Incorporation" Target="Country of Incorporation" DataType="String" />
        <Column Assignment="10" Source="Principal Place of Business" Target="Principal Place of Business" DataType="String" />
        <Column Assignment="11" Source="NonGBM or Agency Indicator" Target="NonGBM or Agency Indicator" DataType="String" />
        <Column Assignment="12" Source="NonGBM or Agency Category" Target="NonGBM or Agency Category" DataType="String" />
        <Column Assignment="13" Source="KYC Level" Target="KYC Level" DataType="String" />
        <Column Assignment="14" Source="Data Source" Target="Data Source" DataType="String" />
        <Column Assignment="15" Source="Markit Match ID" Target="Markit Match ID" DataType="String" />
        <Column Assignment="16" Source="ISDA Amend" Target="ISDA Amend" DataType="String" />
        <Column Assignment="17" Source="Bilateral Data Source" Target="Bilateral Data Source" DataType="String" />
        <Column Assignment="18" Source="Canadian Representation Agreement" Target="Canadian Representation Agreement" DataType="String" />
        <Column Assignment="19" Source="ISDA Status" Target="ISDA Status" DataType="String" />
        <Column Assignment="20" Source="Jurisdiction of Incorporation_HO_PPB" Target="Jurisdiction of Incorporation_HO_PPB" DataType="String" />
        <Column Assignment="21" Source="Registered Derivatives Dealer" Target="Registered Derivatives Dealer" DataType="String" />
        <Column Assignment="22" Source="Province_RDD" Target="Province_RDD" DataType="String" />
        <Column Assignment="23" Source="Canadian Affiliate" Target="Canadian Affiliate" DataType="String" />
        <Column Assignment="24" Source="Province_Affiliate" Target="Province_Affiliate" DataType="String" />
        <Column Assignment="25" Source="Additional Covenant Reporting" Target="Additional Covenant Reporting" DataType="String" />
        <Column Assignment="26" Source="Province_AdditionalCovenantResponsibility" Target="Province_AdditionalCovenantResponsibility" DataType="String" />
        <Column Assignment="27" Source="Consent to Disclosure" Target="Consent to Disclosure" DataType="String" />
        <Column Assignment="28" Source="Reporting Party Rules" Target="Reporting Party Rules" DataType="String" />
        <Column Assignment="29" Source="ISDA Master Agreement" Target="ISDA Master Agreement" DataType="String" />
        <Column Assignment="30" Source="Multilateral Agreement For Dealers" Target="Multilateral Agreement For Dealers" DataType="String" />
        <Column Assignment="31" Source="Clearing Exception" Target="Clearing Exception" DataType="String" />
        <Column Assignment="32" Source="CAD PCA Principal Type" Target="CAD PCA Principal Type" DataType="String" />
        <Column Assignment="33" Source="Consent to all Reporting Requirements" Target="Consent to all Reporting Requirements" DataType="String" />
        <Column Assignment="34" Source="Above participant affiliate threshold" Target="Above participant affiliate threshold" DataType="String" />
        <Column Assignment="35" Source="Above local counterparty threshold" Target="Above local counterparty threshold" DataType="String" />
        <Column Assignment="36" Source="OTC Regulated Clearing Agency Participant" Target="OTC Regulated Clearing Agency Participant" DataType="String" />
        <Column Assignment="37" Source="Crown Corporation Entity" Target="Crown Corporation Entity" DataType="String" />
        <Column Assignment="38" Source="Approved Canadian Derivative Workflow" Target="Approved Canadian Derivative Workflow" DataType="String" />
        <Column Assignment="39" Source="Latest Managerial Approval" Target="Latest Managerial Approval" DataType="String" />
        <Column Assignment="40" Source="LE_AnyCompletedCANDERClassificationInCompletedCase" Target="LE_AnyCompletedCANDERClassificationInCompletedCase" DataType="String" />
        <Column Assignment="41" Source="Cander_DeScope" Target="Cander_DeScope" DataType="String" />
    </RecordTemplate>
</ExtractConfig>

```

assets/templates/China_OnboardingStatus.xml:

```xml
<?xml version="1.0" encoding="utf-8" ?>
<!-- Source=Target deliberately identical for now - real Target renames come later,
     once transform is actually needed (see docs/decisions.md). -->
<ExtractConfig ExtractType="CSV" DateFormat="M/d/yyyy h:mm:ss tt" MaxRetry="5">
    <RecordTemplate>
        <Column Assignment="1" Source="Fenergo ID" Target="Fenergo ID" DataType="String" />
        <Column Assignment="2" Source="Client Name" Target="Client Name" DataType="String" />
        <Column Assignment="3" Source="LEI" Target="LEI" DataType="String" />
        <Column Assignment="4" Source="Global Risk Rating" Target="Global Risk Rating" DataType="String" />
        <Column Assignment="5" Source="Scheduled Review Date" Target="Scheduled Review Date" DataType="String" />
        <Column Assignment="6" Source="China Risk Rating" Target="China Risk Rating" DataType="String" />
        <Column Assignment="7" Source="China Risk Rating (Override)" Target="China Risk Rating (Override)" DataType="String" />
        <Column Assignment="8" Source="China Next Review Date" Target="China Next Review Date" DataType="String" />
        <Column Assignment="9" Source="China Next Review Date (Override)" Target="China Next Review Date (Override)" DataType="String" />
        <Column Assignment="10" Source="China Comments" Target="China Comments" DataType="String" />
        <Column Assignment="11" Source="Country of Incorporation" Target="Country of Incorporation" DataType="String" />
        <Column Assignment="12" Source="Product Category" Target="Product Category" DataType="String" />
        <Column Assignment="13" Source="Product Type" Target="Product Type" DataType="String" />
        <Column Assignment="14" Source="Booking Entity" Target="Booking Entity" DataType="String" />
        <Column Assignment="15" Source="Arranging Entity" Target="Arranging Entity" DataType="String" />
        <Column Assignment="16" Source="Product ID" Target="Product ID" DataType="String" />
    </RecordTemplate>
</ExtractConfig>

```

assets/templates/Product_OnboardingStatus.xml:

```xml
<?xml version="1.0" encoding="utf-8" ?>
<!-- Placeholder - real CSV column headers for this report don't exist yet
     (see docs/decisions.md). Zero columns parses fine (TemplateReader has no
     minimum-column requirement) but transform() will produce an empty-header
     CSV until real Source/Target columns are added here. -->
<ExtractConfig ExtractType="CSV" DateFormat="M/d/yyyy h:mm:ss tt" MaxRetry="5">
    <RecordTemplate>
    </RecordTemplate>
</ExtractConfig>

```

assets/templates/UK_Product_OnboardingStatus.xml:

```xml
<?xml version="1.0" encoding="utf-8" ?>
<!-- Source=Target deliberately identical for now - real Target renames come later,
     once transform is actually needed (see docs/decisions.md). -->
<ExtractConfig ExtractType="CSV" DateFormat="M/d/yyyy h:mm:ss tt" MaxRetry="5">
    <RecordTemplate>
        <Column Assignment="1" Source="FENERGO ID" Target="FENERGO ID" DataType="String" />
        <Column Assignment="2" Source="Legal Entity Type" Target="Legal Entity Type" DataType="String" />
        <Column Assignment="3" Source="Legal Entity Name" Target="Legal Entity Name" DataType="String" />
        <Column Assignment="4" Source="LEI" Target="LEI" DataType="String" />
        <Column Assignment="5" Source="Associated Asset Manager" Target="Associated Asset Manager" DataType="String" />
        <Column Assignment="6" Source="Date of Terms of Business (UK)" Target="Date of Terms of Business (UK)" DataType="String" />
        <Column Assignment="7" Source="Final Global Risk Rating" Target="Final Global Risk Rating" DataType="String" />
        <Column Assignment="8" Source="Final UK Jurisdiction Risk Rating" Target="Final UK Jurisdiction Risk Rating" DataType="String" />
        <Column Assignment="9" Source="Jurisdiction Next Review Date (Calc.)" Target="Jurisdiction Next Review Date (Calc.)" DataType="String" />
    </RecordTemplate>
</ExtractConfig>

```

# new proudct


assets/templates/Product_OnboardingStatus.xml:

```xml
<?xml version="1.0" encoding="utf-8" ?>
<!-- Source=Target deliberately identical for now - real Target renames come later,
     once transform is actually needed (see docs/decisions.md). -->
<ExtractConfig ExtractType="CSV" DateFormat="M/d/yyyy h:mm:ss tt" MaxRetry="5">
    <RecordTemplate>
        <Column Assignment="1" Source="Fenergo ID" Target="Fenergo ID" DataType="String" />
        <Column Assignment="2" Source="Legal Entity Name" Target="Legal Entity Name" DataType="String" />
        <Column Assignment="3" Source="LEI" Target="LEI" DataType="String" />
        <Column Assignment="4" Source="Product ID" Target="Product ID" DataType="String" />
        <Column Assignment="5" Source="Product Category" Target="Product Category" DataType="String" />
        <Column Assignment="6" Source="Product Type" Target="Product Type" DataType="String" />
        <Column Assignment="7" Source="Booking Entity" Target="Booking Entity" DataType="String" />
        <Column Assignment="8" Source="Arranging Entity" Target="Arranging Entity" DataType="String" />
        <Column Assignment="9" Source="Product Status" Target="Product Status" DataType="String" />
        <Column Assignment="10" Source="Product_Index_Key" Target="Product_Index_Key" DataType="String" />
        <Column Assignment="11" Source="Associated Asset Manager" Target="Associated Asset Manager" DataType="String" />
        <Column Assignment="12" Source="Country of Incorporation" Target="Country of Incorporation" DataType="String" />
        <Column Assignment="13" Source="Principal Place of Business" Target="Principal Place of Business" DataType="String" />
        <Column Assignment="14" Source="Non-GBM or Agency Indicator" Target="Non-GBM or Agency Indicator" DataType="String" />
        <Column Assignment="15" Source="Non-GBM or Agency Category" Target="Non-GBM or Agency Category" DataType="String" />
        <Column Assignment="16" Source="KYC Level" Target="KYC Level" DataType="String" />
        <Column Assignment="17" Source="US_Person_2013" Target="US_Person_2013" DataType="String" />
        <Column Assignment="18" Source="US_Person_2020" Target="US_Person_2020" DataType="String" />
        <Column Assignment="19" Source="US_Guarantee_2013" Target="US_Guarantee_2013" DataType="String" />
        <Column Assignment="20" Source="US_Guarantee_2020" Target="US_Guarantee_2020" DataType="String" />
        <Column Assignment="21" Source="US_Conduit_Affiliate_2013" Target="US_Conduit_Affiliate_2013" DataType="String" />
        <Column Assignment="22" Source="US_NaturalPerson_Incorporated" Target="US_NaturalPerson_Incorporated" DataType="String" />
        <Column Assignment="23" Source="Swaps_Through_US_Branch" Target="Swaps_Through_US_Branch" DataType="String" />
        <Column Assignment="24" Source="Swaps_Through_Foreign_Branch" Target="Swaps_Through_Foreign_Branch" DataType="String" />
        <Column Assignment="25" Source="US_Significant_Risk_Subsidiary" Target="US_Significant_Risk_Subsidiary" DataType="String" />
        <Column Assignment="26" Source="SBS_Foreign_Branch" Target="SBS_Foreign_Branch" DataType="String" />
        <Column Assignment="27" Source="Cander_Descope" Target="Cander_Descope" DataType="String" />
        <Column Assignment="28" Source="DF_Descope" Target="DF_Descope" DataType="String" />
        <Column Assignment="29" Source="EMIREA_Descope" Target="EMIREA_Descope" DataType="String" />
        <Column Assignment="30" Source="MAS_Descope" Target="MAS_Descope" DataType="String" />
    </RecordTemplate>
</ExtractConfig>
```
