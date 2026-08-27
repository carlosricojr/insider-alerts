from __future__ import annotations

import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from insider_alerts.research.sec_history import (
    ArchiveRef,
    Coverage,
    HistoryStore,
    OwnerFiling,
    RawObjectStore,
    classify_owner,
    parse_archive_manifest,
    select_complete_archives,
    sync_bulk_archives,
)
from insider_alerts.sec.client import SecResource


def _archive_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "SUBMISSION.tsv",
            "\t".join(
                [
                    "ACCESSION_NUMBER",
                    "FILING_DATE",
                    "PERIOD_OF_REPORT",
                    "DATE_OF_ORIG_SUB",
                    "DOCUMENT_TYPE",
                    "ISSUERCIK",
                    "ISSUERTRADINGSYMBOL",
                ]
            )
            + "\n"
            + "0000000001-24-000001\t02-JAN-2024\t02-JAN-2024\t\t4\t0000000042\tABC\n",
        )
        archive.writestr(
            "REPORTINGOWNER.tsv",
            "ACCESSION_NUMBER\tRPTOWNERCIK\n"
            "0000000001-24-000001\t0000000007\n",
        )
        archive.writestr(
            "NONDERIV_TRANS.tsv",
            "\t".join(
                [
                    "ACCESSION_NUMBER",
                    "NONDERIV_TRANS_SK",
                    "TRANS_DATE",
                    "TRANS_FORM_TYPE",
                    "TRANS_CODE",
                    "TRANS_ACQUIRED_DISP_CD",
                ]
            )
            + "\n"
            + "0000000001-24-000001\t9\t02-JAN-2024\t4\tP\tA\n",
        )
    return output.getvalue()


def _filing(year: int, month: int, *, accession_suffix: int = 1) -> OwnerFiling:
    return OwnerFiling(
        accession_number=f"0000000001-{year % 100:02d}-{accession_suffix:06d}",
        filing_date=date(year, month, 2),
        form_type="4",
        issuer_cik="42",
        owner_ciks=("7",),
        original_submission_date=None,
        transaction_dates=(date(year, month, 1),),
        has_invalid_transaction=False,
    )


def _coverage(
    *,
    complete_from: date = date(2006, 1, 1),
    complete_through: date = date(2026, 12, 31),
    snapshot_sha256: str = "a" * 64,
    missing_quarters: tuple[str, ...] = (),
) -> Coverage:
    return Coverage(
        complete_from=complete_from,
        complete_through=complete_through,
        source_snapshot_sha256=snapshot_sha256,
        missing_quarters=missing_quarters,
    )


def test_manifest_discovery_uses_link_targets_and_requires_unique_quarters() -> None:
    html = (
        '<a href="/files/structureddata/data/insider-transactions-data-sets/'
        '2006q1_form345.zip">old</a>'
        '<a href="/files/datastandardsinnovation/data/insider-transactions-data-sets/'
        '2026q2_form345.zip">new</a>'
    )
    refs = parse_archive_manifest(html)

    assert refs == [
        ArchiveRef(
            year=2006,
            quarter=1,
            url=(
                "https://www.sec.gov/files/structureddata/data/"
                "insider-transactions-data-sets/2006q1_form345.zip"
            ),
        ),
        ArchiveRef(
            year=2026,
            quarter=2,
            url=(
                "https://www.sec.gov/files/datastandardsinnovation/data/"
                "insider-transactions-data-sets/2026q2_form345.zip"
            ),
        ),
    ]

    with pytest.raises(ValueError, match="conflicting URLs"):
        parse_archive_manifest(
            html
            + '<a href="https://www.sec.gov/other/2006q1_form345.zip">duplicate</a>'
        )


def test_complete_archive_selection_fails_on_gap() -> None:
    refs = [
        ArchiveRef(2006, 1, "https://www.sec.gov/2006q1_form345.zip"),
        ArchiveRef(2006, 3, "https://www.sec.gov/2006q3_form345.zip"),
    ]
    with pytest.raises(ValueError, match="2006Q2"):
        select_complete_archives(refs, through=(2006, 3))


def test_bulk_sync_is_resumable_and_binds_snapshot_to_manifest(tmp_path: Path) -> None:
    archive_url = "https://www.sec.gov/files/data/2006q1_form345.zip"
    moved_url = "https://www.sec.gov/files/moved/2006q1_form345.zip"
    manifest = f'<a href="{archive_url}">2006 Q1</a>'.encode()
    resources = {
        "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets": (
            SecResource(manifest, 200, "https://www.sec.gov/manifest", None, None, "text/html")
        ),
        archive_url: SecResource(
            _archive_bytes(), 200, archive_url, '"archive"', None, "application/zip"
        ),
    }

    class FakeClient:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_resource(self, url: str) -> SecResource:
            self.urls.append(url)
            return resources[url]

    store = HistoryStore(tmp_path / "history.db")
    raw = RawObjectStore(tmp_path / "raw")
    client = FakeClient()
    def now() -> datetime:
        return datetime(2026, 8, 26, 20, 0, tzinfo=UTC)

    first = sync_bulk_archives(
        client=client,  # type: ignore[arg-type]
        store=store,
        raw_store=raw,
        through=(2006, 1),
        now_fn=now,
    )
    resources[
        "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
    ] = SecResource(
        f'<a href="{moved_url}">2006 Q1</a>'.encode(),
        200,
        "https://www.sec.gov/manifest",
        None,
        None,
        "text/html",
    )
    resources[moved_url] = SecResource(
        _archive_bytes(), 200, moved_url, '"archive"', None, "application/zip"
    )
    second = sync_bulk_archives(
        client=client,  # type: ignore[arg-type]
        store=store,
        raw_store=raw,
        through=(2006, 1),
        now_fn=now,
    )
    third = sync_bulk_archives(
        client=client,  # type: ignore[arg-type]
        store=store,
        raw_store=raw,
        through=(2006, 1),
        now_fn=now,
    )

    assert (first.downloaded_count, first.reused_count) == (1, 0)
    assert (second.downloaded_count, second.reused_count) == (1, 0)
    assert (third.downloaded_count, third.reused_count) == (0, 1)
    assert second.snapshot_sha256 == third.snapshot_sha256
    assert client.urls == [
        "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
        archive_url,
        "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
        moved_url,
        "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
    ]


def test_archive_alias_reuse_is_bound_to_expected_quarter(tmp_path: Path) -> None:
    store = HistoryStore(tmp_path / "history.db")
    raw = RawObjectStore(tmp_path / "raw")
    retrieved_at = datetime(2026, 8, 26, 20, 0, tzinfo=UTC)
    archive = raw.publish(_archive_bytes())
    store.record_retrieval(
        url="https://www.sec.gov/files/moved/2006q1_form345.zip",
        retrieved_at=retrieved_at,
        raw_object=archive,
        status_code=200,
        etag=None,
        last_modified=None,
        content_type="application/zip",
    )
    store.ingest_archive(
        ArchiveRef(2006, 2, "https://www.sec.gov/files/2006q2_form345.zip"),
        raw_object=archive,
        retrieved_at=retrieved_at,
    )

    assert (
        store.archived_object_for_url(
            "https://www.sec.gov/files/moved/2006q1_form345.zip",
            year=2006,
            quarter=1,
        )
        is None
    )
    assert store.archived_object_for_url(
        "https://www.sec.gov/files/moved/2006q1_form345.zip",
        year=2006,
        quarter=2,
    ) == archive


def test_raw_objects_and_normalized_rows_are_immutable(tmp_path: Path) -> None:
    raw = RawObjectStore(tmp_path / "raw")
    archive_bytes = _archive_bytes()
    published = raw.publish(archive_bytes, suffix=".zip")
    assert published.path.read_bytes() == archive_bytes

    store = HistoryStore(tmp_path / "history.db")
    retrieved_at = datetime(2026, 8, 26, 20, 0, tzinfo=UTC)
    store.record_retrieval(
        url="https://www.sec.gov/2006q1_form345.zip",
        retrieved_at=retrieved_at,
        raw_object=published,
        status_code=200,
        etag='"abc"',
        last_modified=None,
        content_type="application/zip",
    )
    manifest = raw.publish(b"manifest", suffix=".html")
    store.record_retrieval(
        url="https://www.sec.gov/manifest",
        retrieved_at=retrieved_at,
        raw_object=manifest,
        status_code=200,
        etag=None,
        last_modified=None,
        content_type="text/html",
    )
    store.ingest_archive(
        ArchiveRef(2006, 1, "https://www.sec.gov/2006q1_form345.zip"),
        raw_object=published,
        retrieved_at=retrieved_at,
    )
    store.ingest_archive(
        ArchiveRef(2006, 1, "https://www.sec.gov/moved/2006q1_form345.zip"),
        raw_object=published,
        retrieved_at=retrieved_at,
    )
    snapshot_sha = store.create_snapshot(
        manifest_sha256=manifest.sha256,
        members=[(2006, 1, published.sha256)],
        created_at=retrieved_at,
    )

    filings = store.owner_filings(snapshot_sha, "0000000007", cutoff_date=date(2025, 1, 1))
    assert filings == [
        OwnerFiling(
            accession_number="0000000001-24-000001",
            filing_date=date(2024, 1, 2),
            form_type="4",
            issuer_cik="42",
            owner_ciks=("7",),
            original_submission_date=None,
            transaction_dates=(date(2024, 1, 2),),
            has_invalid_transaction=False,
        )
    ]

    with store.connect() as conn, pytest.raises(Exception, match="immutable"):
        conn.execute("UPDATE sec_submissions SET issuer_cik='99'")


def test_classifier_finds_routine_pattern_with_observable_left_censoring() -> None:
    filings = [_filing(year, 3, accession_suffix=year) for year in (2021, 2022, 2023)]
    result = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=filings,
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )

    assert result.state == "routine"
    assert result.left_censored is True
    assert result.routine_since_year == 2024
    assert result.history_observation_start_date == date(2006, 1, 1)
    assert result.history_source_snapshot_sha256 == "a" * 64


def test_classifier_calls_bounded_disjoint_history_opportunistic() -> None:
    filings = [
        _filing(2021, 1, accession_suffix=1),
        _filing(2022, 2, accession_suffix=2),
        _filing(2023, 3, accession_suffix=3),
    ]
    result = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=filings,
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )

    assert result.state == "opportunistic"
    assert result.reason == "opportunistic_until_routine"
    assert result.history_coverage_complete is True
    assert result.left_censored is True


def test_history_coverage_requires_source_snapshot_digest() -> None:
    with pytest.raises(ValueError, match="source snapshot"):
        Coverage(
            complete_from=date(2006, 1, 1),
            complete_through=date(2023, 12, 31),
            source_snapshot_sha256="not-a-digest",
        )

    with pytest.raises(ValueError, match="observation boundary"):
        _coverage(complete_from=date(2021, 1, 1))


def test_history_digest_binds_observation_boundary_and_snapshot() -> None:
    filings = [
        _filing(2021, 1, accession_suffix=1),
        _filing(2022, 2, accession_suffix=2),
        _filing(2023, 3, accession_suffix=3),
    ]
    first = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=filings,
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )
    changed_snapshot = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=filings,
        coverage=_coverage(
            complete_through=date(2023, 12, 31), snapshot_sha256="b" * 64
        ),
    )

    assert first.history_input_sha256 != changed_snapshot.history_input_sha256


def test_opportunistic_state_persists_until_a_later_routine_window() -> None:
    initial = [
        _filing(2018, 1, accession_suffix=1),
        _filing(2019, 2, accession_suffix=2),
        _filing(2020, 3, accession_suffix=3),
    ]
    persisted = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=initial,
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )
    became_routine = classify_owner(
        owner_cik="7",
        classification_year=2027,
        filings=[
            *initial,
            _filing(2024, 5, accession_suffix=4),
            _filing(2025, 5, accession_suffix=5),
            _filing(2026, 5, accession_suffix=6),
        ],
        coverage=_coverage(),
    )

    assert (persisted.state, persisted.reason) == (
        "opportunistic",
        "opportunistic_until_routine",
    )
    assert became_routine.state == "routine"
    assert became_routine.routine_since_year == 2027


def test_classifier_fails_closed_for_missing_year_multiowner_and_amendment() -> None:
    missing = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=[_filing(2021, 1), _filing(2023, 3, accession_suffix=3)],
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )
    assert (missing.state, missing.reason) == (
        "unpartitionable",
        "no_partitionable_three_year_window",
    )

    multiowner = _filing(2022, 2, accession_suffix=2)
    multiowner = OwnerFiling(
        accession_number=multiowner.accession_number,
        filing_date=multiowner.filing_date,
        form_type=multiowner.form_type,
        issuer_cik=multiowner.issuer_cik,
        owner_ciks=("7", "8"),
        original_submission_date=None,
        transaction_dates=multiowner.transaction_dates,
        has_invalid_transaction=False,
    )
    ambiguous = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=[_filing(2021, 1), multiowner, _filing(2023, 3, accession_suffix=3)],
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )
    assert (ambiguous.state, ambiguous.reason) == (
        "unpartitionable",
        "ambiguous_owner_history",
    )

    original = _filing(2021, 1)
    orphan_amendment = OwnerFiling(
        accession_number="0000000001-22-000099",
        filing_date=date(2022, 4, 1),
        form_type="4/A",
        issuer_cik="42",
        owner_ciks=("7",),
        original_submission_date=date(2020, 1, 2),
        transaction_dates=(date(2020, 4, 1),),
        has_invalid_transaction=False,
    )
    amended = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=[original, orphan_amendment, _filing(2022, 2), _filing(2023, 3)],
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )
    assert (amended.state, amended.reason) == (
        "unpartitionable",
        "unresolved_amendment",
    )


def test_classifier_resolves_amendment_only_as_of_filing_cutoff() -> None:
    original = _filing(2021, 1)
    amendment = OwnerFiling(
        accession_number="0000000001-23-000099",
        filing_date=date(2023, 6, 1),
        form_type="4/A",
        issuer_cik="42",
        owner_ciks=("7",),
        original_submission_date=original.filing_date,
        transaction_dates=(date(2021, 3, 1),),
        has_invalid_transaction=False,
    )
    common = [_filing(2022, 3, accession_suffix=2), _filing(2023, 3, accession_suffix=3)]

    before = classify_owner(
        owner_cik="7",
        classification_year=2023,
        filings=[original, amendment, *common],
        coverage=_coverage(complete_through=date(2022, 12, 31)),
    )
    after = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=[original, amendment, *common],
        coverage=_coverage(complete_through=date(2023, 12, 31)),
    )

    assert before.state == "unpartitionable"
    assert after.state == "routine"


def test_later_unresolved_amendment_invalidates_an_absorbed_routine_state() -> None:
    routine = [_filing(year, 3, accession_suffix=year) for year in (2018, 2019, 2020)]
    orphan = OwnerFiling(
        accession_number="0000000001-24-000099",
        filing_date=date(2024, 6, 1),
        form_type="4/A",
        issuer_cik="42",
        owner_ciks=("7",),
        original_submission_date=date(2024, 1, 2),
        transaction_dates=(date(2024, 1, 1),),
        has_invalid_transaction=False,
    )
    result = classify_owner(
        owner_cik="7",
        classification_year=2025,
        filings=[*routine, orphan],
        coverage=_coverage(complete_through=date(2024, 12, 31)),
    )

    assert (result.state, result.reason) == ("unpartitionable", "unresolved_amendment")


def test_classifier_fails_closed_for_archive_gap_or_stale_coverage() -> None:
    filings = [_filing(year, 3, accession_suffix=year) for year in (2021, 2022, 2023)]
    gap = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=filings,
        coverage=_coverage(
            complete_through=date(2023, 12, 31), missing_quarters=("2010Q2",)
        ),
    )
    stale = classify_owner(
        owner_cik="7",
        classification_year=2024,
        filings=filings,
        coverage=_coverage(complete_through=date(2023, 9, 30)),
    )

    assert (gap.state, gap.reason) == ("unpartitionable", "coverage_gap")
    assert (stale.state, stale.reason) == ("unpartitionable", "coverage_not_current")
