"""The installed TLS bundle must retain upstream emSign distrust decisions."""

from __future__ import annotations

import hashlib
import ssl

import certifi
import pytest


@pytest.mark.parametrize(
    ("name", "fingerprint"),
    [
        (
            "emSign ECC Root CA - G3",
            "86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b",
        ),
        (
            "emSign Root CA - C1",
            "125609aa301da0a249b97a8239cb6a34216f44dcac9f3954b14292f2e8c8608f",
        ),
        (
            "emSign ECC Root CA - C3",
            "bc4d809b15189d78db3e1d8cf4f9726a795da1643ca5f1358e1ddb0edc0d7eb3",
        ),
    ],
)
def test_removed_emsign_root_is_not_trusted(name: str, fingerprint: str) -> None:
    # G3: GHSA-6q48-6c7w-9gw2 / the prior published certifi bundle.
    # C1/C3: https://github.com/certifi/python-certifi/compare/2026.10.19...2026.10.23
    context = ssl.create_default_context(cafile=certifi.where())
    trusted = {
        hashlib.sha256(certificate).hexdigest()
        for certificate in context.get_ca_certs(binary_form=True)
    }
    assert trusted, "certifi did not provide any CA certificates"
    assert fingerprint not in trusted, f"removed CA remains trusted: {name}"
