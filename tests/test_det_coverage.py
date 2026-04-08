# SPDX-License-Identifier: MIT

from mermaid_timeline.det_coverage import collect_detected_coverage
from mermaid_timeline.models import ProductCoverage


def test_collect_detected_coverage_returns_list() -> None:
    coverage = collect_detected_coverage([ProductCoverage(product_name="detected")])

    assert [item.product_name for item in coverage] == ["detected"]
