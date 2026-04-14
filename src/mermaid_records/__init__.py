# Bathymetrix™
# https://bathymetrix.com
# © 2026 Bathymetrix, LLC
# Author: Joel D. Simon <jdsimon@bathymetrix.com>
# SPDX-License-Identifier: MIT

"""Top-level package for mermaid_records."""

__version__ = "0.1.0"
__author__ = "Joel D. Simon"
__license__ = "MIT"
__copyright__ = "© 2026 Bathymetrix, LLC"

from .audit import (
    MerCorpusStats,
    audit_server_mer,
)
from .bin2log import (
    Bin2LogConfig,
    Bin2LogError,
    iter_decoded_log_lines,
    update_decoder_database,
)
from .operational_raw import iter_operational_log_entries
from .parse_float_name import FloatName, parse_float_name
from .discovery import (
    iter_bin_files,
    iter_log_files,
    iter_mer_files,
    iter_raw_inputs,
    iter_server_mer,
)
from .models import (
    AcquisitionWindow,
    EvidenceRecord,
    MerDataBlock,
    MerFileMetadata,
    OperationalLogEntry,
    ProductCoverage,
    TimelineStatus,
)
from .mer_raw import iter_mer_data_blocks, parse_mer_file
from .normalize_log import LogJsonlPrototypeSummary, write_log_jsonl_prototypes
from .normalize_mer import MerJsonlPrototypeSummary, write_mer_jsonl_prototypes
from .normalize_pipeline import (
    NormalizationPipelineSummary,
    run_normalization_pipeline,
)

__all__ = [
    "AcquisitionWindow",
    "Bin2LogConfig",
    "Bin2LogError",
    "EvidenceRecord",
    "MerCorpusStats",
    "MerDataBlock",
    "MerFileMetadata",
    "MerJsonlPrototypeSummary",
    "OperationalLogEntry",
    "ProductCoverage",
    "TimelineStatus",
    "LogJsonlPrototypeSummary",
    "FloatName",
    "NormalizationPipelineSummary",
    "audit_server_mer",
    "iter_bin_files",
    "iter_log_files",
    "iter_operational_log_entries",
    "iter_decoded_log_lines",
    "update_decoder_database",
    "iter_mer_files",
    "iter_mer_data_blocks",
    "iter_raw_inputs",
    "iter_server_mer",
    "parse_float_name",
    "parse_mer_file",
    "write_log_jsonl_prototypes",
    "write_mer_jsonl_prototypes",
    "run_normalization_pipeline",
]
