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
    CycleCorpusStats,
    MerCorpusStats,
    audit_processed_cycle,
    audit_processed_cycle_h,
    audit_server_mer,
)
from .bin2cycle import Bin2CycleConfig, Bin2CycleError, iter_decoded_cycle_lines
from .bin2log import (
    Bin2LogConfig,
    Bin2LogError,
    iter_decoded_log_lines,
    update_decoder_database,
)
from .operational_raw import iter_cycle_events, iter_operational_log_entries
from .discovery import (
    iter_bin_files,
    iter_cycle_files,
    iter_emitted_cycle_files,
    iter_log_files,
    iter_mer_env_files,
    iter_mer_files,
    iter_processed_cycle,
    iter_processed_cycle_h_files,
    iter_processed_mer_env_files,
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

__all__ = [
    "AcquisitionWindow",
    "Bin2CycleConfig",
    "Bin2CycleError",
    "Bin2LogConfig",
    "Bin2LogError",
    "CycleCorpusStats",
    "EvidenceRecord",
    "MerCorpusStats",
    "MerDataBlock",
    "MerFileMetadata",
    "MerJsonlPrototypeSummary",
    "OperationalLogEntry",
    "ProductCoverage",
    "TimelineStatus",
    "LogJsonlPrototypeSummary",
    "audit_processed_cycle",
    "audit_processed_cycle_h",
    "audit_server_mer",
    "iter_bin_files",
    "iter_cycle_files",
    "iter_cycle_events",
    "iter_emitted_cycle_files",
    "iter_log_files",
    "iter_operational_log_entries",
    "iter_decoded_cycle_lines",
    "iter_decoded_log_lines",
    "update_decoder_database",
    "iter_mer_env_files",
    "iter_mer_files",
    "iter_processed_cycle",
    "iter_processed_cycle_h_files",
    "iter_processed_mer_env_files",
    "iter_mer_data_blocks",
    "iter_raw_inputs",
    "iter_server_mer",
    "parse_mer_file",
    "write_log_jsonl_prototypes",
    "write_mer_jsonl_prototypes",
]
