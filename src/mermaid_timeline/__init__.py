# Bathymetrix™
# https://bathymetrix.com
# © 2026 Bathymetrix, LLC
# Author: Joel D. Simon <jdsimon@bathymetrix.com>
# SPDX-License-Identifier: MIT

"""Top-level package for mermaid_timeline."""

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
from .cycle_raw import iter_cycle_events, iter_operational_log_entries
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

__all__ = [
    "AcquisitionWindow",
    "Bin2CycleConfig",
    "Bin2CycleError",
    "CycleCorpusStats",
    "EvidenceRecord",
    "MerCorpusStats",
    "MerDataBlock",
    "MerFileMetadata",
    "OperationalLogEntry",
    "ProductCoverage",
    "TimelineStatus",
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
    "iter_mer_env_files",
    "iter_mer_files",
    "iter_processed_cycle",
    "iter_processed_cycle_h_files",
    "iter_processed_mer_env_files",
    "iter_mer_data_blocks",
    "iter_raw_inputs",
    "iter_server_mer",
    "parse_mer_file",
]

__version__ = "0.1.0"
