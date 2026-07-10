"""
Metadata contracts for NGB parsing: TypedDicts and column-metadata defaults.

Binary-format knowledge (dtype ids, record grammar, field/category maps)
lives in :mod:`pyngb.format`.
"""

from typing import Any, TypedDict

__all__ = [  # noqa: RUF022 - order chosen for logical grouping
    "BaseColumnMetadata",
    "BaselinableColumnMetadata",
    "DEFAULT_COLUMN_METADATA",
    "FIELD_APPLICABILITY",
    "FileMetadata",
    "SensitivityCalibration",
    "SensitivityFixpoint",
    "TemperatureCalibration",
    "TemperatureFixpoint",
]


class TemperatureFixpoint(TypedDict, total=False):
    """A single temperature-calibration fixpoint (phase-transition standard).

    Each fixpoint is one row of the Proteus temperature-calibration table:
    ``actual`` vs ``measured`` with a ``weight``, producing a ``corrected`` value.
    Standards vary per calibration (e.g. Biphenyl, Benzoeacid, KClO4, In, Sn) -
    the names and values are read from the file, never hard-coded.

    The relationship between the columns is exact and was verified against every
    available file (residuals ``< 1e-3``)::

        corrected_c = measured_c + correction(measured_c)
        correction(T) = 1e-3*B0 + 1e-5*B1*T + 1e-8*B2*T**2

    where ``[B0, B1, B2]`` are ``TemperatureCalibration.coefficients``. The
    remaining gap ``actual_c - corrected_c`` is the calibration residual.

    Fields:
        name: Standard name as recorded by Proteus.
        actual_c: Actual (literature) transition temperature in °C.
        measured_c: Raw measured transition temperature in °C (before correction).
        weight: Regression weight for this point (1.0 in all observed files).
        corrected_c: Measured value with the calibration polynomial applied (°C).
    """

    name: str
    actual_c: float
    measured_c: float
    weight: float
    corrected_c: float


class TemperatureCalibration(TypedDict, total=False):
    """Temperature-calibration block extracted for traceability/QA only.

    IMPORTANT: The ``sample_temperature`` channel stored in NGB files is already
    temperature-corrected by Proteus. These coefficients must NOT be applied to it
    (doing so would double-correct). They are captured for provenance only.

    The correction polynomial is the active one Proteus used to produce the
    ``corrected_c`` column of each fixpoint (verified by round-trip); it is simply
    not re-applied to the already-corrected sample channel.

    Fields:
        coefficients: Polynomial coefficients [B0, B1, B2]. The Proteus correction
            (NOT re-applied to the data by pyngb) is
            ``correction[°C] = 1e-3*B0 + 1e-5*B1*T_exp + 1e-8*B2*T_exp**2``.
        fixpoints: The phase-transition standards used for the calibration.
        record_path: Path to the external temperature-calibration record (.ngb-ts3).
        date_measured: When the calibration was performed (ISO 8601, UTC).
        gas: Purge gas used during the calibration run.
        crucible_type: Crucible used during the calibration run.
        heating_rate: Heating rate of the calibration run in K/min.
        comment: Operator comment recorded on the calibration run.
    """

    coefficients: list[float]
    fixpoints: list[TemperatureFixpoint]
    record_path: str
    date_measured: str
    gas: str
    crucible_type: str
    heating_rate: float
    comment: str


class SensitivityFixpoint(TypedDict, total=False):
    """A single DSC sensitivity-calibration fixpoint (enthalpy standard).

    Each fixpoint is one row of the Proteus sensitivity-calibration table:
    a standard of known transition enthalpy whose measured DSC peak area
    yields a sensitivity point, regressed into the ``calibration_constants``
    curve. Standards vary per calibration (e.g. Biphenyl, KClO4, CsCl, BaCO3) -
    names and values are read from the file, never hard-coded.

    Two relationships are exact and were verified against every available
    file (residuals ``< 4e-7``, f32 precision)::

        measured_sensitivity = peak_area / enthalpy
        fitted_sensitivity   = (P2 + P3*z + P4*z**2 + P5*z**3) * exp(-z**2)
        z = (temperature_c - P0) / P1

    where ``P0..P5`` are the file's ``calibration_constants`` - these
    standards are the regression behind that curve, and ``fitted_sensitivity``
    is the curve evaluated at each standard's transition temperature.

    Sign convention: ``enthalpy`` and ``peak_area`` are stored by Proteus
    with endothermic transitions negative and are reported as stored.

    Fields:
        name: Standard name as recorded by Proteus.
        temperature_c: Transition temperature of the standard in °C.
        enthalpy: Literature transition enthalpy in J/g (negative = endothermic).
        peak_area: Measured DSC peak area in µV·s/mg (sign matches enthalpy).
        measured_sensitivity: peak_area / enthalpy, in µV/mW.
        weight: Regression weight for this point (1.0 in all observed files).
        fitted_sensitivity: Calibration curve evaluated at temperature_c, in µV/mW.
    """

    name: str
    temperature_c: float
    enthalpy: float
    peak_area: float
    measured_sensitivity: float
    weight: float
    fitted_sensitivity: float


class SensitivityCalibration(TypedDict, total=False):
    """DSC sensitivity-calibration block (traceability/QA only).

    The calibration constants themselves (p0-p5) are exposed separately as
    ``calibration_constants``; this block records where they came from and
    the enthalpy standards they were regressed from.

    Fields:
        record_path: Path to the external sensitivity record (.ngb-es3).
        date_measured: When the calibration was performed (ISO 8601, UTC).
        gas: Purge gas used during the calibration run.
        crucible_type: Crucible used during the calibration run.
        heating_rate: Heating rate of the calibration run in K/min.
        comment: Operator comment recorded on the calibration run.
        fixpoints: The enthalpy standards used for the calibration.
    """

    record_path: str
    date_measured: str
    gas: str
    crucible_type: str
    heating_rate: float
    comment: str
    fixpoints: list[SensitivityFixpoint]


class FileMetadata(TypedDict, total=False):
    """Type definition for file metadata dictionary.

    Mass-related fields grouped together after core identifying fields.
    Reference masses are structurally derived; sample and reference crucible
    masses share one (category, field) shape and are disambiguated by their
    neighboring tables (see pyngb.format.extract.extract_masses).

    Note:
        All fields are optional (total=False) as the NGB binary format does not
        guarantee the presence of any particular metadata field. Files from different
        instruments, software versions, or with different configurations may have
        different subsets of metadata available.
    """

    instrument: str
    project: str
    date_performed: str
    lab: str
    operator: str
    crucible_type: str
    comment: str
    furnace_type: str
    carrier_type: str
    sample_id: str
    sample_name: str
    # Mass group
    sample_mass: float
    crucible_mass: float
    reference_mass: float
    reference_crucible_mass: float
    # Other descriptors
    material: str
    application_version: str
    licensed_to: str
    temperature_program: dict[str, dict[str, Any]]
    calibration_constants: dict[str, float]
    # Calibration provenance (traceability/QA only - never applied to the data)
    temperature_calibration: TemperatureCalibration
    sensitivity_calibration: SensitivityCalibration
    file_hash: dict[str, str]
    # Run environment
    timezone: str
    utc_offset_minutes: int
    correction_file_path: str
    # MFC (Mass Flow Controller) metadata, from the stream-1 device tree:
    # gas identity (name + short formula), full-scale range (ml/min), and
    # the flow setpoint the run actually used (ml/min) — read from the
    # per-stage device states, emitted only when uniform across the
    # program's body stages (per-stage values live in temperature_program;
    # 0.0 means the MFC was configured but not flowing).
    purge_1_mfc_gas: str
    purge_2_mfc_gas: str
    protective_mfc_gas: str
    purge_1_mfc_gas_formula: str
    purge_2_mfc_gas_formula: str
    protective_mfc_gas_formula: str
    purge_1_mfc_range: float
    purge_2_mfc_range: float
    protective_mfc_range: float
    purge_1_mfc_flow: float
    purge_2_mfc_flow: float
    protective_mfc_flow: float
    # Control parameters (PID settings)
    furnace_xp: float
    furnace_tn: float
    furnace_tv: float
    sample_xp: float
    sample_tn: float
    sample_tv: float


class BaseColumnMetadata(TypedDict, total=False):
    """Base column metadata structure for all thermal analysis data columns.

    This defines metadata fields that apply to all column types.
    """

    units: str  # Physical units (e.g., "mg", "°C", "mW", "mg/min")
    processing_history: list[
        str
    ]  # Processing steps applied (e.g., ["raw", "smoothed"])
    source: str  # Data origin (e.g., "measurement", "calculated", "derived")


class BaselinableColumnMetadata(BaseColumnMetadata, total=False):
    """Extended metadata for columns that support baseline correction and calibration.

    This includes the baseline_subtracted field for signals like mass and DSC
    that can be baseline-corrected, and calibration_applied for DSC signals
    that can be calibrated from µV to mW.
    """

    baseline_subtracted: bool  # True if baseline correction has been applied
    calibration_applied: bool  # True if calibration has been applied (DSC only)


# Define which metadata fields apply to which column types
FIELD_APPLICABILITY = {
    "units": "all",  # All columns have units
    "processing_history": "all",  # All columns have processing history
    "source": "all",  # All columns have a source
    "baseline_subtracted": [
        "mass",
        "dsc_signal",
    ],  # Only these can be baseline corrected
    "calibration_applied": [
        "dsc_signal",
    ],  # Only DSC signals can be calibrated
}

# Default metadata for common column types
DEFAULT_COLUMN_METADATA = {
    "time": {"units": "s", "processing_history": ["raw"], "source": "measurement"},
    "mass": {
        "units": "mg",
        "processing_history": ["raw"],
        "source": "measurement",
        "baseline_subtracted": False,
    },
    "sample_temperature": {
        "units": "°C",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "furnace_temperature": {
        "units": "°C",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "dsc_signal": {
        "units": "µV",
        "processing_history": ["raw"],
        "source": "measurement",
        "baseline_subtracted": False,
        "calibration_applied": False,
    },
    "dtg": {
        "units": "mg/min",
        "processing_history": ["calculated"],
        "source": "derived",
    },
    "purge_flow_1": {
        "units": "ml/min",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "purge_flow_2": {
        "units": "ml/min",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "protective_flow": {
        "units": "ml/min",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "furnace_power": {
        "units": "W",
        "processing_history": ["raw"],
        "source": "measurement",
    },
}
