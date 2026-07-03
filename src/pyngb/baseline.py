"""
Baseline subtraction functionality for NGB files.

This module provides functionality to subtract baseline measurements from sample data,
handling both isothermal and dynamic segments appropriately.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import polars as pl

from .constants import FileMetadata

__all__ = ["BaselineSubtractor", "Segment", "subtract_baseline"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@dataclass(frozen=True)
class Segment:
    """A contiguous run of rows subtracted as one unit.

    ``stage`` is the temperature-program stage key the rows belong to, or
    ``None`` for rows recorded outside the program (e.g. a post-program
    cooling tail).
    """

    start: int  # inclusive row index
    end: int  # exclusive row index
    kind: Literal["isothermal", "dynamic", "uncovered"]
    stage: str | None


class BaselineSubtractor:
    """Handles baseline subtraction operations for NGB data."""

    def identify_segments(
        self, df: pl.DataFrame, temperature_program: dict[str, dict[str, float]]
    ) -> list[Segment]:
        """
        Partition every row of ``df`` into contiguous program segments.

        Stage boundaries come from cumulative stage durations matched against
        the ``time`` column. The final stage is closed on the right so a
        sample recorded exactly at the program's end still belongs to it, and
        rows recorded after the program ends form a trailing "uncovered"
        segment — no row is ever dropped.

        Parameters
        ----------
        df : pl.DataFrame
            The data to analyze
        temperature_program : dict
            Temperature program metadata from the file

        Returns
        -------
        list[Segment]
            Ordered, non-overlapping segments covering all rows of ``df``.
        """
        times = df["time"].to_numpy()
        n = len(times)

        # Cumulative stage end-times, in program order
        stages: list[tuple[str, float, float]] = []
        cumulative_time = 0.0
        for stage_name, stage_data in temperature_program.items():
            cumulative_time += stage_data.get("time", 0.0)
            heating_rate = stage_data.get("heating_rate", 0.0)
            stages.append((stage_name, heating_rate, cumulative_time))

        segments: list[Segment] = []
        idx = 0
        for i, (stage_name, heating_rate, end_time) in enumerate(stages):
            side: Literal["left", "right"] = "right" if i == len(stages) - 1 else "left"
            stop = int(np.searchsorted(times, end_time, side=side))
            if stop > idx:
                kind: Literal["isothermal", "dynamic"] = (
                    "isothermal" if abs(heating_rate) < 0.01 else "dynamic"
                )
                segments.append(Segment(idx, stop, kind, stage_name))
                idx = stop

        if idx < n:
            segments.append(Segment(idx, n, "uncovered", None))

        return segments

    def interpolate_baseline(
        self, sample_segment: pl.DataFrame, baseline_segment: pl.DataFrame, axis: str
    ) -> pl.DataFrame:
        """
        Interpolate baseline data to match sample data points.

        Parameters
        ----------
        sample_segment : pl.DataFrame
            Sample data segment
        baseline_segment : pl.DataFrame
            Baseline data segment
        axis : str
            Axis to interpolate on ("time", "sample_temperature", or "furnace_temperature")

        Returns
        -------
        pl.DataFrame
            Interpolated baseline data
        """
        if axis not in sample_segment.columns or axis not in baseline_segment.columns:
            logger.warning(f"Axis '{axis}' not found in data, falling back to 'time'")
            axis = "time"

        # Get sample axis values for interpolation
        sample_axis = sample_segment[axis].to_numpy()
        baseline_axis = baseline_segment[axis].to_numpy()

        # Create interpolated baseline DataFrame
        interpolated_data = {"axis_values": sample_axis}

        # Interpolate each column we need for subtraction
        for col in ["mass", "dsc_signal"]:
            if col in baseline_segment.columns:
                baseline_values = baseline_segment[col].to_numpy()

                # Remove any NaN values for interpolation
                valid_mask = ~(np.isnan(baseline_axis) | np.isnan(baseline_values))
                if np.sum(valid_mask) < 2:
                    # Not enough valid points for interpolation
                    interpolated_values = np.full_like(sample_axis, np.nan)
                else:
                    valid_baseline_axis = baseline_axis[valid_mask]
                    valid_baseline_values = baseline_values[valid_mask]

                    # np.interp requires an increasing axis. Sensor noise
                    # leaves tiny local inversions even on a clean heating
                    # ramp, and a cooling stage arrives reversed; sorting
                    # handles both. (Whether the axis is a usable coordinate
                    # at all is decided upstream by _resolve_axis.)
                    order = np.argsort(valid_baseline_axis, kind="stable")

                    # Linear interpolation, extrapolate with constant values
                    interpolated_values = np.interp(
                        sample_axis,
                        valid_baseline_axis[order],
                        valid_baseline_values[order],
                    )

                interpolated_data[col] = interpolated_values

        # Add the axis column
        interpolated_data[axis] = sample_axis

        return pl.DataFrame(interpolated_data)

    def _resolve_axis(self, baseline_segment: pl.DataFrame, axis: str) -> str:
        """Return ``axis`` if it is a usable interpolation coordinate over
        this baseline segment, else fall back to ``"time"`` with a warning.

        A usable axis moves essentially monotonically across the segment: the
        ratio of net travel to total travel is ~1 for a heating or cooling
        ramp (sensor noise only) and collapses toward 0 for isothermal holds
        or heat-then-cool spans, where interpolating on temperature maps one
        temperature to many times and produces nonsense.
        """
        if axis == "time" or axis not in baseline_segment.columns:
            # A missing axis already falls back to time in interpolate_baseline
            return axis

        x = baseline_segment[axis].to_numpy()
        x = x[~np.isnan(x)]
        if len(x) < 2:
            return axis

        total_travel = float(np.abs(np.diff(x)).sum())
        if total_travel == 0.0 or abs(float(x[-1] - x[0])) / total_travel < 0.95:
            logger.warning(
                f"Axis '{axis}' is not monotonic over this baseline segment "
                "(isothermal hold or heating-direction change); falling back "
                "to 'time' for baseline alignment"
            )
            return "time"

        return axis

    def subtract_segment(
        self, sample_segment: pl.DataFrame, baseline_segment: pl.DataFrame, axis: str
    ) -> pl.DataFrame:
        """
        Subtract baseline from sample for a single segment.

        Parameters
        ----------
        sample_segment : pl.DataFrame
            Sample data segment
        baseline_segment : pl.DataFrame
            Baseline data segment
        axis : str
            Axis to use for alignment

        Returns
        -------
        pl.DataFrame
            Sample data with baseline subtracted
        """
        # Interpolate baseline to match sample points
        interpolated_baseline = self.interpolate_baseline(
            sample_segment, baseline_segment, axis
        )

        # Start with the original sample data
        result = sample_segment.clone()

        # Subtract mass and dsc_signal if available
        for col in ["mass", "dsc_signal"]:
            if col in result.columns and col in interpolated_baseline.columns:
                baseline_values = interpolated_baseline[col]
                result = result.with_columns(
                    [(pl.col(col) - baseline_values).alias(col)]
                )

        return result

    def validate_temperature_programs(
        self, sample_metadata: FileMetadata, baseline_metadata: FileMetadata
    ) -> None:
        """
        Validate that sample and baseline have compatible temperature programs.

        Parameters
        ----------
        sample_metadata : FileMetadata
            Sample file metadata
        baseline_metadata : FileMetadata
            Baseline file metadata

        Raises
        ------
        ValueError
            If temperature programs are incompatible
        """
        sample_temp_prog = sample_metadata.get("temperature_program", {})
        baseline_temp_prog = baseline_metadata.get("temperature_program", {})

        if not sample_temp_prog:
            logger.warning("No temperature program found in sample file")
            return

        if not baseline_temp_prog:
            raise ValueError(
                "Baseline file has no temperature program metadata. "
                "Cannot validate compatibility with sample file."
            )

        # Check if both have the same number of stages
        if len(sample_temp_prog) != len(baseline_temp_prog):
            raise ValueError(
                f"Temperature program mismatch: sample has {len(sample_temp_prog)} stages, "
                f"baseline has {len(baseline_temp_prog)} stages"
            )

        # Check each stage for compatibility
        tolerance = 1e-3  # Tolerance for floating point comparison

        for stage_key in sample_temp_prog:
            if stage_key not in baseline_temp_prog:
                raise ValueError(
                    f"Stage '{stage_key}' missing in baseline temperature program"
                )

            sample_stage = sample_temp_prog[stage_key]
            baseline_stage = baseline_temp_prog[stage_key]

            # Check critical parameters
            critical_params = ["temperature", "heating_rate", "time"]

            for param in critical_params:
                sample_val = sample_stage.get(param, 0.0)
                baseline_val = baseline_stage.get(param, 0.0)

                if abs(sample_val - baseline_val) > tolerance:
                    raise ValueError(
                        f"Temperature program mismatch in stage '{stage_key}', parameter '{param}': "
                        f"sample={sample_val}, baseline={baseline_val}"
                    )

        logger.info("Temperature programs validated successfully")

    def process_baseline_subtraction(
        self,
        sample_df: pl.DataFrame,
        baseline_df: pl.DataFrame,
        sample_metadata: FileMetadata,
        baseline_metadata: FileMetadata,
        dynamic_axis: str = "time",
    ) -> pl.DataFrame:
        """
        Process complete baseline subtraction.

        Parameters
        ----------
        sample_df : pl.DataFrame
            Sample data
        baseline_df : pl.DataFrame
            Baseline data
        sample_metadata : FileMetadata
            Sample file metadata containing temperature program
        baseline_metadata : FileMetadata
            Baseline file metadata containing temperature program
        dynamic_axis : str
            Axis to use for dynamic segment subtraction

        Returns
        -------
        pl.DataFrame
            Processed data with baseline subtracted. Always has exactly as
            many rows as ``sample_df``, in the original order.

        Raises
        ------
        ValueError
            If temperature programs are incompatible
        """
        # Validate temperature programs first
        self.validate_temperature_programs(sample_metadata, baseline_metadata)

        if sample_df.height == 0:
            return sample_df.clone()

        temp_program = sample_metadata.get("temperature_program", {})
        if not temp_program:
            logger.warning("No temperature program found, treating all data as dynamic")
            axis = self._resolve_axis(baseline_df, dynamic_axis)
            return self.subtract_segment(sample_df, baseline_df, axis)

        # Segment both runs by the (validated identical) program so each
        # dynamic sample segment interpolates against the matching baseline
        # stage only. Interpolating a temperature axis against the full
        # baseline run is meaningless wherever that run holds or reverses.
        sample_segments = self.identify_segments(sample_df, temp_program)
        baseline_segments = {
            seg.stage: seg
            for seg in self.identify_segments(baseline_df, temp_program)
            if seg.stage is not None
        }

        n_isothermal = sum(s.kind == "isothermal" for s in sample_segments)
        n_dynamic = sum(s.kind == "dynamic" for s in sample_segments)
        n_uncovered = sum(
            s.end - s.start for s in sample_segments if s.kind == "uncovered"
        )
        logger.info(
            f"Found {n_isothermal} isothermal segments and {n_dynamic} dynamic segments"
        )
        if n_uncovered:
            logger.info(
                f"{n_uncovered} rows lie outside the temperature program "
                "(e.g. post-program cooling); aligning them on time"
            )

        processed_segments = []
        for seg in sample_segments:
            sample_segment = sample_df.slice(seg.start, seg.end - seg.start)

            baseline_match = (
                baseline_segments.get(seg.stage) if seg.stage is not None else None
            )
            if (
                baseline_match is not None
                and baseline_match.end - baseline_match.start >= 2
            ):
                baseline_segment = baseline_df.slice(
                    baseline_match.start, baseline_match.end - baseline_match.start
                )
            else:
                # Uncovered rows, and stages the baseline recorded no rows
                # for, align on time against the full baseline run: time
                # interpolation is exact where the runs overlap and clamps to
                # the baseline's endpoints beyond it.
                baseline_segment = baseline_df

            # Isothermal stages always align on time; dynamic stages use the
            # caller-selected axis when it is usable over the matched segment.
            if seg.kind == "dynamic":
                axis = self._resolve_axis(baseline_segment, dynamic_axis)
            else:
                axis = "time"

            processed_segments.append(
                self.subtract_segment(sample_segment, baseline_segment, axis)
            )

        result = pl.concat(processed_segments)

        # Segments partition the sample rows by construction; anything else
        # is a bug in identify_segments, not a data problem.
        if result.height != sample_df.height:
            raise RuntimeError(
                f"Baseline subtraction changed the row count "
                f"({sample_df.height} -> {result.height}); this is a pyngb bug"
            )

        return result


def subtract_baseline(
    sample_file: str | Path,
    baseline_file: str | Path,
    dynamic_axis: Literal[
        "time", "sample_temperature", "furnace_temperature"
    ] = "sample_temperature",
) -> pl.DataFrame:
    """
    Subtract baseline data from sample data.

    This function loads both sample (.ngb-ss3) and baseline (.ngb-bs3) files,
    validates that they have identical temperature programs, identifies isothermal
    and dynamic segments, and performs appropriate baseline subtraction. For
    isothermal segments, subtraction is done on the time axis. For dynamic
    segments, the user can choose the alignment axis; each dynamic segment is
    interpolated against the baseline rows of the *same* program stage, and
    falls back to the time axis (with a warning) if the chosen axis is not
    monotonic over that stage.

    Only the 'mass' and 'dsc_signal' columns are subtracted. All other columns
    (time, temperatures, flows) are retained from the sample file. The result
    has exactly one row per sample row — rows recorded outside the temperature
    program (e.g. a post-program cooling tail) are subtracted on the time axis
    rather than dropped.

    Parameters
    ----------
    sample_file : str or Path
        Path to the sample file (.ngb-ss3)
    baseline_file : str or Path
        Path to the baseline file (.ngb-bs3). Must have identical temperature
        program to the sample file.
    dynamic_axis : str, default="sample_temperature"
        Axis to use for dynamic segment alignment and subtraction.
        Options: "time", "sample_temperature", "furnace_temperature"

    Returns
    -------
    pl.DataFrame
        DataFrame with baseline-subtracted data

    Raises
    ------
    ValueError
        If temperature programs between sample and baseline are incompatible
    FileNotFoundError
        If either file does not exist

    Examples
    --------
    >>> # Basic subtraction using sample temperature axis for dynamic segments (default)
    >>> df = subtract_baseline("sample.ngb-ss3", "baseline.ngb-bs3")

    >>> # Use time axis for dynamic segment alignment
    >>> df = subtract_baseline(
    ...     "sample.ngb-ss3",
    ...     "baseline.ngb-bs3",
    ...     dynamic_axis="time"
    ... )
    """
    from .api.loaders import read_ngb

    # Load both files
    sample_metadata, sample_table = read_ngb(sample_file, return_metadata=True)
    baseline_metadata, baseline_table = read_ngb(baseline_file, return_metadata=True)

    # Convert to Polars DataFrames
    sample_df = pl.from_arrow(sample_table)
    baseline_df = pl.from_arrow(baseline_table)

    # Ensure we have DataFrames
    if not isinstance(sample_df, pl.DataFrame):
        raise TypeError("Sample data could not be converted to DataFrame")
    if not isinstance(baseline_df, pl.DataFrame):
        raise TypeError("Baseline data could not be converted to DataFrame")

    # Create subtractor and process
    subtractor = BaselineSubtractor()
    result = subtractor.process_baseline_subtraction(
        sample_df, baseline_df, sample_metadata, baseline_metadata, dynamic_axis
    )

    return result
