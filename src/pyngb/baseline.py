"""
Baseline subtraction functionality for NGB files.

This module provides functionality to subtract baseline measurements from sample data,
handling both isothermal and dynamic segments appropriately.
"""

from __future__ import annotations

import logging
from typing import Literal

import numpy as np
import polars as pl

from .constants import FileMetadata

__all__ = ["BaselineSubtractor", "subtract_baseline"]

logger = logging.getLogger(__name__)


class BaselineSubtractor:
    """Handles baseline subtraction operations for NGB data."""

    def identify_segments(
        self, df: pl.DataFrame, temperature_program: dict[str, dict[str, float]]
    ) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        """
        Identify isothermal and dynamic segments based on temperature program.

        Parameters
        ----------
        df : pl.DataFrame
            The data to analyze
        temperature_program : dict
            Temperature program metadata from the file

        Returns
        -------
        tuple[list[tuple[int, int]], list[tuple[int, int]]]
            (isothermal_segments, dynamic_segments) as lists of (start_idx, end_idx) tuples
        """
        isothermal_segments = []
        dynamic_segments = []

        # Sort stages by time (cumulative)
        stages = []
        cumulative_time = 0.0

        for stage_name, stage_data in temperature_program.items():
            stage_time = stage_data.get("time", 0.0)
            heating_rate = stage_data.get("heating_rate", 0.0)
            start_time = cumulative_time
            end_time = cumulative_time + stage_time

            stages.append(
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "heating_rate": heating_rate,
                    "temperature": stage_data.get("temperature", 0.0),
                }
            )

            cumulative_time = end_time

        # Map time ranges to DataFrame indices
        for stage in stages:
            if stage["end_time"] <= stage["start_time"]:
                continue  # Skip zero-duration stages

            # Find indices corresponding to this time range
            mask = (df["time"] >= stage["start_time"]) & (
                df["time"] < stage["end_time"]
            )
            indices = df.with_row_index().filter(mask)["index"].to_list()

            if len(indices) > 0:
                start_idx = min(indices)
                end_idx = max(indices) + 1  # +1 for exclusive end

                if abs(stage["heating_rate"]) < 0.01:  # Essentially zero heating rate
                    isothermal_segments.append((start_idx, end_idx))
                else:
                    dynamic_segments.append((start_idx, end_idx))

        return isothermal_segments, dynamic_segments

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

                    # Linear interpolation, extrapolate with constant values
                    interpolated_values = np.interp(
                        sample_axis, valid_baseline_axis, valid_baseline_values
                    )

                interpolated_data[col] = interpolated_values

        # Add the axis column
        interpolated_data[axis] = sample_axis

        return pl.DataFrame(interpolated_data)

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
            Processed data with baseline subtracted

        Raises
        ------
        ValueError
            If temperature programs are incompatible
        """
        # Validate temperature programs first
        self.validate_temperature_programs(sample_metadata, baseline_metadata)
        # Get temperature program
        temp_program = sample_metadata.get("temperature_program", {})
        if not temp_program:
            logger.warning("No temperature program found, treating all data as dynamic")
            # Treat entire dataset as one dynamic segment
            return self.subtract_segment(sample_df, baseline_df, dynamic_axis)

        # Identify segments
        isothermal_segments, dynamic_segments = self.identify_segments(
            sample_df, temp_program
        )

        logger.info(
            f"Found {len(isothermal_segments)} isothermal segments and {len(dynamic_segments)} dynamic segments"
        )

        # Process each segment
        processed_segments = []

        # Process isothermal segments (always use time axis)
        for start_idx, end_idx in isothermal_segments:
            sample_segment = sample_df.slice(start_idx, end_idx - start_idx)
            baseline_segment = baseline_df  # Use full baseline for interpolation

            processed_segment = self.subtract_segment(
                sample_segment, baseline_segment, "time"
            )
            processed_segments.append(processed_segment)

        # Process dynamic segments (use user-specified axis)
        for start_idx, end_idx in dynamic_segments:
            sample_segment = sample_df.slice(start_idx, end_idx - start_idx)
            baseline_segment = baseline_df  # Use full baseline for interpolation

            processed_segment = self.subtract_segment(
                sample_segment, baseline_segment, dynamic_axis
            )
            processed_segments.append(processed_segment)

        # If no segments found, process as single dynamic segment
        if not processed_segments:
            logger.warning(
                "No valid segments found, processing entire dataset as dynamic"
            )
            return self.subtract_segment(sample_df, baseline_df, dynamic_axis)

        # Combine all segments back together
        result = pl.concat(processed_segments)

        return result


def subtract_baseline(
    sample_file: str,
    baseline_file: str,
    dynamic_axis: Literal[
        "time", "sample_temperature", "furnace_temperature"
    ] = "sample_temperature",
) -> pl.DataFrame:
    """
    Subtract baseline data from sample data.

    This function loads both sample (.ngb-ss3) and baseline (.ngb-bs3) files,
    validates that they have identical temperature programs, identifies isothermal
    and dynamic segments, and performs appropriate baseline subtraction. For
    isothermal segments, subtraction is done on the time axis. For dynamic segments,
    the user can choose the alignment axis.

    Only the 'mass' and 'dsc_signal' columns are subtracted. All other columns
    (time, temperatures, flows) are retained from the sample file.

    Parameters
    ----------
    sample_file : str
        Path to the sample file (.ngb-ss3)
    baseline_file : str
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
