"""
Advanced data analysis and visualization tools for STA data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa

try:
    import matplotlib.pyplot as plt  # type: ignore[import-untyped]
    from matplotlib.figure import Figure  # type: ignore[import-untyped]

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    # Type stubs for when matplotlib is not available
    if TYPE_CHECKING:
        from matplotlib.figure import Figure  # type: ignore[import-untyped]
    else:
        Figure = None

__all__ = [
    "STAAnalyzer",
    "calculate_heat_flow",
    "calculate_mass_loss",
    "find_transitions",
    "onset_temperature",
    "peak_temperature",
]


class STAAnalyzer:
    """Advanced analysis tools for STA (Simultaneous Thermal Analysis) data.

    Provides common thermal analysis calculations including:
    - Mass loss calculations
    - Transition temperature detection
    - Heat flow analysis
    - Peak detection and characterization
    - Derivative calculations

    Examples:
        >>> from pynetzsch import load_ngb_data
        >>> from pynetzsch.analysis import STAAnalyzer
        >>>
        >>> table = load_ngb_data("sample.ngb-ss3")
        >>> analyzer = STAAnalyzer(table)
        >>>
        >>> # Calculate mass loss
        >>> mass_loss = analyzer.mass_loss_percent()
        >>> print(f"Total mass loss: {mass_loss:.1f}%")
        >>>
        >>> # Find thermal transitions
        >>> transitions = analyzer.find_thermal_events()
        >>> for event in transitions:
        >>>     print(f"Event at {event['temperature']:.1f}°C: {event['type']}")
    """

    df: pl.DataFrame  # Type annotation for mypy

    def __init__(self, data: Union[pa.Table, pl.DataFrame]):
        """Initialize analyzer with STA data.

        Args:
            data: PyArrow Table or Polars DataFrame with STA measurements
        """
        if isinstance(data, pa.Table):
            temp_df = pl.from_arrow(data)
            # Ensure we have a DataFrame, not a Series
            if isinstance(temp_df, pl.Series):
                self.df = pl.DataFrame(temp_df)
            else:
                self.df = temp_df
        else:
            self.df = data.clone()

        # Validate required columns
        required_cols = {"time", "temperature"}
        if not required_cols.issubset(set(self.df.columns)):
            missing = required_cols - set(self.df.columns)
            raise ValueError(f"Missing required columns: {missing}")

    def mass_loss_percent(self, column: str = "sample_mass") -> float:
        """Calculate total mass loss percentage.

        Args:
            column: Name of the mass column

        Returns:
            Mass loss as percentage
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found")

        mass_data = self.df.select(pl.col(column))
        initial_mass = mass_data[0, 0]
        final_mass = mass_data[-1, 0]

        return (initial_mass - final_mass) / initial_mass * 100  # type: ignore[no-any-return]

    def derivative_mass_loss(self, column: str = "sample_mass") -> pl.DataFrame:
        """Calculate derivative mass loss (DTG).

        Args:
            column: Name of the mass column

        Returns:
            DataFrame with DTG values
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found")

        # Calculate derivative using temperature as x-axis
        temp = self.df.select("temperature").to_numpy().flatten()
        mass = self.df.select(column).to_numpy().flatten()

        dtg = np.gradient(mass, temp)

        return self.df.with_columns([pl.Series("dtg", dtg).alias("dtg")])

    def find_thermal_events(
        self,
        dsc_column: str = "dsc",
        mass_column: str = "sample_mass",
        min_peak_height: float = 0.1,
        min_mass_change: float = 1.0,
    ) -> list[dict[str, Union[str, float]]]:
        """Identify thermal events (peaks, mass loss steps).

        Args:
            dsc_column: DSC signal column name
            mass_column: Mass column name
            min_peak_height: Minimum DSC peak height for detection
            min_mass_change: Minimum mass change (%) for detection

        Returns:
            List of thermal events with temperature and type
        """
        events = []

        # DSC peak detection
        if dsc_column in self.df.columns:
            dsc_peaks = self._find_peaks(dsc_column, min_peak_height)
            for peak in dsc_peaks:
                events.append(
                    {
                        "temperature": peak["temperature"],
                        "type": "DSC_peak",
                        "intensity": peak["intensity"],
                    }
                )

        # Mass loss step detection
        if mass_column in self.df.columns:
            mass_steps = self._find_mass_steps(mass_column, min_mass_change)
            for step in mass_steps:
                events.append(
                    {
                        "temperature": step["temperature"],
                        "type": "mass_loss",
                        "change_percent": step["change_percent"],
                    }
                )

        # Sort by temperature
        return sorted(events, key=lambda x: x["temperature"])  # type: ignore[arg-type]

    def _find_peaks(self, column: str, min_height: float) -> list[dict[str, float]]:
        """Internal peak detection method."""
        # Simple peak detection - could be enhanced with scipy.signal
        data = self.df.select([column, "temperature"]).to_numpy()
        peaks = []

        # Find local maxima
        for i in range(1, len(data) - 1):
            if (
                data[i, 0] > data[i - 1, 0]
                and data[i, 0] > data[i + 1, 0]
                and abs(data[i, 0]) > min_height
            ):
                peaks.append(
                    {"temperature": float(data[i, 1]), "intensity": float(data[i, 0])}
                )

        return peaks

    def _find_mass_steps(
        self, column: str, min_change: float
    ) -> list[dict[str, float]]:
        """Internal mass step detection method."""
        # Calculate rolling mass change
        df_with_change = self.df.with_columns(
            [(pl.col(column).pct_change() * 100).alias("mass_change_pct")]
        )

        steps = []
        # Find significant mass changes
        significant_changes = df_with_change.filter(
            pl.col("mass_change_pct").abs() > min_change / 10
        )

        if len(significant_changes) > 0:
            for row in significant_changes.iter_rows(named=True):
                steps.append(
                    {
                        "temperature": row["temperature"],
                        "change_percent": row["mass_change_pct"],
                    }
                )

        return steps

    def temperature_range(self) -> tuple[float, float]:
        """Get temperature range of the experiment."""
        temp_stats = (
            self.df.select("temperature").min().item(),
            self.df.select("temperature").max().item(),
        )
        return temp_stats

    def heating_rate_estimate(self) -> float:
        """Estimate average heating rate from data."""
        temp_range = self.temperature_range()
        time_range = (
            self.df.select("time").min().item(),
            self.df.select("time").max().item(),
        )

        delta_temp = temp_range[1] - temp_range[0]
        delta_time = time_range[1] - time_range[0]

        # Convert time from seconds to minutes if needed
        if delta_time > 3600:  # Likely in seconds
            delta_time /= 60

        return delta_temp / delta_time if delta_time > 0 else 0.0  # type: ignore[no-any-return]

    def plot_overview(self, save_path: Union[str, Path] | None = None) -> Figure | None:
        """Create comprehensive overview plot.

        Args:
            save_path: Optional path to save the plot

        Returns:
            Matplotlib Figure object if matplotlib is available
        """
        if not HAS_MATPLOTLIB:
            raise ImportError(
                "matplotlib is required for plotting. Install with: pip install matplotlib"
            )

        fig, axes = plt.subplots(2, 2, figsize=(12, 8))  # type: ignore[attr-defined]
        fig.suptitle("STA Analysis Overview", fontsize=14, fontweight="bold")

        temp = self.df.select("temperature").to_numpy().flatten()

        # Temperature vs Time
        if "time" in self.df.columns:
            time_data = self.df.select("time").to_numpy().flatten()
            axes[0, 0].plot(time_data, temp, "b-", linewidth=1)
            axes[0, 0].set_xlabel("Time (min)")
            axes[0, 0].set_ylabel("Temperature (°C)")
            axes[0, 0].set_title("Temperature Profile")
            axes[0, 0].grid(True, alpha=0.3)

        # DSC Signal
        if "dsc" in self.df.columns:
            dsc = self.df.select("dsc").to_numpy().flatten()
            axes[0, 1].plot(temp, dsc, "r-", linewidth=1)
            axes[0, 1].set_xlabel("Temperature (°C)")
            axes[0, 1].set_ylabel("DSC (μV)")
            axes[0, 1].set_title("DSC Signal")
            axes[0, 1].grid(True, alpha=0.3)

        # Mass Loss
        if "sample_mass" in self.df.columns:
            mass = self.df.select("sample_mass").to_numpy().flatten()
            axes[1, 0].plot(temp, mass, "g-", linewidth=1)
            axes[1, 0].set_xlabel("Temperature (°C)")
            axes[1, 0].set_ylabel("Mass (mg)")
            axes[1, 0].set_title("Thermogravimetric Analysis")
            axes[1, 0].grid(True, alpha=0.3)

        # Combined view
        if "dsc" in self.df.columns and "sample_mass" in self.df.columns:
            ax_dsc = axes[1, 1]
            ax_mass = ax_dsc.twinx()

            dsc = self.df.select("dsc").to_numpy().flatten()
            mass = self.df.select("sample_mass").to_numpy().flatten()

            line1 = ax_dsc.plot(temp, dsc, "r-", alpha=0.7, label="DSC")
            line2 = ax_mass.plot(temp, mass, "g-", alpha=0.7, label="Mass")

            ax_dsc.set_xlabel("Temperature (°C)")
            ax_dsc.set_ylabel("DSC (μV)", color="r")
            ax_mass.set_ylabel("Mass (mg)", color="g")
            ax_dsc.set_title("Combined Analysis")

            # Combined legend
            lines = line1 + line2
            labels = [line.get_label() for line in lines]
            ax_dsc.legend(lines, labels, loc="upper right")

        plt.tight_layout()  # type: ignore[attr-defined]

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")  # type: ignore[attr-defined]

        return fig


def calculate_mass_loss(
    data: Union[pa.Table, pl.DataFrame], mass_column: str = "sample_mass"
) -> float:
    """Calculate total mass loss percentage.

    Convenience function for quick mass loss calculation.

    Args:
        data: STA data table or dataframe
        mass_column: Name of the mass column

    Returns:
        Mass loss percentage

    Examples:
        >>> mass_loss = calculate_mass_loss(table)
        >>> print(f"Mass loss: {mass_loss:.1f}%")
    """
    analyzer = STAAnalyzer(data)
    return analyzer.mass_loss_percent(mass_column)


def find_transitions(
    data: Union[pa.Table, pl.DataFrame], **kwargs
) -> list[dict[str, Union[str, float]]]:
    """Find thermal transitions in STA data.

    Convenience function for thermal event detection.

    Args:
        data: STA data table or dataframe
        **kwargs: Arguments passed to STAAnalyzer.find_thermal_events

    Returns:
        List of thermal events
    """
    analyzer = STAAnalyzer(data)
    return analyzer.find_thermal_events(**kwargs)


def onset_temperature(data: Union[pa.Table, pl.DataFrame]) -> float | None:
    """Find onset temperature from DSC curve.

    Args:
        data: STA data

    Returns:
        Onset temperature or None if not found
    """
    # Simple onset detection - could be enhanced
    analyzer = STAAnalyzer(data)
    events = analyzer.find_thermal_events()

    for event in events:
        if event["type"] == "DSC_peak":
            temp = event["temperature"]
            return temp if isinstance(temp, float) else None

    return None


def peak_temperature(
    data: Union[pa.Table, pl.DataFrame], column: str = "dsc"
) -> float | None:
    """Find peak temperature from DSC curve.

    Args:
        data: STA data
        column: DSC column name

    Returns:
        Peak temperature or None if not found
    """
    df = pl.from_arrow(data) if isinstance(data, pa.Table) else data
    # Ensure we have a DataFrame, not a Series
    if isinstance(df, pl.Series):
        df = pl.DataFrame(df)

    if column not in df.columns:
        return None

    # Find maximum absolute value
    abs_max_idx = df.select(pl.col(column).abs().arg_max()).item()
    return df.select("temperature")[abs_max_idx, 0]  # type: ignore[no-any-return]


def calculate_heat_flow(
    data: Union[pa.Table, pl.DataFrame],
    dsc_column: str = "dsc",
    mass_column: str = "sample_mass",
) -> pl.DataFrame:
    """Calculate specific heat flow (normalized by mass).

    Args:
        data: STA data
        dsc_column: DSC signal column
        mass_column: Sample mass column

    Returns:
        DataFrame with heat_flow_specific column added
    """
    df = pl.from_arrow(data) if isinstance(data, pa.Table) else data.clone()
    # Ensure we have a DataFrame, not a Series
    if isinstance(df, pl.Series):
        df = pl.DataFrame(df)

    if dsc_column not in df.columns or mass_column not in df.columns:
        raise ValueError(f"Required columns not found: {dsc_column}, {mass_column}")

    return df.with_columns(
        [(pl.col(dsc_column) / pl.col(mass_column)).alias("heat_flow_specific")]
    )
