"""Base validation classes and results."""

import logging

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results.

    Stores validation issues, warnings, and overall status.
    """

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.info: list[str] = []
        self.passed_checks: list[str] = []

    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Validation error: {message}")

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Validation warning: {message}")

    def add_info(self, message: str) -> None:
        """Add an info message."""
        self.info.append(message)
        logger.info(f"Validation info: {message}")

    def add_pass(self, check_name: str) -> None:
        """Mark a check as passed."""
        self.passed_checks.append(check_name)

    @property
    def is_valid(self) -> bool:
        """Return True if no errors were found."""
        return len(self.errors) == 0

    @property
    def has_warnings(self) -> bool:
        """Return True if warnings were found."""
        return len(self.warnings) > 0

    def summary(self) -> dict[str, int | bool]:
        """Get validation summary."""
        return {
            "is_valid": self.is_valid,
            "has_warnings": self.has_warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "checks_passed": len(self.passed_checks),
            "total_issues": len(self.errors) + len(self.warnings),
        }

    def report(self) -> str:
        """Generate a formatted validation report."""
        lines = ["=== STA Data Validation Report ===\n"]

        # Summary
        summary = self.summary()
        status = "✅ VALID" if summary["is_valid"] else "❌ INVALID"
        lines.append(f"Overall Status: {status}")
        lines.append(f"Checks Passed: {summary['checks_passed']}")
        lines.append(f"Errors: {summary['error_count']}")
        lines.append(f"Warnings: {summary['warning_count']}\n")

        # Errors
        if self.errors:
            lines.append("🔴 ERRORS:")
            for error in self.errors:
                lines.append(f"  • {error}")
            lines.append("")

        # Warnings
        if self.warnings:
            lines.append("🟡 WARNINGS:")
            for warning in self.warnings:
                lines.append(f"  • {warning}")
            lines.append("")

        # Info
        if self.info:
            lines.append("INFO:")
            for info in self.info:
                lines.append(f"  • {info}")
            lines.append("")

        return "\n".join(lines)
