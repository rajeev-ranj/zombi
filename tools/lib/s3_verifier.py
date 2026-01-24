"""
S3 Parquet Verifier

Utility for verifying Parquet files in S3/MinIO for cold storage tests.
"""

import io
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# Optional imports - graceful degradation if not available
try:
    import boto3
    from botocore.config import Config
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@dataclass
class ParquetFileInfo:
    """Information about a Parquet file in S3."""
    key: str
    size_bytes: int
    row_count: int = 0
    columns: List[str] = None
    min_sequence: Optional[int] = None
    max_sequence: Optional[int] = None


@dataclass
class VerificationResult:
    """Result of verifying S3 Parquet files."""
    success: bool
    total_files: int
    total_rows: int
    total_size_bytes: int
    files: List[ParquetFileInfo]
    errors: List[str]
    missing_sequences: List[int] = None


class S3ParquetVerifier:
    """
    Verifier for Parquet files in S3-compatible storage.

    Supports AWS S3 and MinIO for local testing.
    """

    def __init__(
        self,
        bucket: str,
        endpoint_url: Optional[str] = None,
        region: str = "us-east-1",
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
    ):
        """
        Initialize the S3 verifier.

        Args:
            bucket: S3 bucket name
            endpoint_url: Custom endpoint (for MinIO)
            region: AWS region
            access_key: AWS access key (or from env)
            secret_key: AWS secret key (or from env)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for S3 verification. "
                "Install with: pip install boto3"
            )

        self.bucket = bucket
        self.endpoint_url = endpoint_url

        # Get credentials from env if not provided
        access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY")

        # Create S3 client
        config = Config(
            connect_timeout=5,
            read_timeout=30,
            retries={"max_attempts": 3},
        )

        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=config,
        )

    def list_parquet_files(
        self,
        prefix: str = "",
        max_files: int = 1000,
    ) -> List[Dict]:
        """
        List Parquet files in the bucket.

        Args:
            prefix: Filter files by prefix
            max_files: Maximum number of files to return

        Returns:
            List of file info dicts with key and size
        """
        files = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    files.append({
                        "key": key,
                        "size_bytes": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    })
                    if len(files) >= max_files:
                        return files

        return files

    def read_parquet_file(self, key: str) -> Tuple[Optional["pq.ParquetFile"], Optional[str]]:
        """
        Read a Parquet file from S3.

        Args:
            key: S3 object key

        Returns:
            (ParquetFile, error_message)
        """
        if not PYARROW_AVAILABLE:
            return None, "pyarrow is required for reading Parquet files"

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = response["Body"].read()
            buffer = io.BytesIO(data)
            pf = pq.ParquetFile(buffer)
            return pf, None
        except Exception as e:
            return None, str(e)

    def get_file_info(self, key: str) -> Tuple[Optional[ParquetFileInfo], Optional[str]]:
        """
        Get detailed information about a Parquet file.

        Args:
            key: S3 object key

        Returns:
            (ParquetFileInfo, error_message)
        """
        pf, error = self.read_parquet_file(key)
        if error:
            return None, error

        try:
            metadata = pf.metadata
            schema = pf.schema_arrow

            # Get file size
            head = self.s3.head_object(Bucket=self.bucket, Key=key)
            size_bytes = head["ContentLength"]

            # Read to get sequence range if available
            table = pf.read()
            min_seq = None
            max_seq = None

            if "sequence" in table.column_names:
                seq_col = table.column("sequence")
                import pyarrow.compute as pc
                min_seq = pc.min(seq_col).as_py()
                max_seq = pc.max(seq_col).as_py()

            return ParquetFileInfo(
                key=key,
                size_bytes=size_bytes,
                row_count=metadata.num_rows,
                columns=schema.names,
                min_sequence=min_seq,
                max_sequence=max_seq,
            ), None
        except Exception as e:
            return None, str(e)

    def count_total_rows(
        self,
        prefix: str = "",
        max_files: int = 1000,
    ) -> Tuple[int, int, List[str]]:
        """
        Count total rows across all Parquet files.

        Args:
            prefix: Filter files by prefix
            max_files: Maximum files to process

        Returns:
            (total_rows, total_files, errors)
        """
        files = self.list_parquet_files(prefix, max_files)
        total_rows = 0
        errors = []

        for f in files:
            pf, error = self.read_parquet_file(f["key"])
            if error:
                errors.append(f"Error reading {f['key']}: {error}")
                continue
            total_rows += pf.metadata.num_rows

        return total_rows, len(files), errors

    def verify_data(
        self,
        prefix: str = "",
        expected_rows: Optional[int] = None,
        expected_sequence_range: Optional[Tuple[int, int]] = None,
        max_files: int = 1000,
    ) -> VerificationResult:
        """
        Verify Parquet data in S3 matches expectations.

        Args:
            prefix: Filter files by prefix
            expected_rows: Expected total row count (optional)
            expected_sequence_range: Expected (min, max) sequence (optional)
            max_files: Maximum files to process

        Returns:
            VerificationResult with details
        """
        files_info = []
        errors = []
        total_rows = 0
        total_size = 0
        all_sequences = set()

        files = self.list_parquet_files(prefix, max_files)

        for f in files:
            info, error = self.get_file_info(f["key"])
            if error:
                errors.append(f"Error reading {f['key']}: {error}")
                continue

            files_info.append(info)
            total_rows += info.row_count
            total_size += info.size_bytes

            # Collect sequences if available
            if info.min_sequence is not None and info.max_sequence is not None:
                for seq in range(info.min_sequence, info.max_sequence + 1):
                    all_sequences.add(seq)

        # Check expected rows
        success = True
        if expected_rows is not None:
            if total_rows < expected_rows:
                errors.append(
                    f"Row count mismatch: expected >= {expected_rows}, got {total_rows}"
                )
                success = False

        # Check sequence range
        missing_sequences = None
        if expected_sequence_range and all_sequences:
            expected_min, expected_max = expected_sequence_range
            expected_seqs = set(range(expected_min, expected_max + 1))
            missing = expected_seqs - all_sequences
            if missing:
                missing_sequences = sorted(list(missing))[:100]  # First 100
                errors.append(
                    f"Missing {len(missing)} sequences, first: {missing_sequences[:10]}"
                )
                success = False

        return VerificationResult(
            success=success and len(errors) == 0,
            total_files=len(files_info),
            total_rows=total_rows,
            total_size_bytes=total_size,
            files=files_info,
            errors=errors,
            missing_sequences=missing_sequences,
        )

    def wait_for_files(
        self,
        prefix: str = "",
        min_files: int = 1,
        min_rows: int = 0,
        timeout_secs: int = 120,
        poll_interval_secs: int = 5,
    ) -> Tuple[bool, str]:
        """
        Wait for Parquet files to appear in S3.

        Useful for waiting for flush to complete.

        Args:
            prefix: Filter files by prefix
            min_files: Minimum number of files expected
            min_rows: Minimum total rows expected
            timeout_secs: Maximum wait time
            poll_interval_secs: Time between checks

        Returns:
            (success, message)
        """
        import time

        start = time.time()
        while time.time() - start < timeout_secs:
            files = self.list_parquet_files(prefix)

            if len(files) >= min_files:
                if min_rows > 0:
                    total_rows, _, _ = self.count_total_rows(prefix)
                    if total_rows >= min_rows:
                        return True, f"Found {len(files)} files with {total_rows} rows"
                else:
                    return True, f"Found {len(files)} files"

            time.sleep(poll_interval_secs)

        files = self.list_parquet_files(prefix)
        return False, f"Timeout: found {len(files)} files after {timeout_secs}s"


def check_dependencies() -> Dict[str, bool]:
    """Check which optional dependencies are available."""
    return {
        "boto3": BOTO3_AVAILABLE,
        "pyarrow": PYARROW_AVAILABLE,
    }
