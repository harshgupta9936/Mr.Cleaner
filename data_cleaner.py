"""
Universal Data Cleaner
======================
Supports: CSV, Excel, TSV, JSON, JSONL, XML, YAML, TXT, MD, PDF, RTF
Operations: Missing values, duplicates, outliers, structural errors,
            noisy data smoothing, fuzzy matching, schema validation.

Install dependencies:
    pip install pandas numpy scipy scikit-learn openpyxl xlrd
                lxml pyyaml thefuzz python-Levenshtein pdfplumber
                striprtf jsonschema colorama tqdm

Usage:
    python data_cleaner.py --input data.csv --output cleaned.csv
    python data_cleaner.py --input data.xlsx --schema schema.json
    python data_cleaner.py --input doc.pdf --nlp
    python data_cleaner.py --input data.json --outlier iqr --impute median
"""

import os
import re
import sys
import json
import logging
import argparse
import warnings
from pathlib import Path
from typing import Any, Optional

warnings.filterwarnings("ignore")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("DataCleaner")

# ── Optional-import helpers ───────────────────────────────────────────────────
def _try_import(module: str, pip_name: str = ""):
    try:
        import importlib
        return importlib.import_module(module)
    except ImportError:
        pkg = pip_name or module
        log.warning("Optional package '%s' not found. Run: pip install %s", module, pkg)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  0. SMALL UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
_MISSING_LIKE = {
    "", "nan", "na", "n/a", "null", "nil", "undefined", "-", "--",
}


def _norm_key(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def _is_missing_like(v: Any) -> bool:
    if v is None:
        return True
    try:
        import pandas as pd
        if pd.isna(v):
            return True
    except Exception:
        pass
    if isinstance(v, float):
        try:
            import math
            if math.isnan(v):
                return True
        except Exception:
            pass
    s = _norm_key(v)
    return s in _MISSING_LIKE


_NUM_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
    "hundred": 100,
}


def _try_parse_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not _is_missing_like(v):
        try:
            return float(v)
        except Exception:
            return None
    s = _norm_key(v)
    if not s or s in _MISSING_LIKE:
        return None
    # common word cases from your samples ("two hundred", "thirty", "forty")
    if re.fullmatch(r"[a-z ]+", s):
        parts = s.split()
        if len(parts) == 1 and parts[0] in _NUM_WORDS:
            return float(_NUM_WORDS[parts[0]])
        if len(parts) == 2 and parts[0] in _NUM_WORDS and parts[1] == "hundred":
            return float(_NUM_WORDS[parts[0]] * 100)
    s2 = s.replace(",", "")
    s2 = re.sub(r"^\$+\s*", "", s2)
    s2 = s2.replace("k", "000") if re.fullmatch(r"\d+(\.\d+)?k", s2) else s2
    # keep digits, dot, minus
    s2 = re.sub(r"[^0-9.\-]", "", s2)
    if not s2 or s2 in {"-", ".", "-."}:
        return None
    try:
        return float(s2)
    except Exception:
        return None


def _extract_salary_range(v: Any) -> tuple[Optional[float], Optional[float]]:
    """
    From strings like "$137K-$171K (Glassdoor est.)" → (137000.0, 171000.0)
    """
    if v is None:
        return None, None
    s = str(v)
    s = s.replace(",", "")
    m = re.search(r"\$?\s*(\d+(?:\.\d+)?)\s*[kK]\s*-\s*\$?\s*(\d+(?:\.\d+)?)\s*[kK]", s)
    if not m:
        return None, None
    lo = float(m.group(1)) * 1000.0
    hi = float(m.group(2)) * 1000.0
    return lo, hi


# ═══════════════════════════════════════════════════════════════════════════════
#  1. FILE LOADER
# ═══════════════════════════════════════════════════════════════════════════════
class FileLoader:
    """Detect file type and return a pandas DataFrame or raw text."""

    STRUCTURED = {".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl",
                  ".ndjson", ".xml", ".yaml", ".yml"}
    TEXT_BASED  = {".txt", ".md", ".pdf", ".rtf"}

    @staticmethod
    def _extract_pdf_text(path: str) -> str:
        """
        Extract text from PDF with resilient fallbacks.
        Prioritize plain-flow extraction to avoid large visual spacing artifacts.
        """
        pages: list[str] = []

        pdfplumber = _try_import("pdfplumber", "pdfplumber")
        if pdfplumber is not None:
            try:
                with pdfplumber.open(path) as pdf:
                    for page in pdf.pages:
                        txt = ""
                        try:
                            txt = page.extract_text() or ""
                        except Exception:
                            try:
                                # Fallback for some PDFs where default extraction is poor.
                                txt = page.extract_text(layout=True) or ""
                            except Exception:
                                txt = ""
                        pages.append(txt)
            except Exception:
                pages = []

        text = "\n\n".join(p for p in pages if p and p.strip())
        if text.strip():
            return text

        # Fallback: pypdf
        pypdf = _try_import("pypdf", "pypdf")
        if pypdf is not None:
            try:
                from pypdf import PdfReader
                reader = PdfReader(path)
                chunks = []
                for page in reader.pages:
                    chunks.append(page.extract_text() or "")
                text = "\n\n".join(c for c in chunks if c and c.strip())
                if text.strip():
                    return text
            except Exception:
                pass

        raise ValueError("Could not extract text from PDF (possibly image-only or malformed PDF).")

    @staticmethod
    def _read_csv_loose(path: str):
        import pandas as pd
        # Try common encodings and delimiter sniffing (handles your IMDB ';' file)
        last_err = None
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
            try:
                df = pd.read_csv(path, sep=None, engine="python", encoding=enc)
                return df
            except Exception as e:
                last_err = e
                continue
        raise last_err  # noqa: B904

    @staticmethod
    def load(path: str) -> tuple:
        """Returns (dataframe_or_None, raw_text_or_None, file_type)."""
        import pandas as pd

        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {path}")

        ext = p.suffix.lower()
        log.info("Loading '%s'  [type=%s]", p.name, ext)

        # ── Structured formats ────────────────────────────────────────────────
        if ext == ".csv":
            df = FileLoader._read_csv_loose(path)
            return df, None, "structured"

        if ext == ".tsv":
            df = pd.read_csv(path, sep="\t")
            return df, None, "structured"

        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(path)
            return df, None, "structured"

        if ext == ".json":
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                # Try to find a list inside
                for v in data.values():
                    if isinstance(v, list):
                        df = pd.json_normalize(v)
                        break
                else:
                    df = pd.json_normalize([data])
            return df, None, "structured"

        if ext in (".jsonl", ".ndjson"):
            import pandas as pd
            df = pd.read_json(path, lines=True)
            return df, None, "structured"

        if ext == ".xml":
            lxml = _try_import("lxml", "lxml")
            if lxml is None:
                raise ImportError("Install lxml: pip install lxml")
            df = pd.read_xml(path)
            return df, None, "structured"

        if ext in (".yaml", ".yml"):
            yaml = _try_import("yaml", "pyyaml")
            if yaml is None:
                raise ImportError("Install pyyaml: pip install pyyaml")
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, list):
                df = pd.json_normalize(data)
            else:
                df = pd.json_normalize([data])
            return df, None, "structured"

        # ── Text / NLP formats ────────────────────────────────────────────────
        if ext in (".txt", ".md"):
            text = p.read_text(encoding="utf-8", errors="replace")
            return None, text, "text"

        if ext == ".pdf":
            text = FileLoader._extract_pdf_text(path)
            return None, text, "text"

        if ext == ".rtf":
            striprtf = _try_import("striprtf.striprtf", "striprtf")
            if striprtf is None:
                raise ImportError("Install striprtf: pip install striprtf")
            from striprtf.striprtf import rtf_to_text
            raw = p.read_text(encoding="utf-8", errors="replace")
            return None, rtf_to_text(raw), "text"

        raise ValueError(f"Unsupported file extension: '{ext}'")


# ═══════════════════════════════════════════════════════════════════════════════
#  2. SCHEMA VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════
class SchemaValidator:
    """
    Validates a DataFrame against a JSON Schema-like spec.

    Schema format (JSON file):
    {
      "required_columns": ["id", "name", "age"],
      "column_types": {
        "id":   "int",
        "name": "str",
        "age":  "float"
      },
      "value_ranges": {
        "age": {"min": 0, "max": 120}
      },
      "allowed_values": {
        "gender": ["M", "F", "Other"]
      },
      "not_null": ["id", "name"]
    }
    """

    def __init__(self, schema_path: str):
        with open(schema_path, encoding="utf-8") as f:
            self.schema = json.load(f)

    def validate(self, df) -> list[str]:
        import pandas as pd
        errors = []

        # Required columns
        for col in self.schema.get("required_columns", []):
            if col not in df.columns:
                errors.append(f"MISSING COLUMN: '{col}'")

        # Column types
        for col, expected in self.schema.get("column_types", {}).items():
            if col not in df.columns:
                continue
            try:
                if expected == "int":
                    df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
                elif expected == "float":
                    df[col] = pd.to_numeric(df[col], errors="raise").astype(float)
                elif expected == "str":
                    df[col] = df[col].astype(str)
                elif expected == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="raise")
            except Exception as e:
                errors.append(f"TYPE ERROR [{col}]: cannot cast to {expected} — {e}")

        # Not-null checks
        for col in self.schema.get("not_null", []):
            if col in df.columns and df[col].isnull().any():
                n = df[col].isnull().sum()
                errors.append(f"NULL VIOLATION [{col}]: {n} null values found")

        # Value ranges
        for col, rng in self.schema.get("value_ranges", {}).items():
            if col not in df.columns:
                continue
            num = pd.to_numeric(df[col], errors="coerce")
            if "min" in rng:
                bad = (num < rng["min"]).sum()
                if bad:
                    errors.append(f"RANGE VIOLATION [{col}]: {bad} values below min={rng['min']}")
            if "max" in rng:
                bad = (num > rng["max"]).sum()
                if bad:
                    errors.append(f"RANGE VIOLATION [{col}]: {bad} values above max={rng['max']}")

        # Allowed values
        for col, allowed in self.schema.get("allowed_values", {}).items():
            if col not in df.columns:
                continue
            bad = ~df[col].isin(allowed + [None])
            n = bad.sum()
            if n:
                errors.append(
                    f"ALLOWED-VALUE VIOLATION [{col}]: {n} unexpected values "
                    f"(allowed: {allowed})"
                )

        return errors


# ═══════════════════════════════════════════════════════════════════════════════
#  3. STRUCTURED DATA CLEANER
# ═══════════════════════════════════════════════════════════════════════════════
class StructuredCleaner:
    """Full cleaning pipeline for tabular DataFrames."""

    def __init__(self, cfg: dict):
        self.cfg = cfg

    @staticmethod
    def normalize_column_name(name: str) -> str:
        """Match the same normalization used in fix_structural_errors for headers."""
        import pandas as pd
        return (
            pd.Series([str(name)])
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-\.]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
            .iloc[0]
        )

    @staticmethod
    def _blanks_to_na(df):
        """Treat empty strings and common null tokens as missing (before drop/impute)."""
        import pandas as pd
        # Do not treat the word "none" as missing — it is a valid category in many datasets.
        null_like = {"", "nan", "na", "n/a", "null", "nil", "#n/a", "#na", "nat", "-", "--"}
        for col in df.columns:
            s = df[col]
            if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                continue
            strv = s.astype("string")
            stripped = strv.str.strip()
            mask = stripped.isin(null_like) | stripped.str.lower().isin(null_like) | (stripped == "")
            df.loc[mask, col] = pd.NA
        return df

    # ── 3.1 Missing Values ────────────────────────────────────────────────────
    def handle_missing(self, df):
        import pandas as pd
        import numpy as np
        strategy = self.cfg.get("missing", "impute")          # drop_rows | drop_cols | impute | none
        impute_method = self.cfg.get("impute", "mean")        # mean | median | mode | none

        df = self._blanks_to_na(df)

        before = df.isnull().sum().sum()
        log.info("Missing values before: %d", before)

        if strategy == "drop_rows":
            df = df.dropna(how="any")
        elif strategy == "drop_cols":
            threshold = float(self.cfg.get("drop_col_threshold", 0.5))
            n = len(df)
            if n == 0:
                return df
            # Drop columns where null fraction is STRICTLY greater than threshold (>50% empty)
            min_non_null = int(np.floor(n * (1.0 - threshold)) + 1)
            df = df.dropna(axis=1, thresh=min_non_null)
        else:  # impute
            for col in df.columns:
                if df[col].isnull().sum() == 0:
                    continue
                s = df[col]
                if impute_method in ("mean", "median") and not pd.api.types.is_numeric_dtype(s):
                    test = pd.to_numeric(s, errors="coerce")
                    if test.notna().mean() > 0.85:
                        s = test
                        df[col] = s
                if pd.api.types.is_numeric_dtype(df[col]):
                    if impute_method == "mean":
                        df[col] = df[col].fillna(df[col].mean())
                    elif impute_method == "median":
                        df[col] = df[col].fillna(df[col].median())
                    else:
                        df[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else 0)
                else:
                    # Safe mode for mixed/unorderable object values (lists/dicts/strings)
                    def _stable_key(v):
                        if v is None:
                            return "__none__"
                        try:
                            if pd.isna(v):
                                return "__na__"
                        except Exception:
                            pass
                        if isinstance(v, (list, tuple, set, dict)):
                            try:
                                return json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
                            except Exception:
                                return str(v)
                        return str(v)

                    counts = {}
                    rep = {}
                    for v in df[col].dropna().tolist():
                        k = _stable_key(v)
                        counts[k] = counts.get(k, 0) + 1
                        if k not in rep:
                            rep[k] = v
                    if counts:
                        best_k = max(counts.keys(), key=lambda k: counts[k])
                        fill = rep.get(best_k, "UNKNOWN")
                    else:
                        fill = "UNKNOWN"
                    df[col] = df[col].fillna(fill)

        after = df.isnull().sum().sum()
        log.info("Missing values after:  %d  (removed %d)", after, before - after)
        return df

    # ── 3.2 Duplicates ───────────────────────────────────────────────────────
    def remove_duplicates(self, df):
        """Drop rows that are identical across every column (pandas ``drop_duplicates()``)."""
        import pandas as pd

        before = len(df)
        try:
            df2 = df.drop_duplicates()
        except TypeError:
            # Some datasets (esp. JSON-normalized) can contain unhashable cells (list/dict/set).
            # Build a stable, stringified key frame for duplicate detection.
            def _stable_cell(v):
                if v is None:
                    return None
                try:
                    if pd.isna(v):
                        return v
                except Exception:
                    pass
                if isinstance(v, (list, tuple, set, dict)):
                    try:
                        return json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
                    except Exception:
                        return str(v)
                return v

            key = df.copy()
            for col in key.columns:
                s = key[col]
                if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
                    key[col] = s.map(_stable_cell)
            mask_dup = key.duplicated(keep="first")
            df2 = df.loc[~mask_dup].copy()

        removed = before - len(df2)
        log.info("Duplicates removed: %d", removed)
        return df2

    # ── 3.3 Outlier Treatment ─────────────────────────────────────────────────
    def treat_outliers(self, df):
        import pandas as pd
        import numpy as np

        method   = self.cfg.get("outlier", "iqr")     # iqr | zscore | none
        action   = self.cfg.get("outlier_action", "clip")  # clip | remove | winsorize
        z_thresh = self.cfg.get("z_threshold", 3.0)

        # ── Bug fix: early-return when detection is disabled ──────────────
        if method == "none":
            log.info("Outlier detection disabled — skipping")
            return df

        numeric_cols = df.select_dtypes(include=[np.number]).columns
        total_flagged = 0

        for col in numeric_cols:
            series = df[col].dropna()
            if len(series) < 4:
                continue

            if method == "zscore":
                from scipy import stats
                z = np.abs(stats.zscore(df[col].dropna()))
                mask = pd.Series(False, index=df.index)
                mask[df[col].dropna().index] = z > z_thresh

            else:  # IQR (default)
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                mask = (df[col] < lower) | (df[col] > upper)

            n_flagged = mask.sum()
            total_flagged += n_flagged

            if n_flagged == 0:
                continue

            if action == "remove":
                df = df[~mask]
            elif action == "winsorize":
                from scipy.stats.mstats import winsorize as scipy_winsorize
                df[col] = scipy_winsorize(df[col], limits=[0.05, 0.05])
            else:  # clip
                if method == "iqr":
                    df[col] = df[col].clip(lower=lower, upper=upper)
                else:
                    mean_val = df[col].mean()
                    std_val  = df[col].std()
                    df[col]  = df[col].clip(
                        lower=mean_val - z_thresh * std_val,
                        upper=mean_val + z_thresh * std_val,
                    )

        log.info("Outlier method=%s, action=%s — %d flagged across all columns",
                 method, action, total_flagged)
        return df

    # ── 3.4 Structural Errors ─────────────────────────────────────────────────
    def fix_structural_errors(self, df):
        import pandas as pd

        # Normalize column names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-\.]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )

        import re as _re
        _citation = _re.compile(r"\[[^\]]*\]")
        _footnote = _re.compile(r"[†‡\*]")

        for col in df.select_dtypes(include=["object", "string"]).columns:
            # Strip citation brackets [n] and footnote symbols before anything else
            df[col] = df[col].astype(str).apply(
                lambda v: _re.sub(_footnote, "",
                          _re.sub(_citation, "", v)).strip()
                if v != "nan" else float("nan")
            )
            # Strip whitespace & collapse spaces
            df[col] = df[col].str.strip()
            df[col] = df[col].str.replace(r"\s+", " ", regex=True)
            # NOTE: title-casing disabled — breaks proper nouns (MDNA, etc.)

        # Attempt auto-conversion of numeric strings
        for col in df.select_dtypes(include=["object", "string"]).columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().mean() > 0.8:           # >80% parseable → convert
                df[col] = converted

        # Attempt auto-conversion of datetime strings
        def _try_parse_dates(s: "pd.Series", dayfirst: bool):
            try:
                return pd.to_datetime(s, errors="coerce", dayfirst=dayfirst, utc=False)
            except Exception:
                return pd.to_datetime(pd.Series([pd.NA] * len(s)), errors="coerce")

        def _looks_date_like(txt: str) -> bool:
            t = (txt or "").strip()
            if not t:
                return False
            # Quick screen for common date separators / month names / ISO timestamps.
            if re.search(r"(\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4})", t):
                return True
            if re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b", t, re.I):
                return True
            if re.search(r"\d{4}-\d{2}-\d{2}t", t, re.I):
                return True
            return False

        OUT_FMT = "%d/%m/%Y"
        for col in df.select_dtypes(include=["object", "string"]).columns:
            # Only consider columns with enough date-like values
            sample_raw = df[col].dropna().astype(str).head(80)
            if sample_raw.empty:
                continue
            date_like_rate = sample_raw.map(_looks_date_like).mean()
            if date_like_rate < 0.35:
                continue

            # Try both dayfirst modes; pick the one that parses more values.
            dt0 = _try_parse_dates(df[col], dayfirst=False)
            dt1 = _try_parse_dates(df[col], dayfirst=True)
            r0 = float(dt0.notna().mean())
            r1 = float(dt1.notna().mean())
            dt = dt1 if r1 >= r0 else dt0
            r = max(r0, r1)

            # Only convert if we can parse a meaningful fraction.
            if r < 0.5:
                continue

            # Standardize output as dd/mm/yyyy strings (keeps human-readable dates across formats).
            df[col] = dt.dt.strftime(OUT_FMT).astype("string")

        log.info("Structural fixes applied (column names, whitespace, type coercion, dates)")
        return df

    def apply_global_string_transforms(self, df):
        """
        Optional whole-table string cell transforms from cfg:
          - lowercase_all: lower-case every string cell
          - strip_whitespace_all: remove all whitespace characters
          - remove_special_symbols: strip non-alphanumeric ASCII (keeps a-z, A-Z, 0-9)
        Applied to object / string columns only; order: lowercase → strip whitespace → remove symbols.
        """
        lower = bool(self.cfg.get("lowercase_all", False))
        strip_ws = bool(self.cfg.get("strip_whitespace_all", False))
        strip_sym = bool(self.cfg.get("remove_special_symbols", False))
        raw_excl = self.cfg.get("string_transform_exclude_columns") or []
        if not (lower or strip_ws or strip_sym):
            return df

        import pandas as pd
        import numpy as np

        exclude_norm = set()
        if isinstance(raw_excl, (list, tuple, set)):
            for v in raw_excl:
                if v is None:
                    continue
                exclude_norm.add(self.normalize_column_name(str(v)))

        ws_re = re.compile(r"\s+")
        # Keep spaces when removing symbols; whitespace is handled separately by strip_ws.
        sym_re = re.compile(r"[^a-zA-Z0-9\s]")

        def transform_cell(v):
            if v is None:
                return v
            try:
                if pd.isna(v):
                    return v
            except Exception:
                pass
            if not isinstance(v, (str, bytes)):
                try:
                    if isinstance(v, (float, int)) and not isinstance(v, bool):
                        if isinstance(v, float) and np.isnan(v):
                            return v
                except Exception:
                    pass
            s = str(v)
            if lower:
                s = s.lower()
            if strip_ws:
                s = ws_re.sub("", s)
            if strip_sym:
                s = sym_re.sub("", s)
            return s

        for col in df.columns:
            s = df[col]
            if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                continue
            if self.normalize_column_name(col) in exclude_norm:
                continue
            df[col] = s.map(transform_cell)
        log.info(
            "Global string transforms: lowercase=%s strip_whitespace=%s remove_special_symbols=%s",
            lower,
            strip_ws,
            strip_sym,
        )
        return df

    # ── 3.5 Fuzzy Matching / Typo Correction ─────────────────────────────────
    def fuzzy_correct(self, df, reference_map: Optional[dict] = None):
        """
        reference_map: {column_name: [canonical_value1, canonical_value2, ...]}
        For each string column in the map, replace values with the closest
        canonical match if similarity >= threshold.
        """
        thefuzz = _try_import("thefuzz.process", "thefuzz")
        if thefuzz is None:
            log.warning("Skipping fuzzy matching — install thefuzz")
            return df

        from thefuzz import process as fuzz_process

        threshold = self.cfg.get("fuzzy_threshold", 85)
        total_corrections = 0

        cols_to_check = reference_map or {}

        # Auto-detect high-cardinality string columns when no map given
        if not cols_to_check:
            import pandas as pd
            for col in df.select_dtypes(include=["object", "string"]).columns:
                unique_vals = df[col].dropna().unique()
                if 2 <= len(unique_vals) <= 50:
                    # Build self-referential canonical list from most-frequent values
                    top = df[col].value_counts().head(20).index.tolist()
                    cols_to_check[col] = top

        for col, canonical_list in cols_to_check.items():
            if col not in df.columns or not canonical_list:
                continue
            unique_vals = df[col].dropna().unique()
            correction_map = {}
            for val in unique_vals:
                if val in canonical_list:
                    continue
                match, score = fuzz_process.extractOne(str(val), [str(c) for c in canonical_list])
                if score >= threshold:
                    correction_map[val] = match
            if correction_map:
                df[col] = df[col].replace(correction_map)
                total_corrections += len(correction_map)
                log.info("  Fuzzy [%s]: %d corrections → %s", col, len(correction_map), correction_map)

        log.info("Fuzzy matching complete — total corrections: %d", total_corrections)
        return df

    # ── 3.5b Advanced manual cleaning rules ───────────────────────────────────
    def resolve_column(self, df, raw_name: str) -> Optional[str]:
        """Map user-supplied column label to actual column name after header normalization."""
        if raw_name is None or str(raw_name).strip() == "":
            return None
        key = self.normalize_column_name(str(raw_name))
        for c in df.columns:
            if self.normalize_column_name(c) == key:
                return c
        return None

    def drop_selected_columns(self, df):
        """
        Remove columns listed in cfg['drop_columns'] (list of names matching file headers).
        Runs after structural header normalization; names are resolved like other manual options.
        """
        names = self.cfg.get("drop_columns") or []
        if not names:
            return df
        if not isinstance(names, (list, tuple)):
            log.warning("drop_columns must be a list — skipping")
            return df

        to_drop = []
        for raw in names:
            col = self.resolve_column(df, raw)
            if col:
                to_drop.append(col)
            else:
                log.warning("drop_columns: no column matching '%s' — skipped", raw)

        to_drop = list(dict.fromkeys(to_drop))
        if not to_drop:
            return df

        keep = [c for c in df.columns if c not in set(to_drop)]
        df = df[keep].copy()
        log.info("Dropped %d column(s): %s", len(to_drop), to_drop)
        return df

    def apply_advanced_cleaning(self, df):
        """
        Optional rules in cfg['advanced_cleaning']: list of dicts, applied in order.

        Supported tasks:
          - numeric_cast / cast: { "task":"numeric_cast", "column":"...", "to":"int"|"float", "decimals": optional int }
          - string_to_number: parse string column to numeric (coerce invalid to NA)
          - number_to_string: format numeric column as strings with fixed decimals
          - clean_strings: replace or drop rows for listed token values (NULL, Unknown, etc.)
          - column_math: add / multiply / round numeric columns after coercion

        clean_strings:
          - "action": "replace" | "drop_rows"
          - "match": "exact" (default) — whole cell equals token; or "contains" — cell contains substring
          - "values": ["NULL", "Unknown", ...]
          - "replacement": "" or "__NA__" for pd.NA
          - "case_insensitive": true (default)
        """
        rules = self.cfg.get("advanced_cleaning")
        if not rules:
            return df
        if not isinstance(rules, list):
            log.warning("advanced_cleaning must be a list — skipping")
            return df

        import pandas as pd
        import numpy as np

        for i, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue
            col = self.resolve_column(df, rule.get("column", ""))
            if col is None:
                log.warning("Advanced rule #%d: column not found: %s", i, rule.get("column"))
                continue
            task = (rule.get("task") or rule.get("type") or "").lower()

            try:
                if task in ("numeric_cast", "cast", "cast_numeric"):
                    to = str(rule.get("to") or rule.get("target") or "float").lower()
                    decimals = rule.get("decimals")
                    num = pd.to_numeric(df[col], errors="coerce")
                    if to in ("int", "integer", "int64"):
                        df[col] = num.round(0).astype("Int64")
                    elif to in ("float", "double", "float64", "number"):
                        if decimals is not None:
                            d = int(decimals)
                            num = num.round(d)
                        df[col] = num.astype(float)
                    else:
                        log.warning("Unknown cast target '%s' for column '%s'", to, col)

                elif task in ("string_to_number", "string_column_to_numeric", "coerce_string_numeric"):
                    num = pd.to_numeric(df[col], errors="coerce")
                    df[col] = num

                elif task in ("number_to_string", "numeric_to_string", "numeric_column_to_string"):
                    d = int(rule.get("decimals", 2))

                    def _fmt_num(v):
                        if v is None:
                            return pd.NA
                        try:
                            if pd.isna(v):
                                return pd.NA
                        except Exception:
                            pass
                        try:
                            return f"{float(v):.{d}f}"
                        except (TypeError, ValueError):
                            return pd.NA

                    df[col] = df[col].map(_fmt_num).astype("string")

                elif task in ("clean_strings", "sanitize_strings", "string_bad_values"):
                    action = (rule.get("action") or "replace").lower()
                    values = rule.get("values") or []
                    if isinstance(values, str):
                        values = [v.strip() for v in values.split(",") if v.strip()]
                    if not values:
                        continue
                    ci = bool(rule.get("case_insensitive", True))
                    match_mode = (rule.get("match") or rule.get("mode") or "exact").lower()
                    repl = rule.get("replacement", "")
                    if repl is None or (isinstance(repl, str) and repl.upper() == "__NA__"):
                        repl_out = pd.NA
                    else:
                        repl_out = repl

                    s = df[col].astype("string")

                    bad_norm = []
                    for b in values:
                        bb = str(b).strip()
                        bad_norm.append(bb.lower() if ci else bb)

                    if match_mode in ("contains", "substring", "substr"):
                        if action == "drop_rows":
                            def row_contains_bad(x):
                                if x is None or (hasattr(pd, "isna") and pd.isna(x)):
                                    return False
                                tl = str(x).strip().lower() if ci else str(x).strip()
                                return any(frag in tl for frag in bad_norm)

                            mask = s.map(row_contains_bad)
                            df = df.loc[~mask].copy()
                        else:

                            def repl_cell_contains(x):
                                if x is None or (hasattr(pd, "isna") and pd.isna(x)):
                                    return x
                                tl = str(x).strip().lower() if ci else str(x).strip()
                                if any(frag in tl for frag in bad_norm):
                                    return repl_out
                                return x

                            df[col] = s.map(repl_cell_contains)
                    else:
                        bad_set = set(bad_norm)

                        if action == "drop_rows":
                            def is_bad_row(x):
                                if x is None or (hasattr(pd, "isna") and pd.isna(x)):
                                    return False
                                t = str(x).strip()
                                chk = t.lower() if ci else t
                                return chk in bad_set

                            mask = s.map(is_bad_row)
                            df = df.loc[~mask].copy()
                        else:

                            def repl_cell_exact(x):
                                if x is None or (hasattr(pd, "isna") and pd.isna(x)):
                                    return x
                                t = str(x).strip()
                                chk = t.lower() if ci else t
                                if chk in bad_set:
                                    return repl_out
                                return x

                            df[col] = s.map(repl_cell_exact)

                elif task in ("column_math", "column_arithmetic", "math_column"):
                    op = (rule.get("op") or rule.get("operation") or "").lower().replace(" ", "")
                    num = pd.to_numeric(df[col], errors="coerce")
                    if op in ("multiply", "mul", "*"):
                        factor = float(rule.get("by") or rule.get("factor") or 1.0)
                        df[col] = num * factor
                    elif op in ("divide", "div", "/"):
                        divisor = float(rule.get("by") or rule.get("divisor") or 1.0)
                        df[col] = num / divisor if divisor != 0 else num
                    elif op in ("add", "+"):
                        df[col] = num + float(rule.get("value", 0))
                    elif op in ("subtract", "sub", "-"):
                        df[col] = num - float(rule.get("value", 0))
                    elif op == "round":
                        d = int(rule.get("decimals", 2))
                        df[col] = num.round(d)
                    elif op in ("abs",):
                        df[col] = num.abs()
                    elif op in ("floor",):
                        df[col] = np.floor(num)
                    elif op in ("ceil",):
                        df[col] = np.ceil(num)
                    else:
                        log.warning("Unknown column_math op '%s' for column '%s'", op, col)

                else:
                    log.warning("Unknown advanced task '%s' — skipped", task)
            except Exception as e:
                log.warning("Advanced rule #%d on '%s' failed: %s", i, col, e)

        log.info("Advanced manual cleaning rules applied (%d rule(s))", len(rules))
        return df

    # ── 3.6 Noisy Data Smoothing ──────────────────────────────────────────────
    def smooth_noise(self, df):
        import pandas as pd
        import numpy as np

        method = self.cfg.get("smooth", "none")    # none | binning | rolling | regression
        if method == "none":
            return df

        numeric_cols = df.select_dtypes(include=[np.number]).columns

        # Columns that must never be smoothed: IDs, ordinals, raw counts
        SKIP_SMOOTH = set(self.cfg.get("skip_smooth_cols", []))
        # Auto-detect likely ID / ordinal columns (monotonic integers)
        for col in numeric_cols:
            s = df[col].dropna()
            if s.is_monotonic_increasing and (s == s.astype(int)).all():
                SKIP_SMOOTH.add(col)

        if method == "binning":
            bins = self.cfg.get("bins", 10)
            for col in numeric_cols:
                if col in SKIP_SMOOTH:
                    log.info("  Binning: skipping ordinal/ID column '%s'", col)
                    continue
                try:
                    binned = pd.cut(df[col], bins=bins, labels=False)
                    bin_means = df[col].groupby(binned).transform("mean")
                    df[col] = bin_means
                except Exception:
                    pass
            log.info("Smoothing: equal-width binning (%d bins)", bins)

        elif method == "rolling":
            window = self.cfg.get("window", 3)
            for col in numeric_cols:
                df[col] = df[col].rolling(window=window, min_periods=1, center=True).mean()
            log.info("Smoothing: rolling mean (window=%d)", window)

        elif method == "regression":
            from sklearn.linear_model import LinearRegression
            x_col = self.cfg.get("smooth_x_col")
            if x_col and x_col in df.columns:
                X = df[[x_col]].fillna(df[x_col].mean())
                for col in numeric_cols:
                    if col == x_col:
                        continue
                    y = df[col].fillna(df[col].mean())
                    lr = LinearRegression()
                    lr.fit(X, y)
                    df[col] = lr.predict(X)
                log.info("Smoothing: linear regression against '%s'", x_col)
            else:
                log.warning("Regression smoothing requires --smooth-xcol <column_name>")

        return df

    # ── 3.7 Manual column formats (optional) ─────────────────────────────────
    def apply_column_formats(self, df):
        """
        Apply per-column output types from cfg['column_formats']:
        { "column_name_or_label": "int"|"integer"|"float"|"str"|"string"|"datetime"|"date"|"bool"|"boolean"|"category" }
        Keys may be original labels; they are normalized the same way as dataframe headers.
        """
        fm = self.cfg.get("column_formats") or {}
        if not fm:
            return df
        import pandas as pd
        import numpy as np

        # map normalized name -> actual column in df
        canon_to_actual = {self.normalize_column_name(c): c for c in df.columns}

        for raw_key, fmt in fm.items():
            if fmt is None or (isinstance(fmt, str) and not str(fmt).strip()):
                continue
            key = self.normalize_column_name(str(raw_key))
            col = canon_to_actual.get(key)
            if col is None:
                log.warning("Column format: no column matching '%s' (normalized: '%s')", raw_key, key)
                continue
            f = str(fmt).strip().lower()
            try:
                if f in ("int", "integer", "int64"):
                    num = pd.to_numeric(df[col], errors="coerce")
                    df[col] = num.round(0).astype("Int64")
                elif f in ("float", "double", "float64", "number"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif f in ("str", "string", "text"):
                    df[col] = df[col].astype("string")
                elif f in ("datetime", "timestamp"):
                    # dayfirst=True parses DD/MM/YYYY consistently (common in café / EU exports).
                    dts = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    df[col] = dts.dt.strftime("%d/%m/%Y").astype("string")
                elif f == "date":
                    dts = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    df[col] = dts.dt.strftime("%d/%m/%Y").astype("string")
                elif f in ("bool", "boolean"):
                    true_set = {"1", "true", "t", "yes", "y"}
                    false_set = {"0", "false", "f", "no", "n"}

                    def _to_bool_cell(v):
                        if v is None:
                            return pd.NA
                        try:
                            if pd.isna(v):
                                return pd.NA
                        except Exception:
                            pass
                        t = str(v).strip().lower()
                        if t in true_set:
                            return True
                        if t in false_set:
                            return False
                        return pd.NA

                    df[col] = df[col].map(_to_bool_cell).astype("boolean")
                elif f in ("category", "categorical"):
                    df[col] = df[col].astype("category")
                else:
                    log.warning("Unknown column format '%s' for column '%s' — skipped", fmt, col)
            except Exception as e:
                log.warning("Could not apply format '%s' to column '%s': %s", fmt, col, e)

        log.info("Manual column formats applied where keys matched")
        return df

    # ── Full Pipeline ─────────────────────────────────────────────────────────
    def clean(self, df, reference_map=None):
        log.info("── Starting structured cleaning pipeline ──")
        # "none" = no automatic cleaning — only drop columns, advanced rules, column formats.
        if self.cfg.get("missing") == "none":
            log.info(
                "Missing strategy 'none': skipping imputation, structural fixes, fuzzy matching"
            )
            df = self.drop_selected_columns(df)
            df = self.apply_global_string_transforms(df)
            df = self.apply_advanced_cleaning(df)
            df = self.apply_column_formats(df)
            if self.cfg.get("drop_duplicate_rows", True):
                df = self.remove_duplicates(df)
            log.info("── Cleaning complete. Shape: %s ──", df.shape)
            return df
        df = self.handle_missing(df)
        if self.cfg.get("drop_duplicate_rows", True):
            df = self.remove_duplicates(df)
        df = self.fix_structural_errors(df)
        df = self.apply_global_string_transforms(df)
        df = self.drop_selected_columns(df)
        df = self.apply_advanced_cleaning(df)
        df = self.fuzzy_correct(df, reference_map)
        df = self.apply_column_formats(df)
        log.info("── Cleaning complete. Shape: %s ──", df.shape)
        return df


# ═══════════════════════════════════════════════════════════════════════════════
#  3B. PAIRED (LEARNED) CLEANING MODEL
# ═══════════════════════════════════════════════════════════════════════════════
class PairedCleaningModel:
    """
    Learns messy→cleaned transformations from paired CSVs and replays them.

    It is intentionally "example-driven" (fits what your cleaned files did),
    not a generic perfect cleaner.
    """

    def __init__(self, dataset_models: Optional[list[dict]] = None):
        self.dataset_models: list[dict] = dataset_models or []

    @staticmethod
    def _df_str(df):
        import pandas as pd
        out = df.copy()
        for c in out.columns:
            out[c] = out[c].astype("string")
        # normalize obvious missing tokens to <NA>
        out = out.replace(list(_MISSING_LIKE), pd.NA)
        out = out.replace({s.upper(): pd.NA for s in _MISSING_LIKE if s})
        return out

    @staticmethod
    def _canon_col(s: str) -> str:
        s = str(s).strip()
        s = re.sub(r"[\s\-\.]+", "_", s)
        s = re.sub(r"[^\w_]", "", s)
        return s.lower()

    @staticmethod
    def _infer_column_map(messy_cols: list[str], cleaned_cols: list[str]) -> dict[str, str]:
        """
        Map messy col → cleaned col using:
        - canonical exact match
        - position match (if same length)
        - fuzzy match as fallback
        """
        messy_can = {c: PairedCleaningModel._canon_col(c) for c in messy_cols}
        cleaned_can = {c: PairedCleaningModel._canon_col(c) for c in cleaned_cols}

        cleaned_by_can = {}
        for c, cc in cleaned_can.items():
            cleaned_by_can.setdefault(cc, []).append(c)

        mapping: dict[str, str] = {}

        # exact canonical matches
        for m in messy_cols:
            cc = messy_can[m]
            if cc in cleaned_by_can and len(cleaned_by_can[cc]) == 1:
                mapping[m] = cleaned_by_can[cc][0]

        # same-position fallback
        if len(messy_cols) == len(cleaned_cols):
            for i, m in enumerate(messy_cols):
                mapping.setdefault(m, cleaned_cols[i])

        # fuzzy fallback
        remaining_m = [m for m in messy_cols if m not in mapping]
        remaining_c = [c for c in cleaned_cols if c not in set(mapping.values())]
        if remaining_m and remaining_c:
            thefuzz = _try_import("thefuzz.process", "thefuzz")
            if thefuzz is not None:
                from thefuzz import process as fuzz_process
                for m in remaining_m:
                    best = fuzz_process.extractOne(PairedCleaningModel._canon_col(m),
                                                   [PairedCleaningModel._canon_col(c) for c in remaining_c])
                    if not best:
                        continue
                    match_can, score = best
                    if score >= 80:
                        # pick first cleaned column with that canonical name
                        for c in remaining_c:
                            if PairedCleaningModel._canon_col(c) == match_can:
                                mapping[m] = c
                                break

        return mapping

    @staticmethod
    def _learn_value_maps(messy_series, clean_series) -> dict:
        """
        Build:
        - exact_map: raw→clean (most frequent)
        - norm_map: normalized→clean (most frequent)
        - fill_missing: if messy missing-like maps to constant clean value
        """
        import pandas as pd
        from collections import Counter, defaultdict

        pairs = []
        for mv, cv in zip(messy_series.tolist(), clean_series.tolist()):
            pairs.append((mv, cv))

        exact_counter: dict[str, Counter] = defaultdict(Counter)
        norm_counter: dict[str, Counter] = defaultdict(Counter)
        to_missing_exact = Counter()
        to_missing_norm = Counter()
        missing_clean_counter = Counter()

        for mv, cv in pairs:
            if pd.isna(cv):
                # learn that some non-missing inputs should be blanked out
                if not _is_missing_like(mv):
                    mv_s = str(mv)
                    to_missing_exact[mv_s] += 1
                    to_missing_norm[_norm_key(mv_s)] += 1
                continue
            cv_s = str(cv)
            if _is_missing_like(mv):
                missing_clean_counter[cv_s] += 1
                continue
            mv_s = str(mv)
            exact_counter[mv_s][cv_s] += 1
            norm_counter[_norm_key(mv_s)][cv_s] += 1

        exact_map = {k: v.most_common(1)[0][0] for k, v in exact_counter.items() if v}
        norm_map = {k: v.most_common(1)[0][0] for k, v in norm_counter.items() if v}

        fill_missing = None
        if missing_clean_counter:
            fill_missing = missing_clean_counter.most_common(1)[0][0]

        return {
            "exact_map": exact_map,
            "norm_map": norm_map,
            "to_missing_exact": [k for k, n in to_missing_exact.items() if n >= 3],
            "to_missing_norm": [k for k, n in to_missing_norm.items() if n >= 3],
            "fill_missing": fill_missing,
        }

    def fit_pair(self, messy_path: str, cleaned_path: str, name: Optional[str] = None):
        import pandas as pd

        messy_df, _, t1 = FileLoader.load(messy_path)
        clean_df, _, t2 = FileLoader.load(cleaned_path)
        if t1 != "structured" or t2 != "structured":
            raise ValueError("fit_pair expects structured (CSV/Excel/JSON/...) inputs")

        messy_df = self._df_str(messy_df)
        clean_df = self._df_str(clean_df)

        # if row counts mismatch, learn only column-level transformations safely
        n = min(len(messy_df), len(clean_df))
        messy_df = messy_df.head(n)
        clean_df = clean_df.head(n)

        col_map = self._infer_column_map(list(messy_df.columns), list(clean_df.columns))

        # learn per-column maps for overlapping columns
        col_models: dict[str, dict] = {}
        for mcol, ccol in col_map.items():
            if ccol not in clean_df.columns or mcol not in messy_df.columns:
                continue
            col_models[ccol] = self._learn_value_maps(messy_df[mcol], clean_df[ccol])

        # derived columns: currently only salary min/max from "Salary Estimate"
        derived = {}
        if "Salary Estimate" in messy_df.columns and {"Min Salary", "Max Salary"}.issubset(set(clean_df.columns)):
            derived["Min Salary"] = {"type": "salary_range", "source": "Salary Estimate", "which": "min"}
            derived["Max Salary"] = {"type": "salary_range", "source": "Salary Estimate", "which": "max"}

        signature = sorted([self._canon_col(c) for c in messy_df.columns])
        self.dataset_models.append({
            "name": name or f"{Path(messy_path).stem}__to__{Path(cleaned_path).stem}",
            "signature": signature,
            "column_map": col_map,      # messy→cleaned (names)
            "column_models": col_models, # cleaned_col → maps learned from messy values
            "derived": derived,         # cleaned_col → derivation spec
            "cleaned_columns": list(clean_df.columns),
        })
        return self

    def save(self, path: str):
        payload = {"version": 1, "dataset_models": self.dataset_models}
        Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def load(path: str) -> "PairedCleaningModel":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return PairedCleaningModel(dataset_models=payload.get("dataset_models", []))

    def _select_dataset_model(self, df) -> Optional[dict]:
        sig = set([self._canon_col(c) for c in df.columns])
        best = None
        best_score = -1.0
        for m in self.dataset_models:
            ms = set(m.get("signature", []))
            if not ms:
                continue
            score = len(sig & ms) / max(1, len(ms))
            if score > best_score:
                best = m
                best_score = score
        if best_score < 0.4:
            return None
        return best

    def apply(self, input_path: str):
        import pandas as pd

        df, _, t = FileLoader.load(input_path)
        if t != "structured":
            raise ValueError("apply expects structured inputs")
        df = self._df_str(df)

        model = self._select_dataset_model(df)
        if model is None:
            # Fallback: run the heuristic cleaner in a conservative config
            log.warning("No learned dataset model matched this file; falling back to heuristic cleaner.")
            cfg = {
                "missing": "impute",
                "impute": "mode",
                "outlier": "none",
                "outlier_action": "clip",
                "z_threshold": 3.0,
                "smooth": "none",
                "bins": 10,
                "window": 3,
                "fuzzy_threshold": 90,
                "remove_punctuation": True,
                "lowercase": False,
                "remove_digits": False,
                "remove_stopwords": False,
                "lemmatize": False,
            }
            return StructuredCleaner(cfg).clean(df)

        # rename messy columns to cleaned column names (where mapped)
        rename = model.get("column_map", {})
        df2 = df.rename(columns=rename).copy()

        # ensure all cleaned columns exist
        out = pd.DataFrame(index=df2.index)
        for c in model.get("cleaned_columns", []):
            if c in df2.columns:
                out[c] = df2[c]
            else:
                out[c] = pd.NA

        # derived columns
        derived = model.get("derived", {})
        for col, spec in derived.items():
            if spec.get("type") == "salary_range":
                src = spec.get("source")
                which = spec.get("which")
                if src in df.columns:
                    vals = []
                    for v in df[src].tolist():
                        lo, hi = _extract_salary_range(v)
                        vals.append(lo if which == "min" else hi)
                    out[col] = vals

        # apply value maps per cleaned column
        for c, cm in (model.get("column_models") or {}).items():
            if c not in out.columns:
                continue
            exact_map = cm.get("exact_map", {})
            norm_map = cm.get("norm_map", {})
            to_missing_exact = set(cm.get("to_missing_exact", []) or [])
            to_missing_norm = set(cm.get("to_missing_norm", []) or [])
            fill_missing = cm.get("fill_missing", None)

            def _map_one(v):
                if _is_missing_like(v) or (hasattr(pd, "isna") and pd.isna(v)):
                    return fill_missing if fill_missing is not None else pd.NA
                s = str(v)
                if s in to_missing_exact:
                    return pd.NA
                if s in exact_map:
                    return exact_map[s]
                nk = _norm_key(s)
                if nk in to_missing_norm:
                    return pd.NA
                if nk in norm_map:
                    return norm_map[nk]
                # light generic normalization
                return s.strip()

            out[c] = out[c].map(_map_one)

        # numeric post-processing (keeps your cleaned files' numeric styles closer)
        for c in out.columns:
            # attempt numeric if mostly numeric (but avoid slash-formats like "140/90")
            s = out[c].astype("string")
            slash_rate = s.str.contains(r"/", regex=True, na=False).mean()
            if slash_rate < 0.05:
                parsed = out[c].map(_try_parse_number)
                if parsed.notna().mean() >= 0.85:
                    out[c] = pd.to_numeric(parsed, errors="coerce")

        return out


# ═══════════════════════════════════════════════════════════════════════════════
#  4. NLP / TEXT CLEANER
# ═══════════════════════════════════════════════════════════════════════════════
class TextCleaner:
    """Preprocesses raw text for NLP tasks."""

    MARKDOWN_PATTERNS = [
        (re.compile(r"#{1,6}\s*"),          ""),   # headings
        (re.compile(r"\*{1,2}(.+?)\*{1,2}"), r"\1"),  # bold / italic
        (re.compile(r"`{1,3}.*?`{1,3}", re.DOTALL), ""),  # code blocks
        (re.compile(r"!\[.*?\]\(.*?\)"),    ""),   # images
        (re.compile(r"\[(.+?)\]\(.*?\)"),   r"\1"),  # links
        (re.compile(r"---+"),               ""),   # horizontal rules
    ]

    @staticmethod
    def _get_wordnet_pos(treebank_tag: str):
        """
        Map Penn Treebank POS tags to WordNet POS tags.
        Defaults to noun when unknown.
        """
        if not treebank_tag:
            return "n"
        t = treebank_tag[0].upper()
        if t == "J":
            return "a"  # adjective
        if t == "V":
            return "v"  # verb
        if t == "R":
            return "r"  # adverb
        return "n"      # noun

    @staticmethod
    def _normalize_whitespace_preserve_layout(text: str) -> str:
        """
        Normalize noisy spacing while keeping line breaks and indentation shape.
        This avoids flattening PDF-extracted text into one long line.
        """
        text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
        out_lines = []
        for raw_line in text.split("\n"):
            line = raw_line.replace("\t", "    ")
            m = re.match(r"^\s*", line)
            lead = m.group(0) if m else ""
            body = line[len(lead):]
            body = re.sub(r"[^\S\n]+", " ", body).strip()
            out_lines.append((lead + body).rstrip() if body else "")
        return "\n".join(out_lines).strip("\n")

    def clean(self, text: str, cfg: dict) -> str:
        log.info("── Starting NLP text cleaning pipeline ──")
        original_len = len(text)
        pdf_document_mode = bool(cfg.get("pdf_document_mode", False))

        # Step 1: Strip Markdown syntax
        for pattern, replacement in self.MARKDOWN_PATTERNS:
            text = pattern.sub(replacement, text)

        # Step 2: Remove HTML tags (common in RTF/PDF artifacts)
        text = re.sub(r"<[^>]+>", " ", text)

        # Step 3+: Token-level NLP transforms.
        # In PDF document mode, keep text readable as a document and avoid destructive token ops.
        if not pdf_document_mode:
            # Step 3: Remove punctuation (optional, default on)
            if cfg.get("remove_punctuation", True):
                text = re.sub(r"[^\w\s]", " ", text)

        # Step 4: Lowercase
        if cfg.get("lowercase", True):
            text = text.lower()

        if not pdf_document_mode:
            # Step 5: Remove digits (optional)
            if cfg.get("remove_digits", False):
                text = re.sub(r"\d+", " ", text)

        # Step 6: Remove URLs
        text = re.sub(r"https?://\S+|www\.\S+", " ", text)

        # Step 7: Whitespace normalization
        # Keep layout for PDFs so indentation/line structure is not destroyed.
        if cfg.get("preserve_layout", False):
            text = self._normalize_whitespace_preserve_layout(text)
        else:
            text = re.sub(r"\s+", " ", text).strip()

        # Step 8: Stopword removal (optional, requires NLTK)
        if not pdf_document_mode and cfg.get("remove_stopwords", False):
            nltk = _try_import("nltk", "nltk")
            if nltk:
                try:
                    from nltk.corpus import stopwords
                    stop_words = set(stopwords.words("english"))
                    text = " ".join(w for w in text.split() if w not in stop_words)
                except LookupError:
                    try:
                        import nltk as _nltk
                        _nltk.download("stopwords", quiet=True)
                        from nltk.corpus import stopwords
                        stop_words = set(stopwords.words("english"))
                        text = " ".join(w for w in text.split() if w not in stop_words)
                    except Exception as e:
                        log.warning("NLTK stopwords unavailable; skipping stopword removal (%s)", e)
                except Exception as e:
                    log.warning("Stopword removal failed; skipping (%s)", e)

        # Step 9: Lemmatization (optional, POS-aware for better quality)
        if not pdf_document_mode and cfg.get("lemmatize", False):
            nltk = _try_import("nltk", "nltk")
            if nltk:
                try:
                    import nltk as _nltk
                    from nltk.stem import WordNetLemmatizer
                    from nltk import pos_tag
                    wnl = WordNetLemmatizer()
                    toks = text.split()
                    tagged = pos_tag(toks)
                    text = " ".join(
                        wnl.lemmatize(tok, pos=TextCleaner._get_wordnet_pos(tag))
                        for tok, tag in tagged
                    )
                except LookupError:
                    try:
                        import nltk as _nltk
                        _nltk.download("wordnet", quiet=True)
                        _nltk.download("omw-1.4", quiet=True)
                        _nltk.download("averaged_perceptron_tagger", quiet=True)
                        _nltk.download("averaged_perceptron_tagger_eng", quiet=True)
                        from nltk.stem import WordNetLemmatizer
                        from nltk import pos_tag
                        wnl = WordNetLemmatizer()
                        toks = text.split()
                        tagged = pos_tag(toks)
                        text = " ".join(
                            wnl.lemmatize(tok, pos=TextCleaner._get_wordnet_pos(tag))
                            for tok, tag in tagged
                        )
                    except Exception as e:
                        log.warning("NLTK wordnet unavailable; skipping lemmatization (%s)", e)
                except Exception as e:
                    log.warning("Lemmatization failed; skipping (%s)", e)

        log.info("Text cleaned: %d → %d chars", original_len, len(text))
        return text


# ═══════════════════════════════════════════════════════════════════════════════
#  5. OUTPUT WRITER
# ═══════════════════════════════════════════════════════════════════════════════
class OutputWriter:

    @staticmethod
    def write(df_or_text, output_path: str, file_type: str):
        p = Path(output_path)
        ext = p.suffix.lower()

        if file_type == "text":
            p.write_text(df_or_text, encoding="utf-8")
            log.info("Text written → %s", output_path)
            return

        df = df_or_text
        if ext == ".csv":
            df.to_csv(output_path, index=False)
        elif ext in (".xlsx", ".xls"):
            df.to_excel(output_path, index=False)
        elif ext == ".tsv":
            df.to_csv(output_path, sep="\t", index=False)
        elif ext == ".json":
            # Keep datetimes readable (avoid epoch milliseconds).
            df.to_json(output_path, orient="records", indent=2, date_format="iso")
        elif ext in (".jsonl", ".ndjson"):
            df.to_json(output_path, orient="records", lines=True, date_format="iso")
        elif ext in (".yaml", ".yml"):
            yaml = _try_import("yaml", "pyyaml")
            if yaml is None:
                raise ImportError("Install pyyaml")
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(df.to_dict(orient="records"), f, allow_unicode=True)
        else:
            # Default: CSV
            df.to_csv(output_path, index=False)

        log.info("DataFrame written → %s  (rows=%d, cols=%d)",
                 output_path, len(df), len(df.columns))


# ═══════════════════════════════════════════════════════════════════════════════
#  6. REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════
class ReportGenerator:

    @staticmethod
    def generate(df_before, df_after, validation_errors: list[str], output_path: str):
        import pandas as pd

        lines = [
            "=" * 60,
            "  DATA CLEANING REPORT",
            "=" * 60,
            "",
            "[ BEFORE CLEANING ]",
            f"  Rows      : {len(df_before)}",
            f"  Columns   : {len(df_before.columns)}",
            f"  Nulls     : {df_before.isnull().sum().sum()}",
            f"  Duplicates: {df_before.duplicated().sum()}",
            "",
            "[ AFTER CLEANING ]",
            f"  Rows      : {len(df_after)}",
            f"  Columns   : {len(df_after.columns)}",
            f"  Nulls     : {df_after.isnull().sum().sum()}",
            f"  Duplicates: {df_after.duplicated().sum()}",
            "",
        ]

        if validation_errors:
            lines += ["[ SCHEMA VALIDATION ERRORS ]"]
            for err in validation_errors:
                lines.append(f"  ✗ {err}")
            lines.append("")
        else:
            lines.append("[ SCHEMA VALIDATION ]  ✓ All checks passed")
            lines.append("")

        lines += [
            "[ COLUMN SUMMARY (after) ]",
            df_after.dtypes.to_string(),
            "",
            "[ NUMERIC STATS (after) ]",
            df_after.describe().to_string(),
            "",
            "=" * 60,
        ]

        report_text = "\n".join(lines)

        report_file = Path(output_path).with_suffix(".report.txt")
        report_file.write_text(report_text, encoding="utf-8")
        log.info("Report saved → %s", report_file)
        print("\n" + report_text)


# ═══════════════════════════════════════════════════════════════════════════════
#  7. CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(
        description="Universal Data Cleaner — CSV / Excel / JSON / XML / YAML / TXT / PDF / RTF",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    sub = p.add_subparsers(dest="command")

    # fit/apply for learned model
    p_fit = sub.add_parser("fit", help="Fit a learned cleaning model from messy→cleaned pairs")
    p_fit.add_argument("--pair", action="append", nargs=2, metavar=("MESSY", "CLEANED"),
                       help="Add a training pair: --pair messy.csv cleaned.csv (repeatable)")
    p_fit.add_argument("--model-out", required=True, help="Where to save the learned model JSON")

    p_apply = sub.add_parser("apply", help="Apply a learned cleaning model to a new file")
    p_apply.add_argument("--model", required=True, help="Path to learned model JSON")
    p_apply.add_argument("--input", required=True, help="Path to input file")
    p_apply.add_argument("--output", required=True, help="Path for cleaned output file (csv/xlsx/...)")

    # default command (backwards compatible): heuristic cleaner
    p_clean = sub.add_parser("clean", help="Run the heuristic cleaner (existing pipeline)")
    p_clean.add_argument("--input",   "-i", required=True,  help="Path to input file")
    p_clean.add_argument("--output",  "-o", default=None,   help="Path for cleaned output file")
    p_clean.add_argument("--schema",  "-s", default=None,   help="Path to JSON schema file for validation")
    # Missing values
    p_clean.add_argument("--missing", default="impute",
                   choices=["impute", "drop_rows", "drop_cols", "none"],
                   help="Strategy for missing values; 'none' runs only drop columns / advanced rules / formats (default: impute)")
    p_clean.add_argument("--impute",  default="mean",
                   choices=["mean", "median", "mode"],
                   help="Imputation method when --missing=impute (default: mean)")

    # Outliers
    p_clean.add_argument("--outlier", default="iqr",
                   choices=["iqr", "zscore", "none"],
                   help="Outlier detection method (default: iqr)")
    p_clean.add_argument("--outlier-action", default="clip",
                   choices=["clip", "remove", "winsorize"],
                   help="What to do with outliers (default: clip)")
    p_clean.add_argument("--z-threshold", type=float, default=3.0,
                   help="Z-score threshold (default: 3.0)")

    # Smoothing
    p_clean.add_argument("--smooth", default="none",
                   choices=["none", "binning", "rolling", "regression"],
                   help="Noise smoothing method (default: none)")
    p_clean.add_argument("--smooth-xcol", default=None,
                   help="X column for regression smoothing")
    p_clean.add_argument("--bins",   type=int,   default=10,  help="Number of bins (binning, default: 10)")
    p_clean.add_argument("--window", type=int,   default=3,   help="Rolling window size (default: 3)")

    # Fuzzy matching
    p_clean.add_argument("--fuzzy-threshold", type=int, default=85,
                   help="Fuzzy match similarity threshold 0-100 (default: 85)")
    p_clean.add_argument("--fuzzy-map", default=None,
                   help="JSON file mapping column→[canonical values] for fuzzy correction")

    p_clean.add_argument("--keep-duplicates", action="store_true",
                   help="Keep rows that are fully identical across all columns")

    # NLP flags (for text files)
    p_clean.add_argument("--nlp",                action="store_true", help="Force NLP text cleaning mode")
    p_clean.add_argument("--keep-punctuation",   action="store_true", help="[NLP] Keep punctuation")
    p_clean.add_argument("--no-lowercase",       action="store_true", help="[NLP] Skip lowercasing")
    p_clean.add_argument("--remove-digits",      action="store_true", help="[NLP] Strip digit tokens")
    p_clean.add_argument("--remove-stopwords",   action="store_true", help="[NLP] Remove stopwords (needs NLTK)")
    p_clean.add_argument("--lemmatize",          action="store_true", help="[NLP] Lemmatize words (needs NLTK)")

    # Misc
    p_clean.add_argument("--report", action="store_true", help="Generate cleaning report (.report.txt)")
    p_clean.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = p.parse_args()
    if args.command is None:
        # Backwards compatible: treat as heuristic clean command
        # by re-parsing as if "clean" was provided.
        args = p.parse_args(["clean", *sys.argv[1:]])
    return args


def main():
    args = parse_args()

    # ── Learned model commands ────────────────────────────────────────────────
    if args.command == "fit":
        if not args.pair:
            raise SystemExit("No training pairs provided. Use --pair messy.csv cleaned.csv (repeatable).")
        model = PairedCleaningModel()
        for messy, clean in args.pair:
            model.fit_pair(messy, clean)
        model.save(args.model_out)
        log.info("Learned model saved → %s  (datasets=%d)", args.model_out, len(model.dataset_models))
        return

    if args.command == "apply":
        model = PairedCleaningModel.load(args.model)
        out_df = model.apply(args.input)
        OutputWriter.write(out_df, args.output, "structured")
        log.info("Done ✓  →  %s", args.output)
        return

    # ── Heuristic cleaning command (existing behavior) ────────────────────────
    if args.verbose:
        log.setLevel(logging.DEBUG)

    # ── Build config dict ─────────────────────────────────────────────────────
    cfg = {
        "missing":        args.missing,
        "impute":         args.impute,
        "drop_duplicate_rows": not args.keep_duplicates,
        "outlier":        args.outlier,
        "outlier_action": args.outlier_action,
        "z_threshold":    args.z_threshold,
        "smooth":         args.smooth,
        "smooth_x_col":   args.smooth_xcol,
        "bins":           args.bins,
        "window":         args.window,
        "fuzzy_threshold":args.fuzzy_threshold,
        # NLP
        "remove_punctuation": not args.keep_punctuation,
        "lowercase":          not args.no_lowercase,
        "remove_digits":      args.remove_digits,
        "remove_stopwords":   args.remove_stopwords,
        "lemmatize":          args.lemmatize,
    }

    # ── Determine output path ─────────────────────────────────────────────────
    in_path = Path(args.input)
    if args.output:
        out_path = args.output
    else:
        out_path = str(in_path.parent / f"{in_path.stem}_cleaned{in_path.suffix}")

    # ── Load file ─────────────────────────────────────────────────────────────
    df, text, file_type = FileLoader.load(args.input)

    # ── Force NLP mode ────────────────────────────────────────────────────────
    if args.nlp and file_type == "structured":
        log.warning("--nlp flag set but file appears structured; cleaning as structured.")

    # ══════════════════════════════════════════════════════════════════════════
    #  TEXT / NLP PATH
    # ══════════════════════════════════════════════════════════════════════════
    if file_type == "text" or args.nlp:
        if text is None:
            log.error("No text content found.")
            sys.exit(1)

        cleaned_text = TextCleaner().clean(text, cfg)

        # Output
        text_out = out_path if out_path.endswith(".txt") else str(Path(out_path).with_suffix(".txt"))
        OutputWriter.write(cleaned_text, text_out, "text")
        log.info("Done ✓")
        return

    # ══════════════════════════════════════════════════════════════════════════
    #  STRUCTURED / TABULAR PATH
    # ══════════════════════════════════════════════════════════════════════════
    import copy
    df_before = df.copy()

    # Schema validation (before cleaning)
    validation_errors = []
    if args.schema:
        validator = SchemaValidator(args.schema)
        validation_errors = validator.validate(df)
        if validation_errors:
            log.warning("Schema validation found %d issue(s):", len(validation_errors))
            for err in validation_errors:
                log.warning("  %s", err)
        else:
            log.info("Schema validation: all checks passed ✓")

    # Fuzzy reference map
    reference_map = None
    if args.fuzzy_map:
        with open(args.fuzzy_map, encoding="utf-8") as f:
            reference_map = json.load(f)

    # Run cleaning pipeline
    cleaner = StructuredCleaner(cfg)
    df_clean = cleaner.clean(df, reference_map)

    # Write output
    OutputWriter.write(df_clean, out_path, "structured")

    # Report
    if args.report:
        ReportGenerator.generate(df_before, df_clean, validation_errors, out_path)

    log.info("Done ✓  →  %s", out_path)


if __name__ == "__main__":
    main()
