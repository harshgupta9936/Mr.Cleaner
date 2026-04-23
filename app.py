"""
Mr.Cleaner — Flask Web Server
=============================
Run:  python app.py
Open: http://localhost:5000
"""

import io
import os
import json
import tempfile
import traceback
from pathlib import Path

from flask import (
    Flask, request, jsonify, send_file,
    render_template,
)

# ── Import the cleaning engine ────────────────────────────────────────────────
from data_cleaner import FileLoader, StructuredCleaner, TextCleaner, OutputWriter

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB

# Allowed extensions
STRUCTURED = {".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl",
              ".ndjson", ".xml", ".yaml", ".yml"}
TEXT_EXT   = {".txt", ".md", ".pdf", ".rtf"}
ALLOWED    = STRUCTURED | TEXT_EXT

DOWNLOAD_TARGETS = {"nochange", "csv", "json", "txt"}

def _safe_duplicate_count(df) -> int:
    """
    df.duplicated() / drop_duplicates() can fail when cells are unhashable (lists/dicts).
    This counts duplicates using a stable, stringified key frame in that case.
    """
    import pandas as pd

    try:
        return int(df.duplicated().sum())
    except TypeError:
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
        return int(key.duplicated().sum())


def _parse_json_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value) if value.strip() else []
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _download_options_for_ext(source_ext: str) -> list[str]:
    e = (source_ext or "").lower()
    if e in {".csv", ".tsv", ".xls", ".xlsx"}:
        return ["nochange", "csv", "json"]
    if e == ".xml":
        return ["csv", "json"]
    if e in {".pdf", ".rtf", ".md", ".txt"}:
        return ["txt"]
    if e in {".json", ".jsonl", ".ndjson", ".yaml", ".yml"}:
        return ["nochange", "csv", "json"]
    return ["nochange"]


# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/inspect_columns", methods=["POST"])
def inspect_columns():
    """
    Accepts multipart/form-data:
      file – the data file
    Returns JSON with detected structured columns (best-effort).
    Used by the UI to enable checkbox-based column pickers for non-CSV formats too.
    """
    if "file" not in request.files:
        return jsonify(error="No file provided"), 400

    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify(error="Empty filename"), 400

    ext = Path(uploaded.filename).suffix.lower()
    if ext not in ALLOWED:
        return jsonify(error=f"Unsupported file type: '{ext}'"), 415

    with tempfile.TemporaryDirectory() as tmp:
        in_path = os.path.join(tmp, uploaded.filename)
        uploaded.save(in_path)
        try:
            df, _text, file_type = FileLoader.load(in_path)
        except Exception as e:
            return jsonify(error=f"Failed to load file: {e}"), 422

        if file_type != "structured" or df is None:
            return jsonify(columns=[]), 200

        try:
            cols = [str(c) for c in list(df.columns)]
        except Exception:
            cols = []
        return jsonify(columns=cols), 200

@app.route("/preview", methods=["POST"])
def preview():
    """
    Accepts multipart/form-data:
      file  – the data file
      rows  – optional int rows (default 50)
    Returns JSON preview for ALL supported formats.
    """
    if "file" not in request.files:
        return jsonify(error="No file provided"), 400

    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify(error="Empty filename"), 400

    ext = Path(uploaded.filename).suffix.lower()
    if ext not in ALLOWED:
        return jsonify(error=f"Unsupported file type: '{ext}'"), 415

    try:
        n_rows = int(request.form.get("rows", "50"))
        n_rows = max(1, min(400, n_rows))
    except Exception:
        n_rows = 50

    with tempfile.TemporaryDirectory() as tmp:
        in_path = os.path.join(tmp, uploaded.filename)
        uploaded.save(in_path)
        try:
            df, text, file_type = FileLoader.load(in_path)
        except Exception as e:
            return jsonify(error=f"Failed to load file: {e}"), 422

        if file_type == "text":
            snippet = (text or "")
            snippet = snippet[:20000]
            return jsonify(type="text", text=snippet), 200

        # structured
        import pandas as pd
        df2 = df.head(n_rows).copy() if df is not None else pd.DataFrame()

        # Ensure JSON-safe values (datetime -> ISO string)
        for c in df2.columns:
            if pd.api.types.is_datetime64_any_dtype(df2[c]):
                df2[c] = df2[c].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            else:
                # objects that are Timestamp-like but stored as object
                df2[c] = df2[c].map(lambda v: v.isoformat() if isinstance(v, (pd.Timestamp,)) else v)

        data = {
            "columns": [str(c) for c in list(df2.columns)],
            "rows": df2.astype(object).where(pd.notnull(df2), None).values.tolist(),
        }
        return jsonify(type="structured", **data), 200


@app.route("/download_options", methods=["POST"])
def download_options():
    """
    Returns allowed download targets for a file extension.
    Input: multipart/form-data with optional `ext` or `filename`.
    """
    ext = (request.form.get("ext") or "").strip().lower()
    if not ext:
        filename = (request.form.get("filename") or "").strip()
        ext = Path(filename).suffix.lower() if filename else ""
    if ext and not ext.startswith("."):
        ext = "." + ext
    return jsonify(options=_download_options_for_ext(ext), ext=ext), 200


@app.route("/convert_download", methods=["POST"])
def convert_download():
    """
    Convert current working file for download.
    Accepts multipart/form-data:
      file          - current working file
      target_format - one of: nochange, csv, json, txt
      source_ext    - source extension hint (e.g., .xml)
      source_name   - source filename hint
    """
    if "file" not in request.files:
        return jsonify(error="No file provided"), 400

    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify(error="Empty filename"), 400

    target = (request.form.get("target_format") or "").strip().lower()
    if target not in DOWNLOAD_TARGETS:
        return jsonify(error=f"Unsupported target format: {target}"), 400

    source_ext = (request.form.get("source_ext") or Path(uploaded.filename).suffix).strip().lower()
    if source_ext and not source_ext.startswith("."):
        source_ext = "." + source_ext
    allowed = _download_options_for_ext(source_ext)
    if target not in allowed:
        return jsonify(error=f"Target '{target}' not allowed for source '{source_ext}'"), 400

    source_name = (request.form.get("source_name") or uploaded.filename).strip() or uploaded.filename
    source_stem = Path(source_name).stem

    if target == "nochange":
        return send_file(
            io.BytesIO(uploaded.read()),
            as_attachment=True,
            download_name=source_name,
        )

    with tempfile.TemporaryDirectory() as tmp:
        in_path = os.path.join(tmp, uploaded.filename)
        uploaded.save(in_path)

        try:
            df, text, file_type = FileLoader.load(in_path)
        except Exception as e:
            return jsonify(error=f"Failed to load file for conversion: {e}"), 422

        if target == "txt":
            if file_type == "structured":
                try:
                    body = df.to_string(index=False) if df is not None else ""
                except Exception:
                    body = ""
            else:
                body = text or ""
            out_name = source_stem + ".txt"
            return send_file(
                io.BytesIO(body.encode("utf-8")),
                as_attachment=True,
                download_name=out_name,
                mimetype="text/plain",
            )

        if file_type != "structured" or df is None:
            return jsonify(error="CSV/JSON conversion requires structured data"), 422

        out_ext = ".csv" if target == "csv" else ".json"
        out_name = source_stem + out_ext
        out_path = os.path.join(tmp, out_name)
        OutputWriter.write(df, out_path, "structured")
        with open(out_path, "rb") as fh:
            file_bytes = fh.read()
        return send_file(
            io.BytesIO(file_bytes),
            as_attachment=True,
            download_name=out_name,
        )


# ─────────────────────────────────────────────────────────────────────────────
@app.route("/clean", methods=["POST"])
def clean():
    """
    Accepts multipart/form-data:
      file      – the data file
      options   – JSON string of cleaning options
    Returns the cleaned file as a download attachment.
    """
    if "file" not in request.files:
        return jsonify(error="No file provided"), 400

    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify(error="Empty filename"), 400

    ext = Path(uploaded.filename).suffix.lower()
    if ext not in ALLOWED:
        return jsonify(error=f"Unsupported file type: '{ext}'"), 415

    # ── Parse options ─────────────────────────────────────────────────────────
    raw_opts = request.form.get("options", "{}")
    try:
        opts = json.loads(raw_opts)
    except json.JSONDecodeError:
        opts = {}

    col_formats = opts.get("column_formats")
    if isinstance(col_formats, str):
        try:
            col_formats = json.loads(col_formats) if col_formats.strip() else {}
        except json.JSONDecodeError:
            col_formats = {}

    adv_clean = _parse_json_list(opts.get("advanced_cleaning", []))
    drop_cols = _parse_json_list(opts.get("drop_columns", []))
    exclude_cols = _parse_json_list(opts.get("string_transform_exclude_columns", []))

    drop_dup = opts.get("drop_duplicate_rows")
    if drop_dup is None:
        drop_dup = True
    else:
        drop_dup = bool(drop_dup)

    cfg = {
        "missing":         opts.get("missing",        "impute"),
        "impute":          opts.get("impute",         "mean"),
        "drop_duplicate_rows": drop_dup,
        "drop_col_threshold": float(opts.get("drop_col_threshold", 0.5)),
        "column_formats":  col_formats if isinstance(col_formats, dict) else {},
        "advanced_cleaning": adv_clean if isinstance(adv_clean, list) else [],
        "drop_columns": [str(x).strip() for x in drop_cols if str(x).strip()] if isinstance(drop_cols, list) else [],
        "fuzzy_threshold": int(opts.get("fuzzy_threshold", 85)),
        # Global string transforms (tabular)
        "lowercase_all":          bool(opts.get("lowercase_all", False)),
        "strip_whitespace_all":   bool(opts.get("strip_whitespace_all", False)),
        "remove_special_symbols": bool(opts.get("remove_special_symbols", False)),
        "string_transform_exclude_columns": [str(x).strip() for x in exclude_cols if str(x).strip()],
        # NLP
        "remove_punctuation": opts.get("remove_punctuation", True),
        "lowercase":          opts.get("lowercase",           True),
        "remove_digits":      opts.get("remove_digits",       False),
        "remove_stopwords":   opts.get("remove_stopwords",    False),
        "lemmatize":          opts.get("lemmatize",           False),
    }

    with tempfile.TemporaryDirectory() as tmp:
        in_path  = os.path.join(tmp, uploaded.filename)
        # Choose stable structured output extension.
        # - .xls writes are unreliable without extra engines -> emit .xlsx
        # - .xml write path is not implemented in OutputWriter -> emit .csv
        out_ext = ext
        if ext == ".xls":
            out_ext = ".xlsx"
        elif ext == ".xml":
            out_ext = ".csv"
        out_name = Path(uploaded.filename).stem + "_cleaned" + out_ext
        out_path = os.path.join(tmp, out_name)

        uploaded.save(in_path)

        # ── Load ─────────────────────────────────────────────────────────────
        try:
            df, text, file_type = FileLoader.load(in_path)
        except Exception as e:
            return jsonify(error=f"Failed to load file: {e}"), 422

        # ── Clean ─────────────────────────────────────────────────────────────
        try:
            if file_type == "text":
                # For PDF inputs, convert to TXT output for stable NLP processing and re-apply cycles.
                cfg["preserve_layout"] = (ext == ".pdf")
                cfg["pdf_document_mode"] = False
                cleaned_text = TextCleaner().clean(text, cfg)
                stem = Path(uploaded.filename).stem

                # Return cleaned text. For PDF inputs this intentionally converts to .txt.
                out_ext = ".txt" if ext == ".pdf" else ".txt"
                return send_file(
                    io.BytesIO(cleaned_text.encode("utf-8")),
                    as_attachment=True,
                    download_name=stem + "_cleaned" + out_ext,
                    mimetype="text/plain",
                )

            # Structured
            before_rows  = len(df)
            before_nulls = int(df.isnull().sum().sum())
            before_dups  = _safe_duplicate_count(df)

            cleaner  = StructuredCleaner(cfg)
            df_clean = cleaner.clean(df)

            after_rows  = len(df_clean)
            after_nulls = int(df_clean.isnull().sum().sum())
            after_dups  = _safe_duplicate_count(df_clean)

            OutputWriter.write(df_clean, out_path, "structured")

            # Read file into memory BEFORE temp dir is cleaned up
            # (send_file streams lazily — file would be gone by then)
            with open(out_path, "rb") as fh:
                file_bytes = fh.read()

            summary = json.dumps({
                "rows_before":   before_rows,
                "rows_after":    after_rows,
                "rows_removed":  before_rows - after_rows,
                "nulls_before":  before_nulls,
                "nulls_after":   after_nulls,
                "dups_removed":  before_dups - after_dups,
                "cols":          list(df_clean.columns),
            })

            response = send_file(
                io.BytesIO(file_bytes),
                as_attachment=True,
                download_name=out_name,
            )
            response.headers["X-Clean-Summary"] = summary
            return response

        except Exception:
            tb = traceback.format_exc()
            return jsonify(error="Cleaning failed", detail=tb), 500


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("templates", exist_ok=True)
    os.makedirs("static",    exist_ok=True)
    port = int(os.environ.get("PORT", 5000))
    print("\n  Mr.Cleaner Web App")
    print(f"  Open → http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
