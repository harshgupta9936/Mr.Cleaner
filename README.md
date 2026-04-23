# Mr.Cleaner - Universal Data Cleaner

A dark-themed Flask web app and Python cleaning engine for messy datasets and documents.


Mr.Cleaner supports both:
- **Web UI workflow** (drag, clean, preview, download)
- **CLI workflow** (scriptable cleaning for pipelines)

---

## Live Demo

- **Production URL:** [https://mr-cleaner.onrender.com](https://mr-cleaner.onrender.com)
- Note: On free Render tier, first request after inactivity may take ~30-60 seconds.

---

## Screenshots

```md
![Home](screenshots/home.png)
![Cleaning Options](screenshots/Cleaning_options.png)
![Advanced and Column-wise Rules](screenshots/advanced_and_column_wise_rules.png)
![Preview and Results](screenshots/preview.png)
```

---

## Features

- Cleans **structured files**: CSV, TSV, XLS/XLSX, JSON/JSONL/NDJSON, XML, YAML
- Cleans **text files**: TXT, MD, PDF, RTF
- Missing-value handling (impute / drop rows / drop columns / manual-only mode)
- Duplicate row removal
- Structural normalization (headers, spacing, type coercion, date normalization)
- Global string transforms (lowercase, whitespace removal, special-character removal)
- Fuzzy correction for string values
- Advanced per-column rules:
  - cast numeric/string/datetime/bool/category
  - string-to-number and number-to-string conversion
  - clean or remove rows by bad token patterns
  - column math operations
- Interactive **column-wise find & replace** in UI
- Live file preview and flexible download format conversion

---

## Project Structure

```text
.
|- app.py                  # Flask web server + API endpoints
|- data_cleaner.py         # Core cleaning engine (structured + text pipelines)
|- templates/
|  `- index.html           # Full web UI (styles + behavior)
`- requirements.txt        # Python dependencies
```

---

## Requirements

- Python **3.10+** recommended
- pip

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Run Web App

```bash
python app.py
```

Then open:

- [http://localhost:5000](http://localhost:5000)

---

## Deploy Free (Render)

This project is ready for free deployment on Render.

### Manual Web Service setup (Render)
- **Build command:** `pip install -r requirements.txt`
- **Start command:** `gunicorn app:app`

### Remove local copy after deployment (optional)
After deployment is live and verified, you can safely remove your local folder and later re-clone from GitHub anytime.

---

## Web Workflow

1. Upload or drag a file.
2. Configure cleaning sections (missing values, advanced rules, column-wise actions, etc.).
3. Click **Apply Changes**.
4. Review preview/results.
5. Download in an allowed format.

---

## CLI Usage

The core cleaner can run directly from `data_cleaner.py`.

### Basic clean

```bash
python data_cleaner.py clean --input data.csv --output data_cleaned.csv
```

### With options

```bash
python data_cleaner.py clean --input data.csv --output out.csv --missing impute --impute median --outlier none
```

### Learned model mode

Fit a model from messy->clean examples:

```bash
python data_cleaner.py fit --pair messy.csv clean.csv --model-out model.json
```

Apply learned model:

```bash
python data_cleaner.py apply --model model.json --input new_data.csv --output cleaned.csv
```

---

## Supported Input Formats

- Structured: `.csv`, `.tsv`, `.xlsx`, `.xls`, `.json`, `.jsonl`, `.ndjson`, `.xml`, `.yaml`, `.yml`
- Text: `.txt`, `.md`, `.pdf`, `.rtf`

---

## Notes

- PDF cleaning is text-extraction based; scanned/image-only PDFs may not extract well.
- Some optional NLP steps (stopwords, lemmatization) require NLTK resources.
- XML output writing is not currently implemented directly; web flow converts XML outputs to CSV/JSON.

