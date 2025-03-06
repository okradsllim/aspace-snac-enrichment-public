# Dataset Preprocessing and Cleaning

*Last updated: February 22, 2025*

### Initial Assessment 

Following the conceptual groundwork laid out in `01_introduction_and_research.md`, I turned my attention to the actual dataset. My first task was understanding the structure of our spreadsheet containing 18,771 ArchivesSpace agent records. The file included six columns, though two lacked headers: the agent URI, sort name (often with birth/death dates), Library of Congress authority control URL, creator information, and what would later be identified as SNAC ARKs and additional authorities.

I created `scan_csv.py` in the `src/data/` directory to help with the initial inspection, focusing on verifying column presence and basic data integrity. All scanning operations are logged to `logs/scan_csv.log` for reference and reproducibility.

### The Excel and Encoding Saga

During preprocessing, I worked through multiple approaches to ensure `snac_uris_outfile_cleaned.csv` was both properly formatted and free of mojibake issues. Initially, I considered writing a Python script (`fix_mojibake.py`) to explicitly read the original CSV in UTF-8, fix the encoding in the "sort_name" column, and overwrite the cleaned version. However, I discovered that Excel itself could correctly interpret the encoding when importing through Data → Get Data → From Text/CSV, making an additional script unnecessary. 

This process taught me something interesting about Excel's behavior: it handles encoding differently depending on how you open a CSV file. Double-clicking a CSV doesn't always respect UTF-8 encoding, but importing via Data → Get Data → From Text/CSV ensures correct handling by default 😅. 

After fixing the encoding at the input stage, I encountered another quirk: when `clean_csv.py` ran on this already UTF-8 encoded source, the output file still displayed mojibake when double-clicked in Excel. The solution turned out to be surprisingly simple - modifying a single line in `clean_csv.py`:

```python
# Before
df.to_csv(output_path, index=False)

# After
df.to_csv(output_path, index=False, encoding="utf-8-sig")
```

This change ensures the cleaned CSV is explicitly saved with a BOM (Byte Order Mark), which Excel recognizes when opening files directly.

### Processing Evolution

Through this process, my working files evolved into three versions:
1. `snac_uris_outfile_original.csv` – the untouched original for reference
2. `snac_uris_outfile_cleaned.csv` – the properly encoded, cleaned version used by my scripts
3. `snac_uris_outfile.xlsx` – a semi-original version that played its part but is no longer the primary working file

During script development, I also updated the code to handle a deprecation warning:

```python
# Initial approach using deprecated method
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Updated to use newer Pandas functionality
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
```

### Looking Forward

While I've made progress with the basic data structure and encoding issues, I've noted that the `additional_authorities` column requires special attention. Each cell in this column contains a list of varying length (but at least one member) of authority URIs/URLs. I've already noticed that over 200 cells contain ARKs linking to authority records managed by the Bibliothèque nationale de France. This complexity might warrant its own cleaning and validation process.The next phase will focus on API integration, but I may need to circle back for additional cleaning steps, particularly for these authority identifiers. The exact approach will become clearer as I dive deeper into how these identifiers will be used in the integration process.