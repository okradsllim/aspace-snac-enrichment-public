## ASpace-SNAC Enrichment Project

### Introduction

This repository contains the code, data, and documentation for an **ongoing project** to enrich Yale Library's ArchivesSpace agent records with SNAC ARK identifiers. The project broadly aims to enhance linked data connections between ArchivesSpace records and external authority IDs: its focus now is on Social Networks and Archival Context (SNAC).

### Repository Structure

```
ASPACE-SNAC-ENRICHMENT
├── logs/                          # Logs related to errors and processing
├── notebooks and reports/         # Documentation, analysis, and process notebooks
├── src/                           # Source code for data processing and API interaction
├── README.md                      # This file
├── requirements.txt               # Python dependencies
```

### Project Phases

The entire project has been broken down into 4 phases:

**Phase 1: Data Preparation & Analysis** - Cleaning and unifying legacy data from an old spreadsheet, developing schemas, and testing API integration.

**Phase 2: Test Environment Implementation** - Executing batch updates of agent records in the ArchivesSpace test environment.

**Phase 3: Production Implementation** - Migrating verified changes to the production environment with rollback procedures.

**Phase 4: Expansion & Enhancement** - Extending enrichment to all remaining agent records and incorporating additional authority identifiers.


### Project Status

Phase 2 has been completed with great results. Of the initial subset of 18,771 agent records (from ~65,000 total agents in Yale's ArchivesSpace), 18,638 were successfully processed, achieving a 99.98% success rate.

Key metrics from Phase 2:
- Total records processed: 18,771
- Records successfully updated with SNAC ARKs: 18,638 (99.29%)
- Records requiring further investigation: 133 (0.71%)

### Documentation

The project is extensively documented to facilitate understanding and future adaptation:

- **Notebooks and Reports**: The `notebooks and reports/` directory contains markdown notebooks documenting various aspects of the project, including research, data preprocessing, API exploration, and implementation results.

- **Code Documentation**: Code within the `src/` directory is written with ample commentary and documentation to explain the purpose and functionality of each script and module.

- **Logs**: Detailed logs in the `logs/` directory track all operations, errors, and process metrics for reference and troubleshooting.

### Contact

For questions about this project or metadata practices at the Beinecke Rare Book & Manuscript Library at Yale University, please contact [Will Nyarko](mailto:william.nyarko@yale.edu).