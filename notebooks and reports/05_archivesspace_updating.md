# ArchivesSpace Agent Update Process

## Introduction

This notebook documents the implementation and results of Phase 2 (Test Environment Implementation) of the ASpace-SNAC Enrichment Project. Following the successful completion of Phase 1 (Data Preparation & Analysis), I executed the batch update process to add SNAC ARK identifiers to ArchivesSpace agent records in the test environment.

This document covers the technical approach, implementation details, results, verification process, and key insights gained during this critical phase of the project.

## Update Implementation

### Technical Approach

I implemented the update process by adding SNAC ARKs as non-primary identifiers in the `agent_record_identifiers` array within each ArchivesSpace agent record. The update script (`src/api/update_aspace.py`) performs the following operations:

1. Reads the master CSV file with verified SNAC ARKs
2. Filters out records with known errors
3. Retrieves each agent record from ArchivesSpace via API
4. Checks if the record already has a SNAC ARK
5. If not, adds the SNAC ARK as a new identifier
6. Sends the updated record back to ArchivesSpace
7. Logs the results of each operation

The script employs batch processing and concurrent workers to optimize performance while maintaining stability.

### Update Method

For each agent record, the SNAC ARK was added using this structure:

```json
{
  "primary_identifier": false,
  "record_identifier": "[SNAC ARK URL]",
  "source": "snac",
  "jsonmodel_type": "agent_record_identifier"
}
```

This approach ensures the SNAC identifier is properly integrated into the ArchivesSpace data model, making it available through both the API and web interface.

### Implementation Details

- **Batch size**: 20 records per batch
- **Worker threads**: 8 concurrent workers
- **Process Timeline**: Approximately 6.5 hours (March 4, 2025)
- **Processing rate**: ~48 records per minute
- **Retry logic**: Up to 3 attempts per record with exponential backoff
- **Monitoring**: Real-time progress tracking via `check_progress.sh`

## Results

### Update Statistics

I achieved the following results from the batch update process:

| Category | Count | Percentage |
|----------|-------|------------|
| Total records in master CSV | 18,771 | 100% |
| Records successfully updated | 8,519 | 45.38% |
| Records skipped (already had ARK) | 10,119 | 53.91% |
| Records failed during update | 2 | 0.01% |
| Records with missing status | 131 | 0.70% |

### Record Type Breakdown

The processed records fell into these categories:

- People: 16,360 (87.15%)
- Corporate entities: 2,411 (12.85%)
- Families: 0 (0%)

### Failed Updates Analysis

Only two records failed to update, both receiving 400 Bad Request errors:

1. **Virgil** (ID: 69398)
   - ASpace URI: `/agents/people/69398`
   - LOC URI: `http://id.loc.gov/authorities/names/n79014062`
   - SNAC ARK: `http://n2t.net/ark:/99166/w6tr8912`
   - Web URL: [View in ArchivesSpace](https://testarchivesspace.library.yale.edu/agents/agent_person/69398)

2. **Isidore of Seville** (ID: 93678)
   - ASpace URI: `/agents/people/93678`
   - LOC URI: `http://id.loc.gov/authorities/names/n80139470`
   - SNAC ARK: `http://n2t.net/ark:/99166/w6d50skd`
   - Web URL: [View in ArchivesSpace](https://testarchivesspace.library.yale.edu/agents/agent_person/93678)

These two records appear to have unusually complex data structures that may have caused the API to reject the updates.

### Records Not Processed

131 records (0.70%) were not processed during the update. These records:
- All have SNAC ARKs assigned
- Consist of 126 people and 5 corporate entities
- Were likely excluded because they were identified as problematic during Phase 1

These records are documented in `src/data/records_missing_status.csv` for further investigation.

## Verification Process

### Initial Verification Issue

During verification, I encountered a discrepancy that initially caused concern. The verification script was searching for SNAC ARKs in a non-existent `external_ids` array rather than in the correct `agent_record_identifiers` array where they were actually added.

### Verification Solution

Upon examination of successfully updated records, I confirmed that SNAC ARKs were properly added to the `agent_record_identifiers` array. For example, in the record for Georges Clemenceau (`/agents/people/77764`):

```json
"agent_record_identifiers": [
  {
    "primary_identifier": true,
    "record_identifier": "http://id.loc.gov/authorities/names/n79096737",
    "source": "naf",
    "jsonmodel_type": "agent_record_identifier"
  },
  {
    "primary_identifier": false,
    "record_identifier": "http://n2t.net/ark:/99166/w6rv0rjj",
    "source": "snac",
    "jsonmodel_type": "agent_record_identifier"
  }
]
```

This confirmed that the update process was working correctly, and the ARKs were being added in the appropriate location within the ArchivesSpace data model.

### Multi-level Verification

I implemented a comprehensive verification approach:
1. **API response checks**: Confirming ARKs appear in the correct JSON structure
2. **Web interface verification**: Ensuring ARKs display correctly in the ArchivesSpace UI
3. **Sample verification**: Testing random records across the dataset

## Key Insights

Through this implementation phase, I gained several valuable insights:

### API vs. UI Display Considerations

The ArchivesSpace API responses and user interface displays can differ in subtle ways. There's also a lag between when a record is updated via API and when ArchivesSpace faceting will reflect that update, even when the UI display of the individual record shows the update immediately.

### Progress Monitoring Importance

For long-running processes, effective progress monitoring is essential. The `check_progress.sh` script I developed provided real-time insights into the update process, helping to:
- Track completion rate
- Identify potential issues early
- Estimate remaining time
- Provide confidence in the process

### Data Structure Knowledge

Understanding ArchivesSpace's data model proved crucial for successful implementation. Specifically, knowing how identifiers are stored (`agent_record_identifiers` vs. potential alternatives) ensured the updates were correctly structured and integrated.

### Verification Complexity

Verification requires checking both API responses and the web interface. My initial verification approach demonstrated that assumptions about data structure can lead to false concerns if not properly validated against the actual system implementation.

## Conclusion & Next Steps

Phase 2 (Test Environment Implementation) has been overwhelmingly successful, with 99.98% of attempted updates completed successfully. The SNAC ARK enrichment is now visible in both the API and web interface for 18,638 records, significantly enhancing the linked data connections between ArchivesSpace and SNAC.

In accordance with the project plan outlined in the companion document, the next steps are:

1. **Complete Phase 2**
   - Address the 2 failed records (Virgil and Isidore of Seville)
   - Investigate the 131 unprocessed records
   - Finalize test environment verification

2. **Initiate Phase 3 (Production Implementation)**
   - Develop production migration plan with rollback procedures
   - Establish update schedule and communication strategy
   - Prepare for the transition from test to production environment

This notebook will serve as a reference for both the completion of Phase 2 and the planning for Phase 3, ensuring a smooth transition to the production implementation of the SNAC ARK enrichment project.