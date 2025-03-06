# SNAC ID Integration Project: Research & Development

*Last updated: February 22, 2025*

### Project Context

When my boss assigned this ticket to me—-my first ticket as a new metadata specialist joining the team-—she explained it so clearly and even supplied sample code she'd written in the past that I could adapt. I initially thought I'd be done within a few days 😅. To be honest, I was so focused on what seemed like a straightforward end goal that I hadn't paid much attention to how the current structure and content of the 5-year-old spreadsheet would affect the timeline. I probably vibed a bit too comfortably with how she'd approached it as a more experienced person with all the proper instincts, without considering that my own intuition for estimating work, research, and attention to detail was more eager than discerning at this point.

After taking a closer look at the data (applying preliminary basic Excel commands and formulas), I came to my senses! The good thing is that right from the beginning, the coding part has consistently not been the main challenge. Before getting this job, I'd spent months perusing and documenting her publicly available and archived scripts for the kind of ETL workflows this job entails, so I've felt fairly comfortable in that aspect. In my dev experience, I've noticed that the task of writing the code itself is often not the hard part—it's the tasks of truly understanding the problem, the domain, the research and decision-making to ensure the best approach which can be more daunting. But I digress.

### Project Overview

The Lux team has requested that SCMS add SNAC identifiers to ArchivesSpace agent records. While our immediate focus is on processing an existing spreadsheet from 2020 containing 18,771 SNAC IDs associated with ArchivesSpace agent URIs, the ultimate aim is to handle our entire agent records collection. Through faceting via the staff interface via my web browser, I discovered we have:
- 54,883 person agent records
- 9,256 corporate entities records
- 783 family records
- 1 software record

Interestingly, only 2 records currently have SNAC information associated with them (also found through web browser faceting). These two were created on 2023-10-26 and modified about a year later, but they use actual SNAC IDs (non-resolvable) rather than SNAC ARKs (resolvable). Having just two records out of 50k+ makes this aspect manageable, but it raises some interesting questions about our agent record practices.

The current project consists of four main tasks:
1. Update 18,771 agent records with a new record identifier subrecord containing the SNAC ID
2. Run a report in ArchivesSpace identifying records with LCNAF IDs but no SNAC ID
3. Query SNAC API or Wikidata to retrieve SNAC IDs for records with LCNAF IDs but missing SNAC IDs
4. Update ArchivesSpace with the newly retrieved SNAC IDs

### Initial Data Exploration

I started by examining the provided spreadsheet containing ArchivesSpace agent data. The dataset includes 18,771 records with the following columns:
- `uri` – ArchivesSpace agent URI
- `sort_name` – Agent name, often with birth/death dates
- `authority_id` – Library of Congress authority control URL
- `created_by` – Indicates who created the record
- Two unnamed columns (later identified as `snac_arks` and `additional_authorities`)

Initial inspection revealed a couple formatting issues that would need to be addressed before proceeding with API integration. I have written more on these issues in the `02_preprocessing_and_cleaning.md` notebook.

### Conceptual Challenges: ARKs vs SNAC IDs

A deeper look at the dataset revealed a potential conceptual challenge that required additional research. The project description uses the term "SNAC ID," but I had taken that for granted. The spreadsheet contains what appear to be ARKs (Archival Resource Keys). I spent some more time time looking into the relationship between these identifiers, and how it would affect my project. 

#### Understanding SNAC IDs and ARKs

From my SNAC training with Jerry Simmons and co. summer of 2024, I recalled information about SNAC Constellation IDs, but needed to clarify their relationship with ARKs. This research led to an illuminating analogy. Within the SNAC ecosystem,:

- **SNAC Constellation IDs** are like Social Security Numbers - primary identifiers within the SNAC system
- **ARKs** are like passports - resolution mechanisms that point to those identifiers

#### ARK Structure Analysis

Some examination of the spreadsheet revealed an important pattern. Using a basic Control+F search across the "SNAC ARKs" column in Excel, I confirmed that:

1. All 18,000+ records shared the same pattern up to the final slash
2. Every ARK in the first unnamed column contained the string "99166"

This consistent pattern prompted further investigation into the meaning of "99166" and its relationship to SNAC.

### NAAN Investigation

My research into ARK structure led me to investigate the "99166" component:

1. **What is a NAAN?**
   - NAAN stands for Name Assigning Authority Number
   - It's a unique identifier assigned to organizations that create ARK identifiers

2. **Significance of "99166"**
   - This specific NAAN is reserved for "People, Groups, and Institutions as Agents"
   - It is not exclusively assigned to SNAC
   - It's used by multiple non-organizational entities

This discovery raised important questions about SNAC's institutional status and the uniqueness of the identifiers in our dataset.

### Implications for API Integration

The shared usage of NAAN "99166" has important implications for how I was going to approach the rest of my project:

1. **Query Precision**
   - If 99166 is shared, so that other organizations could use it too, then how does one find SNAC only 99166s in the wild? How could I ensure that my API queries would return only SNAC-specific results?
   - What additional parameters or validation steps are needed?
   - Can I be confident that while within SNAC, all ARKs that match with our ASpace agent records will have 99166?
   - How about those SNAC constellations which also include ARKs from BnF? Could they be in my way when I don't need them?

2. **Pattern Confirmation**
   - Having spent a good chunk of time looking at their API documentation, it turns out that all exemplifications about how to use the API where ARKs were a search parameter for targeting a SNAC Constellation (and where that parameter could be used in place the Constellation ID) had the 99166/xxx: suggesting to me that SNAC constellations which had ARKs as a primary key would contain 99166 NAAN, or that SNAC wouldn't use any other NAAN beside 99166.
   - But not all 99166 ARKs necessarily belong to SNAC
   - This requires careful validation during our integration process

I'm continuing to research the SNAC API to understand how ARKs are represented and whether we can confidently assert that "all SNAC ARKs will have 99166."

### Future Considerations

My investigation of the two existing SNAC records provides an opportunity to discuss agent record creation practices with my boss (Note to self: I still don't have permission to edit agent records via the staff interface). The non-resolvable integers used as authority IDs for these agents suggest that non-SNAC sources might also have non-resolvable identifiers—potentially spawning a new project on its own.

There's also the serial position effect to consider: at least one archivist has expressed that they'd prefer the "Source" selection list for adding/creating an agent in ASpace not to have SNAC appear towards the bottom. While I don't have strong feelings about this, I believe a broader discussion with the Archival Description Unit would be beneficial for establishing consistent practices.

### Implementation Plan

Based on the project requirements and initial research, I've developed the following tentative implementation plan:

1. **Data Preparation**
   - Clean and standardize the existing dataset
   - Set up proper validation for ARK identifiers

2. **ArchivesSpace Integration**
   - Develop scripts to query the ArchivesSpace API (`api/query_aspace.py`)
   - Create update routines for adding SNAC IDs to agent records

3. **SNAC ID Retrieval**
   - Research SNAC API endpoints and query patterns (`api/query_snac.py`)
   - Develop Wikidata query alternatives as backup

4. **Validation & Reporting**
   - Implement validation checks for data integrity
   - Create reports on updated records and any issues encountered

### Next Steps

The next phase of this project involves preprocessing and cleaning the dataset before API integration. This process is documented in the subsequent notebook, followed by technical exploration of the ArchivesSpace and SNAC APIs in later notebooks.