# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a **non-code repository** containing user training materials and sample data for the Green Party's canvassing campaign in the Brent constituency. The software platform itself is "TTW Digital" (a third-party service) — this repository holds documentation and data for teams using that platform.

## Repository Contents

### Directory Structure

```
User Material - Canvassers and Action Day/   # Materials for frontline canvassers
User Material - Local Admins/                # Materials for campaign coordinators
```

### Material Types

- **PDF User Guides** (`UG C1`–`UG C5`, `UG A`): Step-by-step guides covering signup, party/user management, electoral register upload, canvassing rounds, and action days
- **MP4 Video Tutorials**: Screen recordings of the TTW Digital platform
- **CSV Data Templates**: Electoral register formats for uploading voter data into TTW Digital
- **CSV Test Datasets**: 1000-voter sample files for testing uploads (with/without election history)

## CSV Data Format

Electoral register CSVs follow a specific schema used by TTW Digital:

- **Core fields**: `Elector No. Prefix`, `Elector No.`, `Elector No. Suffix`, `Full Elector No.`, `Forename`, `Middle Names`, `Surname`, `Address1`–`Address6`, `PostCode`, `UPRN`, `Date of Attainment`
- **Per-election fields** (repeated per election year): `Green Voting Intention`, `Party`, `Voted`, `Postal Voter`
- **Voting intention codes**: `1`=Green, `2`=Conservative, `3`=Labour, `4`=LibDem, `5`=Other
- **Party codes**: `G`=Green, `Con`=Conservative, `Lab`=Labour, `LD`=LibDem, `REF`=Reform
- **Voted flag**: `v` indicates the elector voted in that election
