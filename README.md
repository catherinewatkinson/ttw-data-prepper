# TTW Data Prepper

Tools and materials for preparing council electoral register data for upload to [TTW Digital](https://ttwdigital.co.uk), the Green Party's canvassing platform.

## What's in this repo

### `tools/` — Python data-processing tools

A collection of scripts for cleaning, merging, and updating electoral register data:

- **Clean a council register** (names, addresses, postcodes, dates) into TTW upload format
- **Merge multiple registers** with canvassing/election data
- **Update an existing TTW app-export** with fresh canvassing data
- **Cross-check a membership list** against the register to find unregistered members
- **Validate** every output to ensure nothing was corrupted

All tools are pure Python (3.7+, standard library only — no dependencies).

**See [`tools/README.md`](tools/README.md) for full installation and usage instructions, including a Quick Start for address cleanup.**

### `User Material - Canvassers and Action Day/`

Training materials (PDF guides and MP4 videos) for frontline canvassers.

### `User Material - Local Admins/`

Training materials and template CSVs for local campaign coordinators managing the TTW Digital platform.

## Quick start

For the most common task — cleaning up a council register CSV for TTW upload:

```bash
python3 tools/clean_register.py council_data.csv cleaned.csv
```

See [`tools/README.md`](tools/README.md) for everything else.
