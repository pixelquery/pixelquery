# Security Policy

## Supported Versions

PixelQuery is currently in `0.x` (pre-1.0). Only the latest minor release line
receives security fixes.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

Once `1.0.0` is released, supported versions will follow a longer lifecycle and
this table will be updated.

## Reporting a Vulnerability

If you believe you have found a security vulnerability in PixelQuery, please
**do not** open a public GitHub issue. Public disclosure before a fix is
available puts users at risk.

Instead, report the issue privately by either:

1. **Email** — send a description to **thsghdud13@gmail.com** with the subject
   line `[SECURITY] PixelQuery: <short summary>`.
2. **GitHub private advisory** — use the
   [GitHub Security Advisory](https://github.com/pixelquery/pixelquery/security/advisories/new)
   form on this repository to file a private report.

Please include:

- A clear description of the vulnerability and its impact.
- Steps to reproduce, or a minimal proof-of-concept.
- The PixelQuery version, Python version, and operating system you observed
  the issue on.
- Any suggested mitigation, if you have one.

## Response Timeline

- **Acknowledgement**: within 72 hours of receipt.
- **Initial assessment**: within 7 days, including a severity classification
  and an estimated fix timeline.
- **Fix and disclosure**: depending on severity and complexity. Critical
  issues will be prioritized.

## Disclosure Policy

PixelQuery follows a coordinated disclosure model. Once a fix is available,
we will:

1. Release a patched version on PyPI.
2. Publish a GitHub Security Advisory with details of the issue, affected
   versions, and credit to the reporter (unless anonymity is requested).
3. Update `CHANGELOG.md` under a `Security` section.

## Scope

In scope:

- The PixelQuery Python package and its release artifacts on PyPI.
- The contents of this repository.

Out of scope:

- Third-party dependencies (please report those upstream — `obstore`,
  `icechunk`, `virtualizarr`, `zarr`, `xarray`, `rasterio`, etc.).
- Issues in user-supplied configuration, credentials, or deployment
  environments.
- Vulnerabilities that require attacker-controlled COG files combined with
  unrelated downstream tools, unless PixelQuery itself can be made to
  misbehave on otherwise-trusted input.

Thank you for helping keep PixelQuery and its users safe.
