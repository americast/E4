# Bank domain analytical priors — optional, evaluation-informed variant

**Exposure notice:** This optional resource is informed by prior inspection of scored evaluation failures, including discussion of log-focused analyses after those failures. It is not evaluation-independent and must not be described as an untouched domain prior. Use only in a separately labeled evaluation-informed prompt variant; compare it only with the essentials-only brief under a predeclared protocol. No scored labels, incident identifiers, or answer mappings are included here.

The following are human-specified analytical hypotheses, not guaranteed useful algorithms or claims about any particular case. Their source is `previously inspected evaluation failures; prompt-development discussion after those failures`, with exposure class `evaluation-informed`. The caller retains this input provenance and tracks use in generated artifacts separately; no benefit is claimed here.

- **Log context summaries:** If and only if the supplied public schema declares suitable fields, consider bounded comparisons of log category/name/message counts or rates between reference and alert windows, grouped by observed component identity where such identity is explicitly available. Do not assume an error/severity field or treat message presence as proof of cause.
- **Metric-to-trace context:** If declared fields and identity links support it, consider whether component-level metric changes coincide with changes in span duration or structure across the two windows. Coincidence is evidence of association, not proof of causation.
- **Window contrasts:** Where reference semantics and data coverage are adequate, consider robust per-component comparisons across reference and alert windows. Treat reference health as unknown unless it is established by supplied metadata.

These suggestions should be omitted when fields, coverage, or identity links are unavailable. Do not force every suggestion into an operator or playbook. Do not claim these priors improve accuracy without a separate controlled comparison.
