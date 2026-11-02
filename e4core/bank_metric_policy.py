"""Shared immutable, exact-source semantics for Bank metrics and derived measures.

No substring, component-ID, temporal-shape, or alert-effect fallback is used.
Unknown fields stay ineligible until a source-aware rule with provenance is
added to the reviewed policy.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = ROOT / "config/bank_metric_kind_policy.json"
_POLICY_SCHEMA = "e4-bank-metric-kind-policy/1"
_SOURCE_TABLES = frozenset({
    "bank/metric_container.csv",
    "bank/metric_app.csv",
    "bank/trace_span.csv",
    "bank/log/log_service.csv",
})
_SUPPORTED_RANKING_KINDS = frozenset({"gauge", "rate", "event_count"})


def _policy_source_key(source_table: str) -> str:
    if source_table == "bank/log/log_service.csv":
        return "log_service"
    return source_table.removeprefix("bank/").removesuffix(".csv")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class MetricSemantics:
    """Trusted semantics and provenance for one exact source/field pair."""
    source_table: str
    field_name: str
    kind: str
    unit: str
    unit_status: str
    direction: str
    ranking_eligible: bool
    transform: str
    reset_policy: str
    classification_status: str
    rule_id: str
    rule_version: str
    selector_ref: str | None
    provenance: tuple[str, ...]
    derived_from: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return an allowlist-safe JSON projection."""
        value = asdict(self)
        value["provenance"] = list(self.provenance)
        value["derived_from"] = list(self.derived_from)
        return value


@dataclass(frozen=True, slots=True)
class MetricSelectorRef:
    """Stable model-visible selector; it never relies on list position or regex parsing."""
    selector_ref: str
    source_table: str
    rule_id: str
    rule_version: str
    selector_kind: str
    description: str
    exact_field_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = {"selector_ref": self.selector_ref, "source_table": self.source_table,
            "rule_id": self.rule_id, "rule_version": self.rule_version,
            "selector_kind": self.selector_kind, "description": self.description}
        if self.exact_field_name is not None:
            value["exact_field_name"] = self.exact_field_name
        return value


@dataclass(frozen=True, slots=True)
class SelectorResolution:
    """Host-resolved exact observed fields for one stable selector reference."""
    selector_ref: str
    source_table: str
    status: str
    matches: tuple[MetricSemantics, ...]
    policy_sha256: str
    case_data_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {"selector_ref": self.selector_ref, "source_table": self.source_table,
            "status": self.status, "policy_sha256": self.policy_sha256,
            "case_data_sha256": self.case_data_sha256,
            "matches": [match.to_dict() for match in self.matches],
            "match_count": len(self.matches)}


@dataclass(frozen=True, slots=True)
class BankMetricPolicy:
    """One hash-pinned policy snapshot reusable across a complete aggregation pass."""
    schema_version: str
    sha256: str
    _document: Mapping[str, Any]
    _compiled_rules: Mapping[str, tuple[tuple[re.Pattern[str], Mapping[str, Any]], ...]] = field(
        init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_document", _freeze(self._document))
        compiled: dict[str, tuple[tuple[re.Pattern[str], Mapping[str, Any]], ...]] = {}
        sources = self._document.get("source_semantics", {})
        for source_key, source in sources.items():
            rules = source.get("rules", ()) if isinstance(source, Mapping) else ()
            if not isinstance(rules, tuple):
                raise ValueError("Bank metric source rules must be a list")
            rows = []
            for rule in rules:
                if not isinstance(rule, Mapping):
                    raise ValueError("Bank metric source rule must be an object")
                pattern, rule_id = rule.get("field_pattern"), rule.get("rule_id")
                if not isinstance(pattern, str) or not pattern.startswith("^") or not pattern.endswith("$"):
                    raise ValueError("Bank field patterns must be explicitly anchored with ^ and $")
                if not isinstance(rule_id, str) or not rule_id:
                    raise ValueError("Bank field patterns require a stable rule_id")
                try:
                    rows.append((re.compile(pattern), rule))
                except re.error as exc:
                    raise ValueError(f"invalid Bank metric policy regex for {rule_id}") from exc
            compiled[str(source_key)] = tuple(rows)
        object.__setattr__(self, "_compiled_rules", MappingProxyType(compiled))

    def classify(self, source_table: str, field_name: str) -> MetricSemantics:
        return _classify(self, source_table, field_name)

    @property
    def document(self) -> Mapping[str, Any]:
        """Read-only policy snapshot used for registry/provenance metadata."""
        return self._document

    def exact_rankable_fields(self, source_table: str) -> tuple[str, ...]:
        """List exact configured rankable values; pattern rules need observed values."""
        if source_table not in _SOURCE_TABLES:
            raise ValueError(f"unsupported Bank source table: {source_table!r}")
        semantics = self._document.get("source_semantics", {}).get(_policy_source_key(source_table), {})
        if source_table == "bank/metric_container.csv":
            entries = semantics.get("ranking_allowlist", {})
        elif source_table == "bank/metric_app.csv":
            entries = semantics.get("fields", {})
        elif source_table == "bank/log/log_service.csv":
            entries = semantics.get("derived_fields", {})
            legacy_derived = semantics.get("derived_metric")
            if isinstance(legacy_derived, str):
                entries = {**dict(entries), legacy_derived: {"kind": semantics.get("derived_kind"),
                    "ranking_eligible": semantics.get("ranking_eligible")}}
        else:
            entries = {}
        names = [str(name) for name in entries
            if self.classify(source_table, str(name)).ranking_eligible]
        return tuple(sorted(names))

    def selector_catalog(self, source_table: str) -> tuple[MetricSelectorRef, ...]:
        """Return stable selector refs defined by exact entries or named rules."""
        if source_table not in _SOURCE_TABLES:
            raise ValueError(f"unsupported Bank source table: {source_table!r}")
        refs: dict[str, MetricSelectorRef] = {}
        for field_name in self.exact_rankable_fields(source_table):
            semantics = self.classify(source_table, field_name)
            if semantics.selector_ref:
                refs[semantics.selector_ref] = MetricSelectorRef(
                    semantics.selector_ref, source_table, semantics.rule_id,
                    semantics.rule_version, "exact", field_name, field_name)
        for _pattern, rule in self._compiled_rules.get(_policy_source_key(source_table), ()):
            if rule.get("ranking_eligible") is not True or rule.get("kind") not in _SUPPORTED_RANKING_KINDS:
                continue
            rule_id = str(rule["rule_id"])
            rule_version = str(rule.get("rule_version", "1"))
            ref = f"{rule_id}@{rule_version}"
            refs[ref] = MetricSelectorRef(ref, source_table, rule_id,
                rule_version, "pattern", str(rule.get("description", rule_id)))
        return tuple(refs[key] for key in sorted(refs))

    def resolve_selector(self, selector_ref: str, source_table: str,
                         observed_field_names: tuple[str, ...] | list[str], *,
                         case_data_sha256: str) -> SelectorResolution:
        """Resolve to all matching exact fields, never silently choosing the first."""
        if not isinstance(case_data_sha256, str) or len(case_data_sha256) != 64 or any(
                char not in "0123456789abcdef" for char in case_data_sha256):
            raise ValueError("case_data_sha256 must be a lowercase SHA-256 digest")
        catalog = {entry.selector_ref: entry for entry in self.selector_catalog(source_table)}
        entry = catalog.get(selector_ref)
        if entry is None:
            raise ValueError("selector_ref is not in the trusted metric catalog")
        if not isinstance(observed_field_names, (tuple, list)) or any(
                not isinstance(name, str) or not name for name in observed_field_names):
            raise ValueError("observed_field_names must contain exact non-empty field names")
        names = tuple(observed_field_names)
        if len(set(names)) != len(names):
            raise ValueError("observed_field_names must be unique")
        if entry.selector_kind == "exact":
            selected = [entry.exact_field_name] if entry.exact_field_name in set(names) else []
        else:
            compiled = next((pattern for pattern, rule in self._compiled_rules.get(
                _policy_source_key(source_table), ())
                if rule.get("rule_id") == entry.rule_id and
                   str(rule.get("rule_version", "1")) == entry.rule_version), None)
            if compiled is None:
                raise ValueError("selector_ref no longer resolves to a compiled rule")
            selected = [name for name in names if compiled.fullmatch(name)]
        resolved = tuple(self.classify(source_table, name) for name in selected)
        # An exact override always wins over a family pattern. Such fields stay
        # reachable through their own exact refs; the family ref resolves only
        # the unoverridden members, possibly zero or many.
        matches = tuple(item for item in resolved
            if item.ranking_eligible and item.selector_ref == selector_ref)
        status = "unavailable" if not matches else "resolved_one" if len(matches) == 1 else "resolved_multiple"
        return SelectorResolution(selector_ref, source_table, status, matches,
            self.sha256, case_data_sha256)


def _policy_version(path: Path) -> tuple[str, str, dict[str, Any]]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Bank metric policy must be a regular file")
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Bank metric policy is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict) or document.get("schema_version") != _POLICY_SCHEMA:
        raise ValueError("unsupported Bank metric policy schema")
    return _POLICY_SCHEMA, digest, document


@lru_cache(maxsize=16)
def _load_cached_policy(path_text: str, modified_ns: int, size: int, digest: str) -> BankMetricPolicy:
    del modified_ns, size
    path = Path(path_text)
    schema, observed_digest, document = _policy_version(path)
    if observed_digest != digest:
        raise ValueError("Bank metric policy changed while it was being loaded")
    return BankMetricPolicy(schema, digest, document)


def load_bank_metric_policy(path: Path = DEFAULT_POLICY_PATH) -> BankMetricPolicy:
    """Load a hash-pinned snapshot once; share it across bulk metric processing."""
    path = Path(path).resolve(strict=True)
    stat = path.stat()
    digest = _digest(path)
    return _load_cached_policy(str(path), stat.st_mtime_ns, stat.st_size, digest)


def _unknown(source_table: str, field_name: str, digest: str, *,
             reason: str = "no exact source-field rule") -> MetricSemantics:
    return MetricSemantics(source_table=source_table, field_name=field_name,
        kind="unknown", unit="undeclared", unit_status="undeclared",
        direction="unknown", ranking_eligible=False, transform="none",
        reset_policy="unverified", classification_status="unknown",
        rule_id=f"default-unknown/{source_table}/{digest[:12]}", rule_version="1",
        selector_ref=None, provenance=(reason, f"policy_sha256:{digest}"))


def _rule_entry(policy: BankMetricPolicy, source_key: str, field_name: str) -> tuple[Mapping[str, Any] | None, str | None]:
    rules = policy._compiled_rules.get(source_key, ())
    matches: list[tuple[Mapping[str, Any], str]] = []
    for compiled, rule in rules:
        rule_id = str(rule["rule_id"])
        if compiled.fullmatch(field_name):
            matches.append((rule, rule_id))
    if len(matches) > 1:
        raise ValueError(f"ambiguous Bank metric field rules for {field_name!r}")
    return matches[0] if matches else (None, None)


def _classify(policy: BankMetricPolicy, source_table: str, field_name: str) -> MetricSemantics:
    if not isinstance(source_table, str) or source_table not in _SOURCE_TABLES:
        raise ValueError(f"unsupported Bank source table: {source_table!r}")
    if not isinstance(field_name, str) or not field_name or len(field_name.encode("utf-8")) > 512:
        raise ValueError("Bank metric field name must be non-empty UTF-8 text up to 512 bytes")
    source_path = _policy_source_key(source_table)
    source = policy._document.get("source_semantics", {}).get(source_path, {})
    entry: Mapping[str, Any] | None = None
    rule_id: str | None = None
    scope = "exact"
    derived_from: tuple[str, ...] = ()

    if source_table == "bank/metric_container.csv":
        entry = source.get("ranking_allowlist", {}).get(field_name)
    elif source_table == "bank/metric_app.csv":
        entry = source.get("fields", {}).get(field_name)
    elif source_table == "bank/trace_span.csv" and field_name == "duration":
        entry = {"kind": "unknown", "unit": source.get("duration_unit", "undeclared"),
            "ranking_eligible": False,
            "provenance": "duration field exists but value unit is undeclared by Bank schema"}
    elif source_table == "bank/log/log_service.csv":
        entry = source.get("derived_fields", {}).get(field_name)
        if entry is None and field_name == source.get("derived_metric"):
            entry = {"kind": source.get("derived_kind", "unknown"),
                "unit": source.get("derived_unit", "undeclared"),
                "ranking_eligible": bool(source.get("ranking_eligible", False)),
                "provenance": "derived by counting all hash-pinned log rows per component/log_name/minute",
                "unit_status": "derived_from_source_rows",
                "transform": "count_rows_per_fixed_minute_bucket",
                "derived_from": ("log_id", "timestamp", "cmdb_id", "log_name")}
        if entry is not None:
            scope = "derived"
            derived_from = tuple(entry.get("derived_from", ()))
        elif field_name == "value":
            entry = {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False,
                "provenance": "raw log value is not parsed by the trusted Bank adapter"}
    if entry is None:
        entry, rule_id = _rule_entry(policy, source_path, field_name)
        if entry is not None:
            scope = "pattern"
    if not isinstance(entry, Mapping):
        return _unknown(source_table, field_name, policy.sha256)

    kind = entry.get("kind", "unknown")
    if not isinstance(kind, str) or not kind:
        kind = "unknown"
    unit = entry.get("unit", "undeclared")
    if not isinstance(unit, str) or not unit:
        unit = "undeclared"
    direction = entry.get("direction", "unknown")
    if direction not in {"higher_is_worse", "lower_is_worse", "two_sided", "unknown"}:
        direction = "unknown"
    eligible = bool(entry.get("ranking_eligible", False)) and kind in _SUPPORTED_RANKING_KINDS
    provenance_value = entry.get("provenance", "exact source table/field rule")
    if isinstance(provenance_value, str):
        provenance = (provenance_value,)
    elif isinstance(provenance_value, (tuple, list)) and all(isinstance(x, str) for x in provenance_value):
        provenance = tuple(provenance_value)
    else:
        provenance = ("invalid provenance value; classification remains conservative",)
        eligible = False
        kind = "unknown"
    unit_status = entry.get("unit_status")
    if not isinstance(unit_status, str):
        unit_status = "undeclared" if "undeclared" in unit.casefold() else "working_interpretation"
    reset_policy = entry.get("reset_policy", "not_applicable" if kind in {"gauge", "rate", "event_count"} else "unverified")
    if not isinstance(reset_policy, str):
        reset_policy = "unverified"
    transform = entry.get("transform", "identity" if kind in {"gauge", "rate"} else "none")
    if not isinstance(transform, str):
        transform = "none"
    classification_status = entry.get("classification_status",
        "derived" if scope == "derived" else
        "unknown" if kind == "unknown" else
        "working_interpretation" if scope == "exact" else "documented_rule")
    if not isinstance(classification_status, str):
        classification_status = "unknown"
    rule_id = entry.get("rule_id", rule_id or f"{scope}-exact/{source_table}/{field_name}")
    if not isinstance(rule_id, str) or not rule_id:
        rule_id = f"{scope}-exact/{source_table}/{field_name}"
    rule_version = entry.get("rule_version", "1")
    if not isinstance(rule_version, str) or not rule_version:
        raise ValueError(f"invalid rule_version for Bank metric rule {rule_id}")
    selector_ref = f"{rule_id}@{rule_version}" if eligible else None
    return MetricSemantics(source_table=source_table, field_name=field_name,
        kind=kind, unit=unit, unit_status=unit_status, direction=direction,
        ranking_eligible=eligible, transform=transform, reset_policy=reset_policy,
        classification_status=classification_status, rule_id=rule_id,
        rule_version=rule_version, selector_ref=selector_ref,
        provenance=(*provenance, f"policy_sha256:{policy.sha256}"), derived_from=derived_from)


def classify_bank_metric(source_table: str, field_name: str, *,
                         policy_path: Path = DEFAULT_POLICY_PATH) -> MetricSemantics:
    """Classify one exact Bank source table/field pair with no name fallback."""
    return load_bank_metric_policy(policy_path).classify(source_table, field_name)


__all__ = ["BankMetricPolicy", "MetricSemantics", "classify_bank_metric",
           "load_bank_metric_policy"]
