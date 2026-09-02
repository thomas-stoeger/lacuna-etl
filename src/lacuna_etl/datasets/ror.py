"""ROR — the Research Organization Registry of research-affiliated organizations.

ROR ships its full registry as a zip containing one large JSON array (schema v2),
one object per organization (~127k). The JSON is the authoritative, fully-structured
form (the sibling CSV is a flattened view we ignore). It loads into memory, so this
is an in-memory pandas pipeline: ``extract`` reads the JSON once and reshapes it into
per-table row lists, ``transform`` is a no-op (faithful projection), and ``load``
types, validates, and writes.

The grain key is the ROR id (``RorId``, the canonical ``https://ror.org/…`` URL). The
parent ``organizations`` table keeps the one-per-org scalars — the display name, year
established, status, the primary (first) location flattened, and the primary Wikidata
id — and the repeated structures are exploded into child tables keyed by ``ror_id``:
all names, organization types, external ids, relationships, links, locations, and
URL domains. Only the Wikidata external id has a canonical type in this repo
(``WikidataId``); the other external ids (GRID, ISNI, Fundref) are heterogeneous and
stay documented plain strings (the Open Targets precedent). See docs/DESIGN.md for the
table inventory.
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import CountryCode, RorId, WikidataId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_STATUSES = {"active", "inactive", "withdrawn"}
_ORG_TYPES = {"education", "funder", "facility", "company", "healthcare", "nonprofit", "other", "government", "archive"}
_NAME_TYPES = {"ror_display", "label", "alias", "acronym"}
_REL_TYPES = {"child", "parent", "related", "predecessor", "successor"}
_LINK_TYPES = {"website", "wikipedia"}
_EXT_ID_TYPES = {"fundref", "grid", "isni", "wikidata"}

# --------------------------------------------------------------------------
# Parent: one row per organization, keyed by ror_id.
# --------------------------------------------------------------------------
ORGANIZATIONS_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id (canonical https://ror.org/… URL); the grain key"),
    "display_name": ColumnSpec(required=True, description="The org's display name (the single names[] entry tagged 'ror_display')"),
    "established": ColumnSpec(description="Year the organization was established (Int64; nullable)"),
    "status": ColumnSpec(allowed_values=_STATUSES, required=True, description="Registry status: active, inactive, or withdrawn"),
    "primary_country_code": ColumnSpec(identifier=CountryCode, description="ISO 3166-1 alpha-2 country code of the primary (first) location (nullable)"),
    "primary_country_name": ColumnSpec(description="Country name of the primary (first) location"),
    "primary_geonames_id": ColumnSpec(description="GeoNames id of the primary (first) location (Int64)"),
    "primary_lat": ColumnSpec(description="Latitude of the primary (first) location (Float64)"),
    "primary_lng": ColumnSpec(description="Longitude of the primary (first) location (Float64)"),
    "wikidata_id": ColumnSpec(identifier=WikidataId, description="Primary Wikidata id (the preferred, else first, wikidata external id; nullable)"),
    "created_date": ColumnSpec(description="Date the ROR record was created (ISO date string, from admin.created)"),
    "last_modified_date": ColumnSpec(description="Date the ROR record was last modified (ISO date string, from admin.last_modified)"),
}

# --------------------------------------------------------------------------
# Child tables keyed by ror_id.
# --------------------------------------------------------------------------
ORG_NAMES_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id"),
    "name": ColumnSpec(required=True, description="A name string for the organization"),
    "name_type": ColumnSpec(allowed_values=_NAME_TYPES, required=True, description="Name type: ror_display, label, alias, or acronym (one row per (name, type) pair)"),
    "lang": ColumnSpec(description="ISO language code of the name (nullable)"),
}

ORG_TYPES_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id"),
    "type": ColumnSpec(allowed_values=_ORG_TYPES, required=True, description="An organization type (education, funder, facility, company, healthcare, nonprofit, government, archive, other)"),
}

ORG_EXTERNAL_IDS_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id"),
    "id_type": ColumnSpec(allowed_values=_EXT_ID_TYPES, required=True, description="External id system: fundref, grid, isni, or wikidata"),
    "value": ColumnSpec(required=True, description="The external id value (one row per id in the 'all' list); heterogeneous, kept verbatim"),
    "is_preferred": ColumnSpec(required=True, description="Whether this is the system's 'preferred' id for the org"),
}

ORG_RELATIONSHIPS_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id (the subject org)"),
    "related_ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id of the related organization"),
    "relation_type": ColumnSpec(allowed_values=_REL_TYPES, required=True, description="Relationship type: child, parent, related, predecessor, or successor"),
    "label": ColumnSpec(description="Display label of the related organization"),
}

ORG_LINKS_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id"),
    "link_type": ColumnSpec(allowed_values=_LINK_TYPES, required=True, description="Link type: website or wikipedia"),
    "url": ColumnSpec(required=True, description="The link URL"),
}

ORG_LOCATIONS_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id"),
    "geonames_id": ColumnSpec(description="GeoNames id of the location (Int64)"),
    "name": ColumnSpec(description="GeoNames place name"),
    "country_code": ColumnSpec(identifier=CountryCode, description="ISO 3166-1 alpha-2 country code (nullable)"),
    "country_name": ColumnSpec(description="Country name"),
    "country_subdivision_code": ColumnSpec(description="Country subdivision (e.g. state/province) code"),
    "country_subdivision_name": ColumnSpec(description="Country subdivision name"),
    "continent_code": ColumnSpec(description="Continent code (e.g. OC, EU)"),
    "continent_name": ColumnSpec(description="Continent name"),
    "lat": ColumnSpec(description="Latitude (Float64)"),
    "lng": ColumnSpec(description="Longitude (Float64)"),
}

ORG_DOMAINS_SCHEMA = {
    "ror_id": ColumnSpec(identifier=RorId, required=True, description="ROR id"),
    "domain": ColumnSpec(required=True, description="A DNS domain associated with the organization"),
}

# Int64 / Float64 columns that need an explicit cast in load (no identifier).
_INT_COLS = {"organizations": ["established", "primary_geonames_id"], "org_locations": ["geonames_id"]}
_FLOAT_COLS = {"organizations": ["primary_lat", "primary_lng"], "org_locations": ["lat", "lng"]}


@register
class Ror(DatasetPipeline):
    name = "ror"

    _TABLES = [
        ("organizations", ORGANIZATIONS_SCHEMA),
        ("org_names", ORG_NAMES_SCHEMA),
        ("org_types", ORG_TYPES_SCHEMA),
        ("org_external_ids", ORG_EXTERNAL_IDS_SCHEMA),
        ("org_relationships", ORG_RELATIONSHIPS_SCHEMA),
        ("org_links", ORG_LINKS_SCHEMA),
        ("org_locations", ORG_LOCATIONS_SCHEMA),
        ("org_domains", ORG_DOMAINS_SCHEMA),
    ]

    def _load_json(self) -> list[dict]:
        zips = sorted(self.raw_path().glob("*.zip"))
        if len(zips) != 1:
            raise FileNotFoundError(f"ror: expected exactly one *.zip under {self.raw_path()}, found {len(zips)}")
        with zipfile.ZipFile(zips[0]) as zf:
            members = [n for n in zf.namelist() if n.endswith(".json")]
            if len(members) != 1:
                raise FileNotFoundError(f"ror: expected exactly one *.json inside {zips[0].name}, found {len(members)}")
            with zf.open(members[0]) as fh:
                return json.load(fh)

    def extract(self) -> None:
        out: dict[str, list[dict]] = {stem: [] for stem, _ in self._TABLES}

        for org in self._load_json():
            ror_id = org["id"]
            locations = org.get("locations") or []
            primary = (locations[0].get("geonames_details") or {}) if locations else {}
            display = next((n["value"] for n in org.get("names", []) if "ror_display" in (n.get("types") or [])), None)
            wikidata = next(
                (e.get("preferred") or (e["all"][0] if e.get("all") else None)
                 for e in org.get("external_ids", []) if e.get("type") == "wikidata"),
                None,
            )
            admin = org.get("admin") or {}

            out["organizations"].append({
                "ror_id": ror_id,
                "display_name": display,
                "established": org.get("established"),
                "status": org.get("status"),
                "primary_country_code": primary.get("country_code"),
                "primary_country_name": primary.get("country_name"),
                "primary_geonames_id": (locations[0].get("geonames_id") if locations else None),
                "primary_lat": primary.get("lat"),
                "primary_lng": primary.get("lng"),
                "wikidata_id": WikidataId.shorten(wikidata) if wikidata else None,
                "created_date": (admin.get("created") or {}).get("date"),
                "last_modified_date": (admin.get("last_modified") or {}).get("date"),
            })

            for n in org.get("names", []):
                for t in n.get("types") or []:
                    out["org_names"].append({"ror_id": ror_id, "name": n["value"], "name_type": t, "lang": n.get("lang")})
            for t in org.get("types") or []:
                out["org_types"].append({"ror_id": ror_id, "type": t})
            for e in org.get("external_ids", []):
                preferred = e.get("preferred")
                for v in e.get("all") or []:
                    out["org_external_ids"].append({"ror_id": ror_id, "id_type": e.get("type"), "value": v, "is_preferred": v == preferred})
            for r in org.get("relationships", []):
                out["org_relationships"].append({"ror_id": ror_id, "related_ror_id": r.get("id"), "relation_type": r.get("type"), "label": r.get("label")})
            for l in org.get("links", []):
                out["org_links"].append({"ror_id": ror_id, "link_type": l.get("type"), "url": l.get("value")})
            for loc in locations:
                gd = loc.get("geonames_details") or {}
                out["org_locations"].append({
                    "ror_id": ror_id,
                    "geonames_id": loc.get("geonames_id"),
                    "name": gd.get("name"),
                    "country_code": gd.get("country_code"),
                    "country_name": gd.get("country_name"),
                    "country_subdivision_code": gd.get("country_subdivision_code"),
                    "country_subdivision_name": gd.get("country_subdivision_name"),
                    "continent_code": gd.get("continent_code"),
                    "continent_name": gd.get("continent_name"),
                    "lat": gd.get("lat"),
                    "lng": gd.get("lng"),
                })
            for dom in org.get("domains") or []:
                out["org_domains"].append({"ror_id": ror_id, "domain": dom})

        for stem, schema in self._TABLES:
            self.save_parquet(pd.DataFrame(out[stem], columns=list(schema)), self.intermediate_path() / f"{stem}.parquet")
        print(
            f"[{self.name}] organizations: {len(out['organizations']):,}, names: {len(out['org_names']):,}, "
            f"types: {len(out['org_types']):,}, external_ids: {len(out['org_external_ids']):,}, "
            f"relationships: {len(out['org_relationships']):,}, locations: {len(out['org_locations']):,}"
        )

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            for col in _INT_COLS.get(stem, []):
                df[col] = df[col].astype("Int64")
            for col in _FLOAT_COLS.get(stem, []):
                df[col] = df[col].astype("Float64")
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
