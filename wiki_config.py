import os
import yaml
from functools import lru_cache

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "wikis.yaml")

@lru_cache(maxsize=1)
def _load_configs():
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_wiki_config(domain: str) -> dict:
    """Return config for a domain, falling back to en.wikipedia.org defaults."""
    configs = _load_configs()
    return configs.get(domain, configs.get("en.wikipedia.org", {}))

def get_reference_sections(domain: str) -> list:
    return get_wiki_config(domain).get("reference_sections", [])

def get_citation_template_prefixes(domain: str) -> list:
    """Return citation template name prefixes (e.g. 'cite' matches 'cite', 'cite web', 'cite book', etc.)."""
    tpl_config = get_wiki_config(domain).get("citation_templates", {})
    if isinstance(tpl_config, dict):
        return tpl_config.get("prefixes", [])
    # Legacy flat list: treat all entries as prefixes for backward compatibility
    return tpl_config

def get_citation_template_exact(domain: str) -> list:
    """Return citation template names that must match exactly."""
    tpl_config = get_wiki_config(domain).get("citation_templates", {})
    if isinstance(tpl_config, dict):
        return tpl_config.get("exact", [])
    return []
