def get_family(domain: str) -> str:
    if domain == "species.wikimedia.org":
        return "Wikispecies"
    elif domain == "commons.wikimedia.org":
        return "Wikimedia Commons"
    else:
        parts = domain.split(".")
        if len(parts) == 3:
            return parts[1].capitalize()
        else:
            return parts[0].capitalize()

if __name__ == '__main__':
    # No inline tests; see tests/test_wikis.py
    pass
