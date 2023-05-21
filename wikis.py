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
    assert get_family("www.wikidata.org") == "Wikidata"
    assert get_family("wikidata.org") == "Wikidata"
    assert get_family("commons.wikimedia.org") == "Wikimedia Commons"
    assert get_family("species.wikimedia.org") == "Wikispecies"
    assert get_family("zh.wikipedia.org") == "Wikipedia"
    assert get_family("scn.wiktionary.org") == "Wiktionary"
    assert get_family("wikisource.org") == "Wikisource"
