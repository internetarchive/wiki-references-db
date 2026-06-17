from wikis import get_family


def test_get_family_common_domains():
    assert get_family("www.wikidata.org") == "Wikidata"
    assert get_family("wikidata.org") == "Wikidata"
    assert get_family("commons.wikimedia.org") == "Wikimedia Commons"
    assert get_family("species.wikimedia.org") == "Wikispecies"


def test_get_family_language_projects():
    assert get_family("zh.wikipedia.org") == "Wikipedia"
    assert get_family("scn.wiktionary.org") == "Wiktionary"
    assert get_family("wikisource.org") == "Wikisource"
