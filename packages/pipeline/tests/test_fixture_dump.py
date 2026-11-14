import bz2
import xml.etree.ElementTree as ET

from wcd_pipeline import fixture_dump

NS = "http://www.mediawiki.org/xml/export-0.11/"


def _dump(pages):
    body = "".join(
        f"  <page>\n    <title>{t}</title>\n    <ns>0</ns>\n    <id>{pid}</id>\n"
        + "".join(
            f"    <revision>\n      <id>{rid}</id>\n      <timestamp>2020-01-0{i+1}T00:00:00Z</timestamp>\n"
            f"      <text bytes=\"5\" xml:space=\"preserve\">rev{rid}</text>\n    </revision>\n"
            for i, rid in enumerate(revs)
        )
        + "  </page>\n"
        for t, pid, revs in pages
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        f'<mediawiki xmlns="{NS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        f'xsi:schemaLocation="{NS} http://www.mediawiki.org/xml/export-0.11.xsd" version="0.11" xml:lang="en">\n'
        "  <siteinfo>\n    <sitename>Wikipedia</sitename>\n    <dbname>enwiki</dbname>\n  </siteinfo>\n"
        + body + "</mediawiki>\n"
    ).encode("utf-8")


def test_filter_by_id_and_title_across_two_files(tmp_path):
    d1 = tmp_path / "a.xml"; d1.write_bytes(_dump([("Alpha", 1, [11, 12]), ("Beta", 2, [21])]))
    d2 = tmp_path / "b.xml.bz2"; d2.write_bytes(bz2.compress(_dump([("Gamma Delta", 3, [31, 32, 33]), ("Omega", 4, [41])])))
    pages = tmp_path / "pages.txt"; pages.write_text("# ids and titles\n1\nGamma_Delta\n")
    out = tmp_path / "out.xml.bz2"
    rc = fixture_dump.main([str(d1), str(d2), "-p", str(pages), "-o", str(out)])
    assert rc == 0
    root = ET.fromstring(bz2.decompress(out.read_bytes()))
    assert root.tag == f"{{{NS}}}mediawiki"
    assert root.get("version") == "0.11"
    assert root.find(f"{{{NS}}}siteinfo/{{{NS}}}dbname").text == "enwiki"
    got = [(p.find(f"{{{NS}}}title").text, int(p.find(f"{{{NS}}}id").text), len(p.findall(f"{{{NS}}}revision")))
           for p in root.findall(f"{{{NS}}}page")]
    assert got == [("Alpha", 1, 2), ("Gamma Delta", 3, 3)]
    # revision text survives verbatim
    assert root.find(f"{{{NS}}}page/{{{NS}}}revision/{{{NS}}}text").text == "rev11"


def test_missing_pages_reported(tmp_path, capsys):
    d1 = tmp_path / "a.xml"; d1.write_bytes(_dump([("Alpha", 1, [11])]))
    pages = tmp_path / "pages.txt"; pages.write_text("1\n999\nNope\n")
    out = tmp_path / "out.xml"
    assert fixture_dump.main([str(d1), "-p", str(pages), "-o", str(out)]) == 1
    assert "not found: 1 ids, 1 titles" in capsys.readouterr().err
