"""Fixed inputs for the wcd_semantic golden tests.

Two things live here:

* ``CONTENTS`` — ref inner-content strings (what sits between <ref> and
  </ref>, or a bibliography list item) covering every ref-tag subtype and
  template subtype the UVA normalizers know, plus identifier forms.
* ``PAGES`` — a small multi-page citation corpus for the matchers:
  page_id → list of (ref_name, content, n_revisions, first_rev_id). Pages
  share sources by DOI / ISBN / URL / author+year so that every signal pass
  and the cross-page passes have something to do.

Keep this file append-only: the golden files were generated from it with
the upstream UVA code, so changing an existing entry invalidates them.
"""

CONTENTS = [
    # --- templates -------------------------------------------------------
    "{{cite book |last=Diamond |first=Jared |year=2005 |title=Collapse |publisher=Viking |isbn=978-0-14-303655-5}}",
    "{{cite book|last=Diamond|first=Jared|title=Collapse: How Societies Choose to Fail or Succeed|year=2005|location=New York|publisher=Viking|isbn=0-670-03337-5|pages=79–119}}",
    "{{cite journal |last=Fischer |first=Steven Roger |year=1995 |title=Preliminary Evidence for Cosmogonic Texts in Rapanui's Rongorongo Inscriptions |journal=Journal of the Polynesian Society |volume=104 |issue=3 |pages=303–321 |doi=10.2307/20706624}}",
    "{{cite journal|author=Smith, John|date=March 2010|title=A study of things|journal=Nature|volume=464|pages=1–5|pmid=20237412|pmc=2855555|doi=10.1038/nature08888}}",
    "{{Cite web |url=https://example.org/page |title=A page |website=Example |access-date=2020-01-01 |archive-url=https://web.archive.org/web/20200101000000/https://example.org/page |archive-date=2020-01-01}}",
    "{{cite web|url=http://www.bbc.co.uk/news/world-12345678|title=World news story|publisher=BBC News|date=1 January 2015|accessdate=2 January 2015}}",
    "{{cite news |last=Jones |first=Mary |title=Election results |newspaper=The Guardian |date=2019-12-13 |url=https://www.theguardian.com/politics/2019/dec/13/results}}",
    "{{citation |last=Routledge |first=Katherine |year=1919 |title=The Mystery of Easter Island |place=London |publisher=Sifton, Praed}}",
    "{{cite thesis |last=Nguyen |first=T. |year=2012 |title=On Islands |type=PhD |publisher=University of Auckland}}",
    "{{cite encyclopedia |title=Rapa Nui |encyclopedia=Encyclopædia Britannica |year=2008 |url=https://www.britannica.com/place/Easter-Island}}",
    "{{cite arXiv |last=Doe |first=Jane |year=2021 |title=Preprint on citations |eprint=2101.00001 |class=cs.DL}}",
    "{{Cite Q|Q12345678}}",
    "{{cite book |author1=Alfred Métraux |title=Ethnology of Easter Island |year=1940 |publisher=Bishop Museum Press |location=Honolulu}}",
    "{{cite book|authors=Lee, A.; Kim, B.|title=Co-authored volume|year=1999|isbn=9780306406157}}",
    "{{cite journal |last1=Lee |first1=A |year=2001 |title=Trade A |journal=J1 |doi=10.1000/xyz123}}",
    "{{cite book |last=Smith |first=J. |title=Untitled |date=c. 1850}}",
    "{{cite web |title=No author no date |url=https://example.net/x}}",
    "{{cite tweet |user=someone |number=12345 |title=A tweet |date=2020-05-05}}",
    "Prose before the template.{{cite book |last=Embedded |first=E. |year=2000 |title=An embedded citation |isbn=1-234-56789-X}} Prose after.",
    # --- ref-tag subtypes ------------------------------------------------
    "[[#Diamond2005|Diamond 2005]], p. 12",
    "[[#CITEREFRoutledge1919|Routledge (1919)]], pp. 200–210",
    "[https://example.org/island Official site]",
    "https://example.org/visits",
    "[http://www.jps.auckland.ac.nz/document/Volume_104_1995/ JPS volume 104]",
    "{{sfn|Routledge|1919|p=12}}",
    "{{harvnb|Diamond|2005|pp=79–119}}",
    "{{harvnb|Diamond|2005|p=80}}",
    "{{sfnp|Fischer|1995|p=303}} and {{harvnb|Métraux|1940}}",
    "{{rp|12}}",
    "Smith (2003) p. 45",
    "Beveridge 1: 428",
    "Fischer, Steven Roger (1995). \"Preliminary Evidence\". ''Journal of the Polynesian Society'' 104: 303–21.",
    "Heider, ''Anarchism: Left, Right and Green'', AK Press, 1994",
    "Métraux, Alfred (1940). ''Ethnology of Easter Island''. Honolulu: Bishop Museum Press.",
    "Diamond, Jared. ''Collapse''. Viking, 2005. ISBN 978-0-14-303655-5.",
    "J. Smith et al., \"A study of things,\" Nature 464, 1–5 (2010). doi:10.1038/nature08888 PMID 20237412",
    "<cite class=\"citation book\">Diamond, Jared (2005). ''Collapse''. New York: Viking. ISBN 0-670-03337-5.</cite>",
    "See the discussion in the previous section.",
    "ibid",
    "wiki",
    "",
    "   ",
    "&nbsp;",
    "[[Easter Island]] is an island.",
    "<small>Note: figures are approximate.</small>",
    "* {{cite journal |last=Lee |first=A |year=2001 |title=Trade A long enough title here |journal=J1}}\n* {{cite journal |last=Lee |first=B |year=2002 |title=Trade B long enough title here |journal=J2}}",
    "* Diamond, Jared (2005). ''Collapse''. Viking. A bullet with a prose citation that is long.\n* Routledge, Katherine (1919). ''The Mystery of Easter Island''. London. Another long bullet item.",
    "{| class=\"wikitable\"\n! a !! b\n|-\n| 1 || 2\n|}",
    "Name &amp; Sons (1999). ''Title &ndash; Subtitle''. pp. 1&ndash;2.",
    "Smith, J. (2003a). First paper. Journal 1. Smith, J. (2003b). Second paper. Journal 2.",
    "Ministry of Health (2018). ''Annual report''. Wellington.",
    "<!-- hidden --> Visible citation text (2001) by Author",
]

# page_id -> [(ref_name, content, n_revisions, first_rev_id), ...]
PAGES = {
    101: [
        ("diamond", "{{cite book |last=Diamond |first=Jared |year=2005 |title=Collapse |publisher=Viking |isbn=978-0-14-303655-5}}", 3, 1001),
        (None, "Fischer, Steven Roger (1995). \"Preliminary Evidence\". ''Journal of the Polynesian Society'' 104: 303–21.", 2, 1001),
        (None, "{{sfn|Routledge|1919|p=12}}", 3, 1001),
        (None, "[https://example.org/visits Visits]", 1, 1002),
        ("trade", "{{cite journal |last1=Lee |first1=A |year=2001 |title=Trade A |journal=J1 |doi=10.1000/xyz123}}", 2, 1002),
        (None, "{{harvnb|Diamond|2005|p=80}}", 2, 1002),
        (None, "Diamond, Jared. ''Collapse''. Viking, 2005. ISBN 978-0-14-303655-5.", 1, 1003),
        (None, "[[#CITEREFRoutledge1919|Routledge (1919)]], pp. 200–210", 1, 1003),
        (None, "[[#CITEREFRoutledge1919|Routledge 1919]], p. 7", 1, 1003),
        ("fischer", "{{cite journal |last=Fischer |first=Steven Roger |year=1995 |title=Preliminary Evidence for Cosmogonic Texts in Rapanui's Rongorongo Inscriptions |journal=Journal of the Polynesian Society |volume=104 |issue=3 |pages=303–321 |doi=10.2307/20706624}}", 1, 1003),
        (None, "ibid", 1, 1003),
        (None, "{{cite book |last=Routledge |first=Katherine |year=1919 |title=The Mystery of Easter Island |location=London}}", 1, 1003),
    ],
    202: [
        (None, "{{cite book|last=Diamond|first=Jared|title=Collapse: How Societies Choose to Fail or Succeed|year=2005|location=New York|publisher=Viking|isbn=0-670-03337-5|pages=79–119}}", 2, 2001),
        ("web1", "{{Cite web |url=https://example.org/page |title=A page |website=Example |access-date=2020-01-01}}", 2, 2001),
        (None, "https://example.org/page", 1, 2002),
        (None, "Fischer, S. R. (1995). Preliminary evidence for cosmogonic texts. ''Journal of the Polynesian Society'', 104(3), 303–321.", 1, 2002),
        (None, "{{cite journal|author=Smith, John|date=March 2010|title=A study of things|journal=Nature|volume=464|pages=1–5|pmid=20237412|doi=10.1038/nature08888}}", 1, 2002),
        (None, "J. Smith et al., \"A study of things,\" Nature 464, 1–5 (2010). doi:10.1038/nature08888", 1, 2002),
        (None, "Smith (2010) p. 3", 1, 2002),
        (None, "Heider, ''Anarchism: Left, Right and Green'', AK Press, 1994", 1, 2002),
        (None, "{{cite book|last=Routledge|first=K.|year=1919|title=The mystery of Easter Island|publisher=Sifton, Praed}}", 1, 2002),
    ],
    303: [
        (None, "{{cite journal |last=Lee |first=A. |year=2001 |title=Trade A |journal=Journal One |doi=10.1000/xyz123}}", 1, 3001),
        (None, "Lee, A. (2001). ''Trade A''. Journal One.", 1, 3001),
        (None, "Heider, ''Anarchism: Left, Right and Green'', AK Press, 1994", 1, 3001),
        (None, "Heider, Ulrike. ''Anarchism: Left, Right, and Green''. AK Press, 1994.", 1, 3001),
        ("mh", "Ministry of Health (2018). ''Annual report''. Wellington.", 1, 3001),
        ("mh", "Ministry of Health (2018). ''Annual report''. Wellington. Retrieved 2019.", 1, 3001),
        (None, "wiki", 1, 3001),
        (None, "{{cite book |last=Diamond |first=Jared |year=2005 |title=Collapse |publisher=Penguin |isbn=978-0-14-303655-5}}", 1, 3002),
    ],
}

# Revision texts for the signal flags (from the pipeline's synthetic bundle shapes)
REVISION_TEXTS = [
    "plain text with no references at all",
    "a<ref name=\"x\">{{cite web|url=https://e.org|title=t}}</ref> b<ref name=\"x\" /> {{sfn|A|2000}}\n== References ==\n<references />",
    "{{citation |last=R |year=1919 |title=T}} and {{harvnb|R|1919}} and {{r|x}}",
    "<REF>upper case</REF> {{Cite book|title=x}} {{Citation|title=y}}",
]
