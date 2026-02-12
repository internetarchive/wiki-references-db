import pytest

from refs_extractor.syntax import normalize_wikitext


testcases = [
    """
{{Cite_web
| unnamed1
| unnamed2
| foo        = value
| bar        = value2
| paz      =    value 3
| bigpara    = 
Okay so here's the deal guys.

This is a crazy guy citation.
}}
""",
    "[https://example.com {{ flag | USA }}]",
    """
<ref name="Jones 2007">{{cite news |author = Jones, Tim |date = March 27, 2007 |title = Barack Obama: Mother not just a girl from Kansas; Stanley Ann Dunham shaped a future senator |newspaper = [[Chicago Tribune]] |page = 1 (Tempo) |url=http://gbppr.dyndns.org/~gbpprorg/obama/barack.mother.txt |url-status=dead |archive-url=https://web.archive.org/web/20170207112933/http://gbppr.dyndns.org/~gbpprorg/obama/barack.mother.txt |archive-date = February 7, 2017 }}</ref>
""",
    "{{Cite web | url = http://example.com | title = Example | access-date = {{CURRENTYEAR}}-{{CURRENTMONTH}}-{{CURRENTDAY}} }}",
    "[http://example.com {{Cite web | url = http://example.com | title = Example | access-date = 2023-05-19}}]",
    "<ref name=\"test\">{{Cite web | url = http://example.com | title = Example | access-date = 2023-05-19}}<nowiki>{{Not a template}}</nowiki></ref>",
    "{{Cite web | url = http://example.com | title = {{random}} | access-date = 2023-05-19}}",
    "{{Example | unnamed | name=value | foo = bar | 2=second unnamed }}",
    "{{cite web | url = [http://example.com Example] | title = Example Title | access-date = 2023-05-19}}",
    """
<ref>{{cite news |author = Serafin, Peter |date = March 21, 2004 |title = Punahou grad stirs up Illinois politics |newspaper = [[Honolulu_Star-Bulletin]] |url=http://archives.starbulletin.com/2004/03/21/news/story4.html |access-date = March 20, 2008 }}
* {{cite news |author = Scott, Janny |date = March 14, 2008 |title = A free-spirited wanderer who set Obama's path |work = The New York Times |page = A1 |url=https://www.nytimes.com/2008/03/14/us/politics/14obama.html |archive-url=https://web.archive.org/web/20080314042735/http://www.nytimes.com/2008/03/14/us/politics/14obama.html |archive-date=March 14, 2008 |url-access=limited |url-status=live |access-date = November 18, 2011 }}
*Obama (1995, 2004), Chapters 3 and 4.
* Scott (2012), pp. 131–134.
*Maraniss (2012), pp. 264–269.</ref>
""",
    "<ref name=john></ref>",
    "<ref name=john/>",
    "***Hello world",
    "##test2",
    """
<ref>
Multi-line ref
Second line
</ref>
""",
]

testanswers = [
    "{{Cite web|unnamed1|unnamed2|bar=value2|bigpara=Okay so here's the deal guys. This is a crazy guy citation.|foo=value|paz=value 3}}",
    "[https://example.com {{Flag|USA}}]",
    "<ref name=\"Jones 2007\">{{Cite news|archive-date=February 7, 2017|archive-url=https://web.archive.org/web/20170207112933/http://gbppr.dyndns.org/~gbpprorg/obama/barack.mother.txt|author=Jones, Tim|date=March 27, 2007|newspaper=[[Chicago Tribune]]|page=1 (Tempo)|title=Barack Obama: Mother not just a girl from Kansas; Stanley Ann Dunham shaped a future senator|url-status=dead|url=http://gbppr.dyndns.org/~gbpprorg/obama/barack.mother.txt}}</ref>",
    "{{Cite web|access-date={{CURRENTYEAR}}-{{CURRENTMONTH}}-{{CURRENTDAY}}|title=Example|url=http://example.com}}",
    "[http://example.com {{Cite web|access-date=2023-05-19|title=Example|url=http://example.com}}]",
    "<ref name=\"test\">{{Cite web|access-date=2023-05-19|title=Example|url=http://example.com}}<nowiki>{{Not a template}}</nowiki></ref>",
    "{{Cite web|access-date=2023-05-19|title={{Random}}|url=http://example.com}}",
    "{{Example|unnamed|2=second unnamed|foo=bar|name=value}}",
    "{{Cite web|access-date=2023-05-19|title=Example Title|url=[http://example.com Example]}}",
    "<ref>{{Cite news|access-date=March 20, 2008|author=Serafin, Peter|date=March 21, 2004|newspaper=[[Honolulu Star-Bulletin]]|title=Punahou grad stirs up Illinois politics|url=http://archives.starbulletin.com/2004/03/21/news/story4.html}}\n* {{Cite news|access-date=November 18, 2011|archive-date=March 14, 2008|archive-url=https://web.archive.org/web/20080314042735/http://www.nytimes.com/2008/03/14/us/politics/14obama.html|author=Scott, Janny|date=March 14, 2008|page=A1|title=A free-spirited wanderer who set Obama's path|url-access=limited|url-status=live|url=https://www.nytimes.com/2008/03/14/us/politics/14obama.html|work=The New York Times}}\n* Obama (1995, 2004), Chapters 3 and 4.\n* Scott (2012), pp. 131–134.\n* Maraniss (2012), pp. 264–269.</ref>",
    "<ref name=\"john\"></ref>",
    "<ref name=\"john\" />",
    "*** Hello world",
    "## test2",
    """<ref>Multi-line ref
Second line</ref>""",
]


@pytest.mark.parametrize("source,expected", list(zip(testcases, testanswers)))
def test_normalize_wikitext(source, expected):
    assert normalize_wikitext(source) == expected
