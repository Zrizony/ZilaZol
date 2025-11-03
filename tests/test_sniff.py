from crawler.archive_utils import sniff_format, iter_xml_entries


def test_sniff_and_iter():
    raw_xml = b'<?xml version="1.0"?><prices/>'
    assert sniff_format(raw_xml) == "raw"
    assert list(iter_xml_entries(raw_xml, "a.xml"))[0][0].endswith(".xml")

    import gzip, io, zipfile
    gz = gzip.compress(raw_xml)
    assert sniff_format(gz) == "gz"
    assert list(iter_xml_entries(gz, "a.gz"))[0][0].endswith(".xml")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("inside.xml", raw_xml)
    zbytes = buf.getvalue()
    assert sniff_format(zbytes) == "zip"
    items = list(iter_xml_entries(zbytes, "a.zip"))
    assert items and items[0][0] == "inside.xml"


