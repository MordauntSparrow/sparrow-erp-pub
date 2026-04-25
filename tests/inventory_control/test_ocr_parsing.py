from app.plugins.inventory_control.ocr import AmazonInvoiceParser, ParsedInvoice


SAMPLE_AMAZON_TEXT = """
Amazon EU S.a r.l.

Invoice Number: 123-1234567-1234567
Invoice Date: 2025-10-27

SKU123 | Widget One | Qty 2 | 10.00
SKU456 | Widget Two | Qty 1 | 25.50
"""


def test_amazon_invoice_parser_extracts_lines():
    parser = AmazonInvoiceParser()
    parsed: ParsedInvoice = parser.parse(SAMPLE_AMAZON_TEXT, source="amazon")

    assert parsed.invoice_number == "123-1234567-1234567"
    assert parsed.invoice_date == "2025-10-27"
    assert parsed.supplier_name == "Amazon"
    assert len(parsed.lines) == 2
    skus = {line.sku for line in parsed.lines}
    assert "SKU123" in skus and "SKU456" in skus

