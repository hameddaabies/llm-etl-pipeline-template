from pipeline.models import ProductEnriched, RawProduct


def test_raw_product_accepts_nulls():
    r = RawProduct(id="x", name="Thing")
    assert r.raw_brand is None
    assert r.price_usd is None


def test_enriched_rejects_bad_category():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ProductEnriched(
            id="x",
            name="Thing",
            brand="Acme",
            category="not-a-real-category",  # type: ignore[arg-type]
        )


def test_enriched_caps_tags():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ProductEnriched(
            id="x",
            name="Thing",
            brand="Acme",
            category="other",
            tags=["a"] * 9,
        )


def test_enriched_normalizes_tags():
    """Tags are lowercased, trimmed, de-duplicated, and stripped of blanks.

    Regression guard for the ``tags`` field validator: downstream dedup must
    not depend on the LLM emitting clean lowercase tags on every call.
    """
    p = ProductEnriched(
        id="x",
        name="Thing",
        brand="Acme",
        category="other",
        tags=["Wireless", " wireless ", "Compact", "", "  ", "COMPACT"],
    )
    # Casing/whitespace collapse to one entry each; blanks dropped; order kept.
    assert p.tags == ["wireless", "compact"]
