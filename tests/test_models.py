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
