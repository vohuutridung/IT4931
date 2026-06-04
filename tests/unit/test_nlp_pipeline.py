from speed.nlp_pipeline import analyze_sentiment, detect_language, enrich_post, extract_keywords, MODEL_VERSION, _sentiment


def is_vietnamese_model():
    return _sentiment is not None and "phobert" in str(MODEL_VERSION).lower()


def test_sentiment_fallback_positive():
    text = "Bài viết này thật sự rất tuyệt vời và hữu ích!" if is_vietnamese_model() else "This is a great and amazing post"
    result = analyze_sentiment(text)
    assert result["label"] in {"positive", "neutral"}
    assert -1.0 <= result["score"] <= 1.0


def test_sentiment_fallback_negative_finance_terms():
    text = "Cảnh báo rủi ro sập đổ và khủng hoảng tồi tệ" if is_vietnamese_model() else "Beware a dollar confidence crisis and catastrophic warming risk"
    result = analyze_sentiment(text)
    assert result["label"] == "negative"
    assert result["score"] < 0


def test_sentiment_fallback_vietnamese_terms():
    positive = analyze_sentiment("Bài viết rất hay và tuyệt vời ❤️")
    negative = analyze_sentiment("Dịch vụ tệ quá, rất thất vọng")
    assert positive["score"] > 0
    assert negative["score"] < 0


def test_keywords_are_limited():
    keywords = extract_keywords("social media pipeline pipeline analytics", top_n=2)
    assert len(keywords) <= 2
    assert "pipeline" in keywords


def test_enrich_post_schema():
    enriched = enrich_post({"post_id": "p1", "content": "Great social analytics"})
    assert enriched["post_id"] == "p1"
    assert "sentiment_score" in enriched
    assert isinstance(enriched["keywords"], list)


def test_sentiment_sample_f1_at_least_080():
    if is_vietnamese_model():
        positive = [
            "Tôi rất thích sản phẩm tuyệt vời này",
            "Một bài viết cực kỳ hay và ý nghĩa",
            "Rất hài lòng với chất lượng dịch vụ tốt",
            "Sự phát triển mạnh mẽ và an toàn",
        ] * 25
        negative = [
            "Tôi cực kỳ ghét dịch vụ tồi tệ này",
            "Bài viết quá chán và thất vọng",
            "Rất không hài lòng với chất lượng kém",
            "Nguy cơ rủi ro cao và tổn thất lớn",
        ] * 25
    else:
        positive = [
            "I love this excellent social media launch",
            "Great analytics and amazing pipeline results",
            "Happy users like the best product update",
            "This is good, excellent, and useful",
        ] * 25
        negative = [
            "I hate this terrible social media launch",
            "Bad analytics and awful pipeline results",
            "Angry users dislike the worst product update",
            "This is poor, terrible, and unusable",
        ] * 25
    labels = ["positive"] * len(positive) + ["negative"] * len(negative)
    predictions = [
        "positive" if analyze_sentiment(text)["score"] > 0 else "negative"
        for text in positive + negative
    ]
    true_positive = sum(1 for truth, pred in zip(labels, predictions) if truth == pred == "positive")
    false_positive = sum(1 for truth, pred in zip(labels, predictions) if truth == "negative" and pred == "positive")
    false_negative = sum(1 for truth, pred in zip(labels, predictions) if truth == "positive" and pred == "negative")
    precision = true_positive / max(true_positive + false_positive, 1)
    recall = true_positive / max(true_positive + false_negative, 1)
    f1 = 2 * precision * recall / max(precision + recall, 1e-9)
    assert f1 >= 0.80
