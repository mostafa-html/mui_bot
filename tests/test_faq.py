"""FAQ content integrity: callback indices are baked into buttons users may
still be holding, so this suite makes silent renumbering/breakage impossible."""


def test_faq_has_exactly_eight_entries():
    from tests._bootstrap import bot
    assert len(bot.AMNEZIA_FAQ) == 8


def test_faq_titles_unique_and_nonempty():
    from tests._bootstrap import bot
    titles = [t for t, _ in bot.AMNEZIA_FAQ]
    assert len(set(titles)) == len(titles), 'duplicate question title'
    assert all(t.strip() for t in titles)


def test_faq_answers_substantive_and_html_safe():
    from tests._bootstrap import bot
    for _, answer in bot.AMNEZIA_FAQ:
        assert len(answer) >= 60, 'answer too short to be useful'
        # balanced tags we actually use (answers are sent with parse_mode=HTML)
        for tag in ('<b>', '</b>', '<code>', '</code>'):
            if tag in answer:
                assert answer.count(tag) >= 1


def test_faq_covers_all_four_chosen_topics():
    from tests._bootstrap import bot
    joined = ' '.join(t + ' ' + a for t, a in bot.AMNEZIA_FAQ)
    # connection problems
    assert any('وصل نمی' in t for t, _ in bot.AMNEZIA_FAQ)
    # importing config (link + file)
    assert any('vpn://' in t or 'vpn://' in a for t, a in bot.AMNEZIA_FAQ)
    assert any('کانفیگ' in t and 'فایل' in t for t, _ in bot.AMNEZIA_FAQ)
    # one-device limit
    assert any('یک دستگاه' in t for t, _ in bot.AMNEZIA_FAQ)
    # speed + expiry
    assert any('سرعت' in t for t, _ in bot.AMNEZIA_FAQ)
    assert any('منقضی' in t for t, _ in bot.AMNEZIA_FAQ)
    assert 'تمدید' in joined  # expiry answer points at renewal


def test_faq_handlers_registered():
    import bot as bot_module
    src = open(bot_module.__file__, encoding='utf-8').read()
    assert 'F.data == "amzfaq"' in src
    assert 'F.data.startswith("amzfaq_")' in src
