-- Seed ~10 publishers. Idempotent: ON CONFLICT (feed_url) DO NOTHING.
-- poll_interval_seconds is a starting hint; adaptive polling tunes it over time.

INSERT INTO publishers (name, feed_url, site_url, category, publisher_score, poll_interval_seconds) VALUES
    ('Hacker News',         'https://news.ycombinator.com/rss',          'https://news.ycombinator.com',         'tech',     1.0,   600),
    ('Hacker News (Best)',  'https://hnrss.org/best',                    'https://news.ycombinator.com',         'tech',     1.1,   900),
    ('Lobsters',            'https://lobste.rs/rss',                     'https://lobste.rs',                    'tech',     1.0,   900),
    ('arXiv cs.AI',         'http://export.arxiv.org/rss/cs.AI',         'https://arxiv.org/list/cs.AI/recent',  'research', 0.9,  3600),
    ('arXiv cs.DC',         'http://export.arxiv.org/rss/cs.DC',         'https://arxiv.org/list/cs.DC/recent',  'research', 0.9,  3600),
    ('Julia Evans',         'https://jvns.ca/atom.xml',                  'https://jvns.ca',                      'tech',     1.2, 86400),
    ('Dan Luu',             'https://danluu.com/atom.xml',               'https://danluu.com',                   'tech',     1.2, 86400),
    ('Simon Willison',      'https://simonwillison.net/atom.xml',        'https://simonwillison.net',            'tech',     1.1, 21600),
    ('Brendan Gregg',       'https://www.brendangregg.com/blog/rss.xml', 'https://www.brendangregg.com',         'tech',     1.1, 86400),
    ('Martin Kleppmann',    'https://martin.kleppmann.com/feed.xml',     'https://martin.kleppmann.com',         'tech',     1.2, 86400)
ON CONFLICT (feed_url) DO NOTHING;
