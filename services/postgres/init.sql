-- Schema for Reddit Comment Sentiment Scoring (Project 14)

CREATE TABLE IF NOT EXISTS subreddit_sentiment_hourly (
    subreddit VARCHAR(100) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    pos_count INT NOT NULL DEFAULT 0,
    neu_count INT NOT NULL DEFAULT 0,
    neg_count INT NOT NULL DEFAULT 0,
    total INT NOT NULL DEFAULT 0,
    pct_pos NUMERIC(5, 2) NOT NULL DEFAULT 0.00,
    pct_neu NUMERIC(5, 2) NOT NULL DEFAULT 0.00,
    pct_neg NUMERIC(5, 2) NOT NULL DEFAULT 0.00,
    classifier VARCHAR(50) NOT NULL DEFAULT 'vader',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subreddit, window_start, classifier)
);

CREATE INDEX IF NOT EXISTS idx_sub_sentiment_window ON subreddit_sentiment_hourly (window_start);
CREATE INDEX IF NOT EXISTS idx_sub_sentiment_sub ON subreddit_sentiment_hourly (subreddit);
