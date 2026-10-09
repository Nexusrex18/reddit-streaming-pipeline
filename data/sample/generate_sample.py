#!/usr/bin/env python3
"""Generates a realistic sample Reddit comments dataset for Option A.
Spans 4 simulated hours (2015-05-01 00:00 to 04:00 UTC) across several subreddits
with realistic positive, neutral, and negative comments.
"""
import csv
import os
import random
import uuid

SAMPLE_FILE = os.path.join(os.path.dirname(__file__), "reddit_sample.csv")

SUBREDDITS = [
    "AskReddit",
    "technology",
    "gaming",
    "worldnews",
    "movies",
    "science",
    "music",
    "books",
]

AUTHORS = [
    "alex_dev", "coder_sam", "data_nerd", "pixel_gamer", "cinephile99",
    "sky_walker", "quiet_reader", "tech_guru", "curious_cat", "nova_star",
    "quantum_mind", "retro_wave", "coffee_coder", "echo_chamber", "blue_horizon"
]

POSITIVE_COMMENTS = [
    "This is absolutely fantastic! Brilliant work by the developers.",
    "I love this so much, truly a masterpiece and great experience.",
    "Incredible performance, highly recommended for everyone!",
    "Such a wholesome and inspiring story, made my day.",
    "Outstanding progress, the best update we have received so far.",
    "Very impressed with the efficiency and quality. Excellent job!",
    "Amazing soundtrack and stunning visuals, loved every second.",
    "Super helpful explanation, thank you for sharing your great insights!",
    "I am really happy to see this community growing so positively.",
    "Top tier quality! Absolutely wonderful and exciting news."
]

NEUTRAL_COMMENTS = [
    "The conference is scheduled for next Tuesday at 3 PM UTC.",
    "According to the documentation, this feature was introduced in version 2.4.",
    "The dataset contains approximately 50,000 entries across 8 columns.",
    "Here is the link to the official repository and setup instructions.",
    "It depends on the specific hardware configuration and memory capacity.",
    "The average response time was measured at 120 milliseconds.",
    "They released the quarterly financial report this morning.",
    "This parameter can be configured via environment variables or a YAML file.",
    "The article mentions three different approaches to solving the problem.",
    "Standard protocol requires restarting the service after configuration changes."
]

NEGATIVE_COMMENTS = [
    "This is completely unacceptable and very disappointing.",
    "Terrible customer service, worst experience I have ever had.",
    "The update completely broke the system and causes constant crashes.",
    "Horrible design choice, it makes no sense and ruins usability.",
    "Such a waste of time and money, totally regretting this purchase.",
    "Very frustrating bug, nothing works as advertised.",
    "Poorly executed and full of annoying errors and glitches.",
    "I am extremely disappointed with how this was handled by management.",
    "Avoid this at all costs, it is buggy, slow, and totally broken.",
    "Worst release ever, full of regressions and completely unresponsive."
]

# Subreddit sentiment biases to give realistic variance across communities
SUBREDDIT_BIAS = {
    "technology": {"pos": 0.35, "neu": 0.45, "neg": 0.20},
    "gaming":     {"pos": 0.30, "neu": 0.30, "neg": 0.40},
    "science":    {"pos": 0.50, "neu": 0.45, "neg": 0.05},
    "movies":     {"pos": 0.45, "neu": 0.35, "neg": 0.20},
    "worldnews":  {"pos": 0.15, "neu": 0.35, "neg": 0.50},
    "music":      {"pos": 0.60, "neu": 0.30, "neg": 0.10},
    "books":      {"pos": 0.55, "neu": 0.35, "neg": 0.10},
    "AskReddit":  {"pos": 0.35, "neu": 0.40, "neg": 0.25},
}

START_UTC = 1430438400  # 2015-05-01 00:00:00 UTC
HOURS = 4
TOTAL_ROWS = 25000  # 25,000 rows across 4 hours (~6,250 rows/hour)


def generate():
    print(f"Generating {TOTAL_ROWS} sample rows across {HOURS} hours into {SAMPLE_FILE}...")
    fieldnames = ["id", "author", "body", "subreddit", "created_utc", "ups", "downs", "score"]
    
    # Generate timestamp pool ordered across the 4 hours
    total_seconds = HOURS * 3600
    timestamps = sorted([START_UTC + int(random.uniform(0, total_seconds)) for _ in range(TOTAL_ROWS)])
    
    with open(SAMPLE_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, ts in enumerate(timestamps):
            sub = random.choice(SUBREDDITS)
            bias = SUBREDDIT_BIAS[sub]
            
            # Pick sentiment based on community bias
            r = random.random()
            if r < bias["pos"]:
                body = random.choice(POSITIVE_COMMENTS)
            elif r < bias["pos"] + bias["neu"]:
                body = random.choice(NEUTRAL_COMMENTS)
            else:
                body = random.choice(NEGATIVE_COMMENTS)
                
            # Random score / upvotes
            ups = random.randint(1, 250)
            downs = random.randint(0, int(ups * 0.3))
            score = ups - downs
            
            writer.writerow({
                "id": f"c_{uuid.uuid4().hex[:8]}",
                "author": random.choice(AUTHORS),
                "body": body,
                "subreddit": sub,
                "created_utc": ts,
                "ups": ups,
                "downs": downs,
                "score": score
            })
            
    size_mb = os.path.getsize(SAMPLE_FILE) / (1024 * 1024)
    print(f"Successfully generated {SAMPLE_FILE} ({size_mb:.2f} MB, {TOTAL_ROWS} comments)")


if __name__ == "__main__":
    generate()
