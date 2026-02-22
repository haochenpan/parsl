# Diaspora Examples CLI

Single-file CLI: `diaspora.py` (setup, context, and clear).

## Usage

```bash
# from repo root
cd examples/diaspora

# one-time setup
python diaspora.py setup

# fetch context messages from the last 24 hours
python diaspora.py context --topic topic-parsl-local --time-horizon $(( ($(date +%s) - 86400) * 1000 ))

# fetch context messages from the last hour
python diaspora.py context --topic topic-parsl-local --time-horizon $(( ($(date +%s) - 3600) * 1000 ))

# fetch context messages from the last 5 minutes
python diaspora.py context --topic topic-parsl-local --time-horizon $(( ($(date +%s) - 300) * 1000 ))

# recreate topic
python diaspora.py clear --topic topic-parsl-local
```
