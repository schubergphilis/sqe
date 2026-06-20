#!/usr/bin/env python3
"""Exact per-chapter analytics for the ebook. Prose vs code separated by ``` fences."""
import re, glob, os, statistics

CH_DIR = os.path.join(os.path.dirname(__file__), "..", "chapters")

def split_prose_code(text):
    prose, code = [], []
    in_fence = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            code.append(line)
            continue
        (code if in_fence else prose).append(line)
    return "\n".join(prose), "\n".join(code), code

def prose_only(prose):
    # drop headings, table rows, list markers, blockquotes, callout fences, images/links-only
    keep = []
    for ln in prose.splitlines():
        s = ln.strip()
        if not s: continue
        if s.startswith("#"): continue
        if s.startswith("|"): continue
        if s.startswith(":::"): continue
        if s.startswith("!["): continue
        keep.append(ln)
    return "\n".join(keep)

SENT_SPLIT = re.compile(r'[.!?]+(?:\s+|$)')
WORD = re.compile(r"[A-Za-z][A-Za-z'\-]*")
VOWELS = re.compile(r'[aeiouy]+', re.I)

def syllables(w):
    g = VOWELS.findall(w)
    n = len(g)
    if w.lower().endswith('e') and n > 1: n -= 1
    return max(1, n)

def analyze(path):
    raw = open(path, encoding="utf-8").read()
    prose, code, code_lines = split_prose_code(raw)
    body = prose_only(prose)
    words = WORD.findall(body)
    nwords = len(words)
    sents = [s for s in SENT_SPLIT.split(body) if WORD.findall(s)]
    nsent = len(sents)
    sent_lens = [len(WORD.findall(s)) for s in sents] or [0]
    avg_sl = statistics.mean(sent_lens) if sent_lens else 0
    short = sum(1 for l in sent_lens if l <= 8)
    longp = sum(1 for l in sent_lens if l >= 30)
    pct_short = 100*short/nsent if nsent else 0
    # readability (Flesch reading ease) over prose only
    nsyl = sum(syllables(w) for w in words) or 1
    flesch = 206.835 - 1.015*(nwords/max(1,nsent)) - 84.6*(nsyl/max(1,nwords))
    # code ratio
    code_real = sum(1 for l in code_lines if not l.lstrip().startswith("```"))
    prose_lines = sum(1 for l in prose.splitlines() if l.strip())
    total_content = code_real + prose_lines
    code_pct = 100*code_real/total_content if total_content else 0
    # headings & callouts on full text
    headings = len(re.findall(r'(?m)^#{2,3}\s', raw))
    callouts = len(re.findall(r'(?m)^:::', raw)) // 2
    deadends = len(re.findall(r':::\s*\{?\.?deadend', raw))
    excl_prose = body.count("!")
    return dict(file=os.path.basename(path), words=nwords, sents=nsent,
                avg_sl=avg_sl, pct_short=pct_short, longp=longp,
                flesch=flesch, code_pct=code_pct, headings=headings,
                callouts=callouts, deadends=deadends, excl=excl_prose)

rows = [analyze(p) for p in sorted(glob.glob(os.path.join(CH_DIR, "*.md")))]

out = []
out.append("# Ebook Analytics\n")
out.append("Prose metrics exclude code fences, headings, tables, lists, blockquotes.\n")
out.append("| Chapter | Prose words | Sentences | Avg sent len | % short (<=8w) | Long sents (>=30w) | Flesch | Code % of lines | H2/H3 | Callouts | Dead-ends | ! in prose |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
tot = dict(words=0, sents=0)
for r in rows:
    tot["words"]+=r["words"]; tot["sents"]+=r["sents"]
    out.append("| {file} | {words} | {sents} | {avg_sl:.1f} | {pct_short:.0f}% | {longp} | {flesch:.0f} | {code_pct:.0f}% | {headings} | {callouts} | {deadends} | {excl} |".format(**r))
out.append("")
out.append(f"**Totals:** {tot['words']:,} prose words, {tot['sents']:,} sentences, "
           f"~{tot['words']//250} pages @250 prose-words/pg, ~{tot['words']//200} min read @200wpm.\n")
out.append("## Reading the numbers\n")
out.append("- **Avg sent len**: voice target is rhythm. 12-18 is healthy. >22 means long-sentence drift.")
out.append("- **% short**: Jacob's voice leans on short sentences to land points. <20% = not enough punch.")
out.append("- **Flesch**: higher = easier. 50-65 is good for technical prose. <40 = dense.")
out.append("- **Long sents (>=30w)**: each is a candidate to split (voice: 'three clauses max').")
out.append("- **Code %**: very high = a chapter that's more listing than argument; very low in a how-it-works chapter = telling not showing.")
out.append("- **! in prose**: voice forbids exclamation marks in prose. Any non-zero needs a look (may be false positive from code-ish prose).")
print("\n".join(out))
