---
Feature Name: Chinese Fulltext Lexicon Expansion
Tracking Issue: TBD
Date: 2026-04-10
Author: @discord9
---

# Summary

This RFC proposes a bounded lexicon-expansion path for Chinese fulltext search.
The goal is to make queries such as `手机` behave more like users expect when indexed text contains tokens like `手机号`, while still keeping `@@` a term-oriented search operator rather than turning it into unrestricted raw substring search.

Phase 1 matcher semantics are already fixed and out of scope here. This RFC focuses only on recall.

# Motivation

After Phase 1, final matching is no longer the main problem. The remaining gap is index-side recall.

Today, behavior looks roughly like this:

- `手机号` can match because it is an indexed token.
- `登录` can match for the same reason.
- `登录手机号` can also match because query analysis can produce probes that line up with indexed tokens.
- `手机` still cannot reliably match `手机号` once the query really goes through SST/fulltext recall.

This is intuitive from the current architecture:

1. documents are tokenized and the index stores tokens such as `登录` and `手机号`
2. query terms are also analyzed into probes
3. bloom/fulltext recall only sees those probes
4. final `matches_term` correctness runs only after a candidate row is recalled

So the remaining problem is narrow but important:

> allow reasonable Chinese subterm queries to recall longer indexed tokens within a single token boundary

# Goals

1. Make Chinese queries like `手机` recall rows indexed with `手机号`.
2. Support natural substring-style Chinese queries such as `农业 -> 中国农业银行`.
3. Keep the solution compatible with the current bloom-first fulltext design.
4. Keep recall expansion bounded and predictable.

# Non-Goals

1. No change to already-fixed `matches_term` semantics.
2. No unrestricted raw substring search over original log text.
3. No cross-token-boundary matching in v1.
4. No full n-gram indexing redesign in v1.
5. No change to `@@` syntax.

# Why This Direction

This proposal is intentionally a middle path.

From a user-experience perspective, doing nothing after Phase 1 is not enough: users can understand why `手机号` matches, but still find it unintuitive that `手机` does not.

From an implementation perspective, we do not want to jump straight to a heavier redesign such as full n-gram indexing. That would broaden recall, but it would also increase index size, probe count, and rollout complexity.

Lexicon expansion is the proposed compromise:

- it improves obvious Chinese UX gaps such as `手机 -> 手机号`
- it keeps `@@` term-oriented instead of turning it into general substring search
- it preserves the current bloom-first architecture as much as possible
- it bounds complexity by expanding only within indexed tokens and under explicit limits

In short, the goal is not “maximum recall at any cost”. The goal is a practical next step that improves common Chinese search scenarios without paying the full complexity cost of a larger indexing redesign.

# Proposal

## Core Rule

For an eligible Chinese query token `q`, the system may expand recall probes to indexed lexicon tokens that contain `q` as a contiguous substring, but only within a single token.

Examples that should match by recall expansion:

- `手机` -> `手机号`
- `机号` -> `手机号`
- `农业` -> `中国农业银行`
- `银行` -> `中国农业银行`

Examples that should **not** match by recall expansion:

- `机号1888`
- `行账号`

Those are cross-boundary combinations from adjacent text, not reasonable subterms of one token.

## Query Flow

1. `@@` is still rewritten to `matches_term(...)`.
2. Query analysis produces the normal query tokens.
3. For eligible Chinese analyzed tokens, the engine looks up lexicon tokens according to token position:
   - a single-token query may use normal contains expansion
   - the first token in a multi-token query may only expand to tokens that use it as a suffix
   - the last token in a multi-token query may only expand to tokens that use it as a prefix
   - middle tokens do not expand
4. The expanded token set becomes the probe set for bloom/fulltext recall.
5. Final correctness still uses `matches_term`.

This keeps recall and correctness separate:

- **lexicon expansion** broadens candidate recall
- **`matches_term`** decides final truth

## Why a Lexicon

The bloom filter can test membership for known probes, but it cannot enumerate “tokens that contain this query as a substring”.
That requires a lexicon-like structure over indexed tokens.

The lexicon is therefore a recall-only structure with one job:

- given a query token, return exact or containing indexed tokens

It is not the final source of result correctness.

## Expansion Boundaries

To keep behavior and cost under control, expansion should be bounded by policy such as:

- only Han-containing analyzed query tokens are eligible
- shorter queries may need stricter guardrails than longer ones
- maximum number of expansions per query token
- maximum total additional probes per query

Single-character queries should not be ruled out categorically, but they are the most likely place to need stricter limits.

## Known Limitation

This proposal only expands within a single indexed token.
Queries that span token boundaries, even if they look visually contiguous in the original text, remain out of scope in v1.

Examples include:

- `机号188`
- `机号1888`
- `行账号`

This limitation is intentional. It keeps the semantics bounded and avoids pushing `@@` toward arbitrary substring search over adjacent content.

# Comparison with Similar Approaches

Search systems that are built around tokenization usually choose one of a few broad strategies for substring-like recall:

| Approach | User Experience | Implementation Cost | Main Tradeoff |
|---|---|---|---|
| Query-time token matching only | Simple, but `手机` cannot hit `手机号` | Low | Recall is too weak for common Chinese queries |
| Prefix-only expansion | Better for some cases like `手机 -> 手机号` | Low to medium | Misses cases like `农业 -> 中国农业银行` |
| Full n-gram indexing | Strong substring recall | High | Larger index, more probes, more noise |
| Raw substring scan | Very intuitive at first glance | Low index cost, high query cost | Expensive and too permissive for `@@` |
| Lexicon expansion (this RFC) | Covers common intuitive Chinese subterms | Medium | Requires extra lexicon structure and bounded query-time expansion |

This RFC prefers the last option because it sits in a more balanced place:

- stronger Chinese UX than token-only or prefix-only search
- much lower storage and rollout cost than full n-gram indexing
- clearer semantics than raw substring scan

That tradeoff is the main reason to propose lexicon expansion as the next step.

# Current Direction

## Expansion eligibility

- Any analyzed query token containing Han is eligible for expansion, even if it also contains ASCII.
- Mixed tokens like `手机号`, `trace号`, and `key号` follow the same rule as other Han-containing analyzed tokens.
- Single-character queries are not excluded categorically, but they are the most likely place to need stricter guardrails.

## Expansion behavior

- Expansion happens after query analysis, not on the raw full query string.
- Single-token queries may use normal contains expansion.
- Multi-token queries use outward-only expansion:
  - the first token may expand only to tokens that use it as a suffix, for example `登录 -> 立即登录`
  - the last token may expand only to tokens that use it as a prefix, for example `手机号 -> 手机号验证码`
  - middle tokens remain exact and do not expand
- Prefix-only expansion is considered too weak because it misses cases like `农业 -> 中国农业银行` and `机号 -> 手机号`.

## Bounds and fallback

- V1 keeps internal safety caps as implementation guardrails.
- Initial cap values can be hard-coded and adjusted later using real measurements.
- If limits are exceeded, the fallback should be the old exact-only behavior rather than partial truncation.

## Lexicon storage

- The lexicon should be stored per SST and per indexed column.
- It should live alongside the existing bloom/fulltext index artifacts, ideally in the same index file or Puffin package as a separate structure.
- During compaction, the output lexicon should be rebuilt from the union of input token sets, with deduplication and re-encoding.
- To keep storage impact modest, V1 should store only the token subset that matters for Chinese expansion.
- A practical starting point is Han-containing indexed tokens with length greater than one, using a compressed representation.

## Recall vs correctness

- Lexicon expansion is recall-only and exists to broaden probe generation for bloom/fulltext prefiltering.
- Final correctness remains solely the responsibility of `matches_term`.

## Acceptance examples

Should match:

- `手机` -> `手机号`
- `农业` -> `中国农业银行`
- `银行` -> `中国农业银行`

Should not match:

- `机号188`
- `机号1888`
- `行账号` when tokenization is like `中国农业银行 / 账号`

## Performance and rollout expectations

- On Chinese-analyzer columns, additional index overhead should ideally stay around 10% to 30%.
- Query latency should not regress in a clearly noticeable way for normal workloads.
- Storage should not approach a full duplicate of the existing index; per-SST deduplication, Han-focused token selection, and compression should keep it bounded.
- A feature flag or per-column opt-in should not be necessary if old and new index formats remain mutually compatible.

Note: the 10% to 30% number is a heuristic target, not a hard guarantee. A useful first-order approximation is to compare lexicon size against the existing bloom size. In the current implementation, bloom sizing is based on the number of distinct analyzed tokens in each segment, not the number of rows. That makes this target somewhat more plausible, because the proposed lexicon is per-SST and deduplicated, while the bloom index is sized repeatedly at segment granularity. Under the assumptions in this RFC — per-SST deduplication, Han-focused token selection, length > 1 filtering, and compression — it is plausible for the lexicon to remain in the low tens of percent for typical log-like Chinese columns, but this should still be validated with real measurements.

## Still Open

- The exact cap values.
- The exact physical lexicon representation.
- What `EXPLAIN` and debugging output should expose about expansion and fallback.

# Alternatives

## Only fix `matches_term`

Already done, but insufficient for `手机 -> 手机号` once recall truly goes through SST/fulltext index paths.

## Prefix-only expansion

Simpler and cheaper, but not enough for many natural Chinese queries.

## Full n-gram indexing

Broader recall, but much higher storage and probe cost.

## Raw substring search for `@@`

Rejected because it makes `@@` too broad and encourages cross-boundary matches.

# Conclusion

The next step for Chinese fulltext UX should be a bounded lexicon-expansion design, not another change to `matches_term`.

The intended behavior is simple:

- use token-based indexing as today
- allow eligible Chinese query tokens to expand to containing lexicon tokens
- keep expansion within a single token
- keep `matches_term` as the final correctness check

If this direction looks sound, the next design step is to define the lexicon representation, expansion limits, and probe-generation details needed to make this practical.
