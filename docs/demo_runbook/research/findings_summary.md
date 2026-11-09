# Research findings — consolidated synthesis (May 2026)

This document is the **canonical reference** for the cost-story
numbers used in the demo's Table B and Act 3 narration. All claims
here were gathered with Gemini Deep Research (May 2026); the primary
sources are cited inline so each claim can be verified independently.

The demo runbook and `variables.yml` substitute these numbers into
the cost projection; the agent cites the named sources live during
Act 3 Beat 2.

## The three load-bearing claims, with evidence

### Claim 1 — Corpus-level retrieval skew

**Claim**: 20-40% of corpus chunks are ever retrieved in Year 1 of
production RAG traffic for enterprise/domain-expert workloads. The
demo uses 30% as the mid-range.

**Primary source**: Jin et al., 2024, *RAGCache: Efficient Knowledge
Caching for Retrieval-Augmented Generation*. Peking University.
[arXiv:2404.12457](https://arxiv.org/abs/2404.12457).

**Key finding**: On MMLU and Natural Questions benchmarks, the top
**3% of documents serve 60% of all retrieval requests** — a 20×
concentration vs. uniform distribution.

**Supporting evidence**:
- Ruparel et al., 2025 multi-tenant RAG simulation: 74.26% LFU
  hit-rate at Zipfian s=1, implying ~25-30% of corpus is the active
  set.
- Morgan Stanley wealth-management RAG (publicly disclosed): 200+
  analyst teams query a narrow recent-data slice across a
  100,000+ document corpus.

**By workload type**:
- Conversational / customer support: **10-20%**
- Domain-expert / enterprise knowledge (this demo's case): **20-40%**
- Code-grounded / Copilots: **30-50%**

### Claim 2 — Per-document information density

**Claim**: Within a single long-form document, only a small fraction
of chunks ever contributes to a retrieved answer.

**Primary source**: TextRank (Mihalcea) and LexRank (Erkan & Radev) —
graph-based centrality models with PageRank-style eigenvalue
calculation. Established IR literature.

**Key finding**: Sentence centrality is concentrated in **5-15%** of
sentences across most document classes; the rest is conversational
filler, redundant explanations, and supporting context.

**By document class**:
- News articles (lead bias): **5-10%** — Lead-3 achieves SOTA ROUGE
  on CNN/Daily Mail
- **SEC 10-K filings: 10-15%** (the demo's case) — FinanceBench data
- Scientific papers: **15-20%** — concentrated in abstract,
  introduction, conclusion
- **Legal contracts: 100%** — every clause potentially load-bearing
  (counter-evidence)

### Claim 3 — Cache hit-rate amplification

**Claim**: Steady-state cache hit rate reaches **70-90% within
hours** of production traffic on a bounded corpus.

**Primary source**: Lin et al., 2025, *Cache Mechanism for Agent RAG
Systems* (ARC framework). [arXiv:2511.02919](https://arxiv.org/abs/2511.02919).

**Key finding**: **79.8% has-answer rate using 0.015% of total
storage**. Semantic caching groups syntactic variations of the same
intent into single cache keys.

**Supporting evidence**:
- LLMSigCache (arXiv:2602.19811): 82% cache hit-rate on TPC-DS, SSB,
  NYC TLC workloads via semantic intent matching
- Redis semantic caching: 68.8% reduction in LLM API calls
- RAGCache hierarchical (Jin et al., 2024): 4× reduction in TTFT, 2.1×
  throughput vs. vLLM baseline
- Akamai / Cloudflare CDN data: same Pareto distribution on web cache
  hit rates

**Hit-rate trajectory**: logarithmic saturation curve. At t=0, hit
rate is 0%. Sharp "knee" within the first hours of traffic as the
Zipfian head is cached. Steady-state asymptote at 70-90%.

## SEC 10-K Retrieval Heat-Map (FinanceBench-derived)

For SEC 10-K corpora (this demo's content), retrieval is
hyper-concentrated in just three sections:

| Section | Retrieval Share | Storage Tier | Why |
|---|---|---|---|
| **Item 7 (MD&A)** | **40-45%** | Hot (pinned) | Most narrative; explains the "why" behind financial shifts |
| **Item 1A (Risk Factors)** | **25-30%** ← demo's question | Hot (pinned) | Scenario planning: supply chain, regulatory, market exposure |
| **Item 8 (Financial Statements)** | **15-20%** | Hot | Numerical extraction; balance sheets, income, cash flow |
| Item 1 (Business) | 5-10% | Warm | Foundational context |
| Item 9A & Exhibits | <5% | Cold / Archive | Procedural controls, legal boilerplate |

**Source**: FinanceBench (Islam et al., 2023, Patronus AI) — 10,000+
expert-annotated QA across 361 filings from 40 publicly traded
companies. [arXiv:2311.11944](https://arxiv.org/abs/2311.11944).

**Implication**: A markdown-aware chunking strategy that extracts
SEC structured headings as metadata can route chunks
deterministically — Items 7/1A/8 pinned hot, Item 9A and exhibits
straight to cold storage on ingest.

## Counter-evidence — where lazy-tier breaks down

| Workload | Why lazy-tier fails | Architecture instead |
|---|---|---|
| **Legal contract review** | Every clause is load-bearing; cross-references form a dependency graph; standard 512-token chunking severs conditional statements | Full corpus in hot tier; clause-level segmenters (LlamaParse, Unstructured IO, Kira) |
| **Forensic discovery** | Goal is finding *the* needle in the haystack; long-tail is the target | Full-text mandatory; no lazy-tier |
| **Regulatory compliance crawls** (SR 11-7, pharma submission review) | Metric is 100% recall (proving exhaustive coverage), not precision | Disable lazy-tier or bound by compliance framework |

**Citation for legal RAG limits**: "Standard RAG fails terribly on
legal contracts" (community RAG/LangChain analyses) +
[Future AGI, *How to Build a Contract Review RAG Agent in 2026*](https://futureagi.com/blog/contract-review-rag-build-evaluate-2026/).

## Three sources to name live during Act 3 narration

These three are the highest-credibility / most-recognizable proof
points — use them verbatim in the demo:

1. **RAGCache (Jin et al., Peking University, arXiv:2404.12457)** —
   "Top 3% of documents serve 60% of requests on MMLU — 20× more
   concentration than uniform."

2. **ARC framework (Lin et al., 2025, arXiv:2511.02919)** —
   "79.8% cache hit rate using less than 0.015% of storage footprint."

3. **Akamai / Cloudflare CDN data** — "Same Pareto distribution
   that built the CDN industry. We're just moving it from web
   traffic to retrieval traffic."

## Regenerating the research

To regenerate or verify any specific claim, re-run the prompt in
`research/read_fraction_grounding.md` or
`research/information_density_grounding.md` and compare against
this synthesis.
