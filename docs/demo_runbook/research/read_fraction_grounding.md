# Research brief — grounding the read-fraction assumption in the demo's cost projection

## Why this matters

The OneBucket RAG Tiering demo's Act 3 Table B claims **~$25 K/yr** for
the lazy-tier architecture at 1 PB scale, vs **~$1.0 M/yr** for eager
replication. The 40× gap is driven by a single load-bearing assumption:

> **"In year 1, 30 % of corpus chunks get read at least once."**

That 30 % is currently a hand-wave. If the real number is 5 %, the lazy
tier looks even better (~$5 K/yr); if it's 80 %, the architecture's
advantage shrinks substantially. The audience will challenge this
number, and rightly so.

**Two classes of evidence** are needed:

1. **Synthetic / academic benchmarks** — what does the RAG literature
   show about what fraction of a corpus is actually retrieved over time
   for a representative query distribution?
2. **Practitioner / industry case studies** — what do real production
   RAG deployments at known companies (where they've published their
   architecture) report about chunk hit-rates, hot-set sizes, and
   long-tail behavior?

## The prompt — paste into Gemini Deep Research

```
RESEARCH OBJECTIVE
Context: a demonstration of a tiered cloud-object-storage product
positioned for retrieval-augmented generation (RAG) workloads. The
cost model hinges on what fraction of a document corpus is actually
read during a year of production query traffic — i.e. the "read
fraction" or "active set fraction." Rigorous evidence is needed to
ground this number.

Investigate the following, in priority order:

1. ACADEMIC / BENCHMARK EVIDENCE
   - What does the published RAG research literature say about chunk
     retrieval distributions? Specifically: across standard benchmarks
     (BEIR, MS MARCO, Natural Questions, KILT, TREC-DL, custom
     enterprise-RAG benchmarks like FinanceBench, DocRED-RAG,
     LegalBench-RAG), what fraction of the indexed chunks are ever
     retrieved as part of an answer over a representative query set?
   - Are there papers that explicitly characterize the long-tail
     distribution of chunk retrieval frequency? Citations preferred.
   - What does the "neural retrieval skew" literature show — i.e.
     when dense + sparse retrievers are deployed against large
     corpora, what's the practical concentration of hits on a small
     subset of chunks?

2. PRACTITIONER / INDUSTRY CASE STUDIES
   - Find publicly disclosed numbers from production RAG deployments
     at named companies. Specifically look for:
       * Klarna's customer support assistant (LLM/RAG, publicly
         discussed)
       * Shopify's Sidekick / merchant-assist
       * GitHub Copilot's chat / repo-grounded responses
       * Notion AI / Coda AI / similar workspace tools
       * Bloomberg's GPT-finance system
       * Morgan Stanley's wealth-management assistant (RAG over
         research notes)
       * Microsoft 365 Copilot (Graph-grounded retrieval)
   - For each, find: corpus size, chunk count, query volume, and any
     disclosed hit-rate / unique-chunks-touched numbers. Engineering
     blog posts, conference talks (Strange Loop, QCon, AWS reInvent,
     KubeCon, AI Engineer Summit), and earnings-call commentary all
     fair game.

3. SUPPORTING SIGNAL FROM ADJACENT DOMAINS
   - Web caching / CDN research on hit-rate vs. catalog size (the
     classic 80/20 power-law distributions — they're the same
     fundamental shape as RAG retrieval).
   - Recommendation systems' "long tail of unread items" literature.
   - Enterprise search hit-rate studies (pre-LLM era — Elasticsearch /
     Solr deployments).
   These should not be primary evidence but help frame what's
   plausible.

DELIVERABLES

Produce a structured response with:

  A. A table summarizing every concrete data point found, columns:
     [source citation, corpus size, time window, % chunks retrieved
      at least once, hot-set fraction (e.g. top 10% chunks serving
      X% of queries), notes]

  B. A best-estimate range for the "1 year, % chunks retrieved at
     least once" number, broken down by corpus type:
       - High-traffic conversational RAG (customer support style)
       - Domain-expert RAG (legal, financial, medical)
       - Enterprise knowledge-base RAG (Notion / Confluence style)
       - Code-grounded RAG (Copilot style)

  C. A short narrative paragraph (~150 words) characterizing the
     SHAPE of the distribution: is it heavy-tailed (Zipfian)? Bimodal?
     What does this imply about the lazy-tier-vs-eager-replication
     trade-off?

  D. The strongest counter-argument to "lazy tiering is better" —
     what scenarios or workloads actually do approach 100% read
     fraction, where eager replication would be the right call?

Cite primary sources. Distinguish "measured at scale" from "hypothetical."
Flag any cases where the company is the source of the methodology AND
the conclusion (those are biased).
```

## What to do with the output

1. Pick the **median** of the credible practitioner-reported numbers as
   the new Table B anchor. Update `variables.yml` (`operator_intent.workload`) or the
   runbook's Table B assumptions directly.
2. Add a footnote citing 2–3 of the strongest sources so the audience
   can verify.
3. Show the **range** in the demo, not just the point estimate:
   > "Industry-disclosed numbers range from <low>% to <high>%; the
   > $25 K/yr figure assumes <mid>%. Even at <high>%, the lazy tier
   > still costs <high-end> K/yr — well below eager replication's
   > $1 M/yr."

This makes the cost story durable against pushback.
