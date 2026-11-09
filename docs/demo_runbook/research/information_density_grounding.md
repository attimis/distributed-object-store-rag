# Research brief — grounding the per-document information-density assumption

## Why this matters

The OneBucket RAG Tiering demo's Act 4 visual shows ~3-5% of MSFT's
211 chunks lit (warm) after the demo's questions. The cost story
implicitly leans on a deeper assumption: **most chunks in most
documents are never read because most chunks don't carry
answer-relevant information.** The architectural punchline ("you pay
to move what you read") is only as strong as the evidence that "what
you read" is a small fraction of "what you have."

This brief is the companion to `read_fraction_grounding.md`:

- `read_fraction_grounding.md` covers **corpus-level** retrieval skew
  ("of all the docs in your corpus, what % ever get touched")
- This brief covers **document-internal** information density ("of one
  document, what % of its chunks ever contribute to an answer")

Both numbers anchor the cost story. Defensible ranges are needed for both.

## The prompt — paste into Gemini Deep Research

```
RESEARCH OBJECTIVE
Context: a demonstration of a tiered cloud-object-storage product
positioned for retrieval-augmented generation (RAG) workloads. The
cost story claims that within a single retrieved document, only a
small fraction of chunks actually carry the answer-relevant
information that gets pulled into LLM context — typically 5-15%.
Rigorous evidence for this number from existing research is needed.

Investigate the following, in priority order:

1. ACADEMIC / IR FOUNDATIONS — information density in long-form
   documents
   - The classic "lead bias" in news retrieval: how concentrated is
     the answer-relevant content in news articles? Cite Nallapati et
     al., See et al., and the TextRank/LexRank literature.
   - For long-form documents (technical, financial, legal): what does
     extractive-summarization research show about what fraction of
     sentences carry the load? Specifically:
       * SEC 10-K filings: which sections dominate retrieval in
         financial RAG benchmarks (FinanceBench, FinQA, FinRED)?
       * Legal documents: which clauses dominate in LegalBench-RAG
         and similar?
       * Scientific papers: extractive-summarization papers on
         arXiv/PubMed showing abstract + conclusions dominate?
   - "Salience" research: what's the empirical distribution of
     sentence salience scores in long documents?

2. INDUSTRY RAG OBSERVATIONS — what production deployments report
   - Have Anthropic, OpenAI, Pinecone, Weaviate, LlamaIndex,
     Cohere, or Voyage AI published anything on per-document chunk
     access patterns? Look for engineering blogs, research papers,
     and conference talks (AI Engineer Summit, NeurIPS, ICLR).
   - Have any of these companies disclosed the "hot chunk fraction"
     for production RAG: at what % of chunks in a doc does retrieval
     concentrate?

3. ADJACENT DOMAINS — the long-tail / Zipfian shape generally
   - Web caching: classic 80/20 ("the heavy hitters") on object hit
     rates; cite the CDN research literature (Akamai, Cloudflare
     blog posts).
   - Recommendation systems: Netflix prize / "the long tail";
     fraction of catalog that drives the bulk of consumption.
   - Wikipedia article popularity: empirical Zipfian distributions
     well documented in Mediawiki research papers.
   These aren't direct evidence but help frame "is this distribution
   shape plausible for textual semantic density?"

4. INFORMATION-THEORETIC ANCHORS
   - Shannon entropy of long-form text: is most of the "information"
     concentrated in a small fraction of the document?
   - Compression ratios for technical docs: if a 10-K compresses to
     ~25% of its size with gzip, what does that suggest about
     informational redundancy?

DELIVERABLES

A. A table per document class [news / financial 10-K / legal / scientific],
   columns: [source citation, document size, retrieval method,
   % of chunks contributing to ≥1 answer, hot-set concentration
   (e.g. top 10% of chunks serving X% of retrievals), notes]

B. A best-estimate range for "% of chunks in a typical long-form
   document that ever get retrieved across a representative query
   set", broken down by domain.

C. A short narrative (~200 words) characterizing the shape of the
   distribution. Is it Zipfian? Is it bimodal? Is it dominated by
   document structure (abstract/conclusion) or by topical content?

D. A "section heat map" — which sections of a typical financial
   10-K (Item 1A Risk Factors, Item 7 MD&A, Item 8 Financial
   Statements, etc.) actually drive retrieval in published RAG
   benchmarks. This is directly relevant to the demo.

E. The strongest counter-evidence — workloads where every chunk
   matters (e.g. legal contract review, regulatory compliance
   crawls). Where does the "most chunks are dead weight" story
   break down?

Cite primary sources. Distinguish "measured" from "claimed." Flag
papers where the methodology is suspect.
```

## What to do with the output

1. **Update the cost section in `onebucket_rag_demo_runbook.md`**:
   substitute the hand-waved "30% read fraction in Year 1" with a
   researched range, AND add a NEW per-document anchor: "in a typical
   10-K, only ~X% of chunks carry the answer-relevant content for the
   types of questions analysts ask — so even within a queried doc,
   most chunks never get touched."

2. **Add 2-3 footnote citations** in the demo narration so the
   audience can verify on their own. The strongest credibility comes
   from naming sources mid-demo:
   > "BEIR retrieval benchmarks measure this directly. Microsoft
   > Research's published RAG case studies show the same shape."

3. **Add a sub-bullet to Act 4's cost-section narration**:
   > "And underneath all of this is a research-backed assumption: in
   > production RAG corpora, the *active set* — the chunks that ever
   > actually answer a question — is a small fraction of the whole.
   > That's not us hoping; that's industry-measured at scale."

## Related

- `read_fraction_grounding.md` — corpus-level retrieval skew (sister
  brief; cite alongside this one).
- The `multi_hop_demo_question` choice in `variables.yml` should be
  selected to retrieve chunks consistent with the research findings —
  i.e., ones the literature shows are typically high-salience for the
  document class. Re-validate question choice after running the
  research.
