# 01 — Project Concept

## The problem

INGOs (international NGOs) are increasingly launching their own impact investment funds — a fund vehicle alongside their grant-making programs. Most struggle with the *first close*: convincing the first set of LPs to commit capital.

The crowd of "primarily impact-focused" investors is small, well-known, and crowded. The actual lead-gen opportunity is the much larger pool of **generalist investors who already touch impact investing through co-investments** but who would not show up on a "list of impact investors." Those investors are warmer leads than they look, because they have already deployed capital into impact deals — just alongside someone else.

## The thesis

If we map the full investor network surrounding existing INGO impact funds — both the LPs that fund them and the co-investors that show up alongside them in portfolio company cap tables — we can surface generalist investors as warm leads for INGOs running first-close fundraising.

## Entity model (four layers)

```
                INGOs (catalogue)
                  │
                  │ runs / sponsors
                  ▼
            Impact Funds  ◄────────────── LPs / FoFs        (UPSTREAM)
                  │                       (the funders of the fund)
                  │ invests in
                  ▼
          Portfolio Companies  ◄────────  Co-investors      (DOWNSTREAM)
                                          (other investors in the same deal)
```

- **INGOs** — the parent NGOs that have launched impact funds (catalogue layer).
- **Impact Funds** — the fund vehicles. Usually 1:1 with an INGO but allow 1:N.
- **LPs / FoFs** — investors *into* the impact funds. This is the upstream layer.
- **Portfolio Companies** — companies the impact funds invested in.
- **Co-investors** — every other investor that appears alongside the impact fund in those portfolio companies. **This is the warm-lead layer.**

The same investor entity may appear as both an LP and a co-investor — that's a strong signal and should be modeled with one canonical investor record per organization.

## The product

A web dashboard that lets a user:

1. Browse INGO impact funds and see who funds them and what they have invested in.
2. See the co-investor network around each fund's portfolio.
3. **Lead-scoring view**: rank generalist (non-impact-primary) investors by how many INGO-impact-fund portfolio companies they have co-invested in. High count + non-impact-primary = warm lead.
4. **First-close prospect report** for a given INGO/fund: suggest investors based on overlap with similar funds' LP rosters and co-investor sets.

The product *is* the network. Build network views as the primary surface, not a side feature.

## What we are NOT building (v1)

- Team/people scraping for funds or portcos (lower-value than money flows for the impact lead-gen thesis).
- Round-by-round historical reconstruction (start with current cap tables; deepen later).
- Real-time data; daily-or-slower runs are fine.
- Mobile-first UX; this is a desktop research tool.

Skipping these for v1 will save weeks. They can be added once the core network is working.

## Success criteria for v1

- 10–20 INGO impact funds onboarded.
- At least 80% of each fund's portfolio companies captured.
- At least one investor link (LP or co-investor) for >50% of the fund/portco network.
- Network view loads in <2s and supports filtering by impact-primary vs generalist.
- Lead-scoring view exports a CSV usable by a fundraising operator.
