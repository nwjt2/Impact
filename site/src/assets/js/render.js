/* INGO First-Close Tool — shared client-side render helpers.
 *
 * Loaded as a regular script (not a module) on each detail page +
 * on the landing tiles. Exposes a single `IFC` global with all helpers
 * and renderers.
 */
(function (root) {
  "use strict";

  // ---- Helpers ------------------------------------------------------------

  function esc(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function escAttr(s) { return esc(s); }
  function fmtDate(iso) { return iso ? String(iso).slice(0, 10) : ""; }
  function fmtUSDm(v) {
    if (v == null) return "";
    if (v >= 1000) return "$" + (v / 1000).toFixed(1) + "B";
    return "$" + v + "M";
  }
  function monthPrecision(iso, monthOnly) {
    if (!iso) return "";
    var d = fmtDate(iso);
    return monthOnly ? d.slice(0, 7) : d;
  }
  function chip(text, cls) {
    return '<span class="chip ' + (cls || "") + '">' + esc(text) + '</span>';
  }
  function badge(text, cls) {
    return '<span class="badge ' + (cls || "") + '">' + esc(text) + '</span>';
  }
  function sourceLink(url, label) {
    if (!url) return "";
    var lbl = label || "Public source";
    return '<a class="source-link" href="' + escAttr(url) +
      '" target="_blank" rel="noopener noreferrer">' + esc(lbl) + ' &#x2197;</a>';
  }
  function loadJSON(id) {
    var el = document.getElementById(id);
    if (!el) return null;
    try { return JSON.parse(el.textContent); } catch (e) { return null; }
  }

  // ---- Vocabulary maps ----------------------------------------------------

  var SECTOR_LABELS = {
    fi: "Financial Inclusion", agri: "Agriculture", sgb: "Small & Growing Business",
    edu: "Education", health: "Health", climate: "Climate", energy: "Energy",
    water: "Water & Sanitation", housing: "Housing", gender_lens: "Gender Lens",
    gender: "Gender Lens", jobs: "Jobs", tech_for_good: "Tech for Good",
    generalist: "Generalist", infra: "Infrastructure", resilience: "Resilience",
    fragile_states: "Fragile States", nature: "Nature", conservation: "Conservation",
    fintech: "Fintech", msme: "MSME", manufacturing: "Manufacturing",
    humanitarian: "Humanitarian",
  };
  function sectorLabel(slug) {
    return SECTOR_LABELS[slug] || String(slug).replace(/_/g, " ");
  }

  var GEO_LABELS = {
    global: "Global", africa: "Africa", "sub-saharan-africa": "Sub-Saharan Africa",
    ssa: "Sub-Saharan Africa", "east-africa": "East Africa", "west-africa": "West Africa",
    "southern-africa": "Southern Africa", asia: "Asia", "south-asia": "South Asia",
    "east-asia": "East Asia", "southeast-asia": "Southeast Asia", latam: "Latin America",
    "latam-caribbean": "LatAm & Caribbean", mena: "MENA", "global-south": "Global South",
    europe: "Europe", eap: "East Asia & Pacific", us: "United States", uk: "United Kingdom",
  };
  function geoLabel(slug) {
    return GEO_LABELS[slug] || String(slug).replace(/_/g, " ").replace(/-/g, " ");
  }

  var KIND_LABELS = {
    rfp: "RFP", open_call: "Open call", rolling_application: "Rolling application",
    board_meeting: "Board meeting", template_revision: "Template revision",
    regulator_filing: "Regulator filing", conference_deadline: "Conference",
  };

  // Verb that describes what the date *means* for each kind, so the UI
  // never displays a bare date that the reader has to interpret.
  // RFPs/open calls/regulator filings are true deadlines (action by).
  // Board meetings, conferences and template reissues are *event* dates,
  // not application deadlines.
  var DATE_VERBS = {
    rfp: "Apply by",
    open_call: "Apply by",
    regulator_filing: "File by",
    board_meeting: "Board meets",
    conference_deadline: "Event starts",
    template_revision: "Reissued",
    rolling_application: "",
  };
  function dateVerb(kind) {
    return DATE_VERBS[kind] != null ? DATE_VERBS[kind] : "Date";
  }

  var RECURRING_GLOSS = {
    rfp: { rolling: "Rolling applications", quarterly: "Applications open quarterly",
      annual: "Applications open annually", biennial: "Applications open every 2 years",
      monthly: "Applications open monthly" },
    rolling_application: { rolling: "Rolling — no fixed deadline",
      quarterly: "Reviewed quarterly", annual: "Reviewed annually",
      biennial: "Reviewed every 2 years", monthly: "Reviewed monthly" },
    open_call: { rolling: "Rolling open call", quarterly: "Call opens quarterly",
      annual: "Call opens annually", biennial: "Call opens every 2 years",
      monthly: "Call opens monthly" },
    board_meeting: { rolling: "Board meets as needed", quarterly: "Board meets quarterly",
      annual: "Board meets annually", biennial: "Board meets every 2 years",
      monthly: "Board meets monthly" },
    template_revision: { rolling: "Updated as needed",
      quarterly: "Template reissued quarterly", annual: "Template reissued annually",
      biennial: "Template reissued every 2 years", monthly: "Template reissued monthly" },
    regulator_filing: { rolling: "Ongoing filing obligation",
      quarterly: "Filing due quarterly", annual: "Filing due annually",
      biennial: "Filing due every 2 years", monthly: "Filing due monthly" },
    conference_deadline: { rolling: "Rolling registration", quarterly: "Quarterly event",
      annual: "Annual event", biennial: "Biennial event", monthly: "Monthly event" },
  };

  function deadlineStatus(d) {
    if (d.deadline_date) return fmtDate(d.deadline_date);
    var next = d.next_occurrence;
    if (d.recurring === "one_off") {
      return next ? "Expected ~" + fmtDate(next) : "Expected — date TBA";
    }
    var byKind = RECURRING_GLOSS[d.kind] || RECURRING_GLOSS.rfp;
    var gloss = byKind[d.recurring] || "Check link";
    if (next) return gloss + " · next ~" + fmtDate(next);
    return gloss;
  }

  function countryName(meta, code) {
    if (!code) return "";
    var cc = (meta && meta.country_display_names) || {};
    return cc[code] || code;
  }

  // ---- Card renderers -----------------------------------------------------

  // Render the per-card "network LP edges" disclosure block. The registry-side
  // commit lists on DFI / foundation / family-office records only carry
  // commitments to INGO-sponsored funds; the fund_lps.csv pipeline (network
  // LP edges) covers both INGO-backed and non-INGO comparable peer impact
  // funds, with a public source URL on every edge. Surface those here so an
  // entity like Shell Foundation (0 registry commits, 11 network LP edges)
  // is no longer rendered as "no commitments located."
  //
  // The detail page attaches d._network_lp_funds before calling the card
  // renderer; each entry: {fund_slug, fund_name, ingo_backed, source_url}.
  function renderNetworkLpBlock(d) {
    var funds = d._network_lp_funds || [];
    if (!funds.length) return "";
    var prefix = (typeof window !== "undefined" && window.IFC_PATH_PREFIX) || "/";
    var lis = funds.map(function (f) {
      var href = prefix + "peer-funds/#fund-" + f.fund_slug;
      var nameLink = '<a href="' + escAttr(href) + '" class="commit-fund">' +
        esc(f.fund_name) + '</a>';
      var bits = [nameLink];
      bits.push('<span class="commit-ingo muted">' +
        (f.ingo_backed ? "INGO-sponsored" : "non-INGO comparable") + '</span>');
      if (f.source_url) bits.push(sourceLink(f.source_url, "source"));
      return '<li class="commit-row">' + bits.join('<span class="sep">·</span>') + '</li>';
    });
    var summary = "Peer-fund LP commits traceable in scraped LP data (" + funds.length + ")";
    return '<details class="dfi-commits"><summary>' + summary +
      '</summary><ul class="commit-list">' + lis.join("") + '</ul>' +
      '<p class="muted" style="font-size:0.78rem;margin-top:0.4rem;">' +
      'Drawn from network LP edges (fund_lps.csv pipeline). Distinct from the ' +
      'registry-side commitment list above, which is curated to INGO-sponsored funds only.' +
      '</p></details>';
  }


  function renderFundCard(f, meta) {
    var parts = [];
    parts.push('<h3 class="fund-name" id="fund-' + escAttr(f.slug) + '">' + esc(f.name) + '</h3>');

    var hdrBits = [];
    if (f.parent_ingo) hdrBits.push('<span class="fund-ingo">' + esc(f.parent_ingo) + '</span>');
    if (f.parent_ingo_country) hdrBits.push(badge(countryName(meta, f.parent_ingo_country), "badge-country"));
    if (f.status) hdrBits.push(badge(f.status.replace(/_/g, " "), "badge-status badge-status-" + esc(f.status)));
    if (f.vehicle_type) hdrBits.push(badge(f.vehicle_type.replace(/_/g, " "), "badge-vehicle"));
    if (f.validation_status === "unverified") hdrBits.push(badge("Unverified — limited public detail", "badge-unverified"));
    parts.push('<div class="fund-sub">' + hdrBits.join(" ") + '</div>');

    var facts = [];
    if (f.first_close_date) {
      facts.push('<span class="fact"><span class="fact-label">First close</span> ' +
        esc(monthPrecision(f.first_close_date, f.first_close_date_month_only)) + '</span>');
    } else if (f.vintage) {
      facts.push('<span class="fact"><span class="fact-label">Vintage</span> ' + esc(f.vintage) + '</span>');
    }
    if (f.vintage && f.first_close_date) {
      facts.push('<span class="fact"><span class="fact-label">Vintage</span> ' + esc(f.vintage) + '</span>');
    }
    if (f.size_usd_m != null) {
      facts.push('<span class="fact"><span class="fact-label">Size</span> ' + esc(fmtUSDm(f.size_usd_m)) + '</span>');
    }
    if (f.manager && f.manager !== f.parent_ingo) {
      facts.push('<span class="fact"><span class="fact-label">Manager</span> ' + esc(f.manager) + '</span>');
    }
    if (facts.length) parts.push('<div class="fund-facts">' + facts.join("") + '</div>');

    var tagBits = [];
    (f.sector_tags || []).forEach(function (t) { tagBits.push(chip(sectorLabel(t), "chip-sector")); });
    (f.geo_tags || []).forEach(function (t) { tagBits.push(chip(geoLabel(t), "chip-geo")); });
    if (tagBits.length) parts.push('<div class="fund-tags">' + tagBits.join("") + '</div>');

    var rels = [];
    if (f.anchor_lp) rels.push('<span class="rel"><span class="rel-label">Anchor LP:</span> ' + esc(f.anchor_lp) + '</span>');
    if (f.sub_advisor) rels.push('<span class="rel"><span class="rel-label">Sub-advisor:</span> ' + esc(f.sub_advisor) + '</span>');
    if (f.placement_agent) rels.push('<span class="rel"><span class="rel-label">Placement agent:</span> ' + esc(f.placement_agent) + '</span>');
    if (f.named_lps && f.named_lps.length) {
      rels.push('<span class="rel"><span class="rel-label">Named LPs:</span> ' + esc(f.named_lps.join(", ")) + '</span>');
    }
    if (rels.length) parts.push('<div class="fund-rels">' + rels.join('<span class="sep">·</span>') + '</div>');

    if (f.notes) {
      var notes = String(f.notes).trim().replace(/\s+/g, " ");
      parts.push('<p class="fund-notes">' + esc(notes) + '</p>');
    }

    var footBits = [];
    if (f.public_source_url) footBits.push(sourceLink(f.public_source_url, "Public source"));
    if (f.last_seen_at) footBits.push('<span class="muted">Last seen ' + esc(fmtDate(f.last_seen_at)) + '</span>');
    if (footBits.length) parts.push('<div class="fund-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    var clickCls = f.public_source_url ? " card-clickable" : "";
    var dataAttr = f.public_source_url ? ' data-source-url="' + escAttr(f.public_source_url) + '"' : "";
    return '<article class="card fund-card' + clickCls + '" id="fund-card-' + escAttr(f.slug) + '"' + dataAttr + '>' + parts.join("") + '</article>';
  }

  function renderDfiCard(d, meta) {
    var parts = [];
    var defunct = d.status === "defunct";
    parts.push('<h3 class="dfi-name">' + esc(d.name) + '</h3>');
    var hdrBits = [];
    if (defunct) hdrBits.push(badge("DEFUNCT", "badge-defunct"));
    if (d.country) hdrBits.push(badge(countryName(meta, d.country), "badge-country"));
    if (d.policy_remit) hdrBits.push(badge(d.policy_remit, "badge-remit"));
    parts.push('<div class="dfi-sub">' + hdrBits.join(" ") + '</div>');

    if (defunct && d.defunct_note) {
      var sinceTxt = d.defunct_since ? " (since " + esc(fmtDate(d.defunct_since)) + ")" : "";
      parts.push('<div class="dfi-defunct-note"><strong>No longer an active LP' + sinceTxt + '.</strong> ' + esc(d.defunct_note) + '</div>');
    }

    var stats = [];
    var commits = d.ingo_gp_commits || [];
    if (commits.length > 0) {
      stats.push('<span class="stat"><span class="stat-num">' + commits.length + '</span><span class="stat-label">INGO-GP commitments on record</span></span>');
    }
    if (d.ingo_gp_commit_count_5y != null && d.ingo_gp_commit_count_5y > 0) {
      stats.push('<span class="stat"><span class="stat-num">' + d.ingo_gp_commit_count_5y + '</span><span class="stat-label">last 5 years</span></span>');
    }
    if (d.ingo_gp_commit_count_10y != null && d.ingo_gp_commit_count_10y > 0) {
      stats.push('<span class="stat"><span class="stat-num">' + d.ingo_gp_commit_count_10y + '</span><span class="stat-label">last 10 years</span></span>');
    }
    if (!defunct) {
      var tr = d.typical_ticket_usd_m_range;
      if (tr && tr.n != null && tr.n >= 2) {
        stats.push('<span class="stat"><span class="stat-num">' + fmtUSDm(tr.min) + '–' + fmtUSDm(tr.max) + '</span><span class="stat-label">observed ticket (n=' + tr.n + ')</span></span>');
      } else if (d.stated_ticket_usd_m_min != null || d.stated_ticket_usd_m_max != null) {
        var rangeStr = "";
        if (d.stated_ticket_usd_m_min != null && d.stated_ticket_usd_m_max != null) {
          rangeStr = fmtUSDm(d.stated_ticket_usd_m_min) + "–" + fmtUSDm(d.stated_ticket_usd_m_max);
        } else if (d.stated_ticket_usd_m_min != null) {
          rangeStr = "from " + fmtUSDm(d.stated_ticket_usd_m_min);
        } else {
          rangeStr = "up to " + fmtUSDm(d.stated_ticket_usd_m_max);
        }
        stats.push('<span class="stat"><span class="stat-num">' + esc(rangeStr) + '</span><span class="stat-label">stated ticket preference</span></span>');
      }
    }
    if (stats.length) parts.push('<div class="dfi-stats">' + stats.join("") + '</div>');

    var netLp = d._network_lp_funds || [];
    if (commits.length > 0) {
      var lis = commits.map(function (c) {
        var prefix = (typeof window !== "undefined" && window.IFC_PATH_PREFIX) || "/";
        var inner = c.peer_fund_slug
          ? '<a href="' + prefix + 'peer-funds/#fund-' + escAttr(c.peer_fund_slug) + '" class="commit-fund">' + esc(c.peer_fund_name) + '</a>'
          : '<span class="commit-fund">' + esc(c.peer_fund_name) + '</span>';
        var bits = [inner];
        if (c.parent_ingo) bits.push('<span class="commit-ingo muted">' + esc(c.parent_ingo) + '</span>');
        if (c.amount_usd_m != null) bits.push('<span class="commit-amt">' + esc(fmtUSDm(c.amount_usd_m)) + '</span>');
        if (c.commit_date) bits.push('<span class="commit-date muted">' + esc(fmtDate(c.commit_date)) + '</span>');
        if (c.role) bits.push(badge(c.role, "badge-role"));
        if (c.public_source_url) bits.push(sourceLink(c.public_source_url, "source"));
        return '<li class="commit-row">' + bits.join('<span class="sep">·</span>') + '</li>';
      });
      parts.push('<details class="dfi-commits" open><summary>Disclosed INGO-GP commitments (' + commits.length + ')</summary><ul class="commit-list">' + lis.join("") + '</ul></details>');
    } else if (netLp.length > 0) {
      parts.push('<p class="dfi-commits-empty muted">No INGO-GP commitments curated to the registry yet, but this DFI is named as an LP on ' + netLp.length + ' peer impact fund' + (netLp.length === 1 ? "" : "s") + ' in scraped LP data — see below.</p>');
    } else {
      parts.push('<p class="dfi-commits-empty muted">No INGO-GP fund LP commitments located in public press materials at last verification. Stated sectors and geographies are sourced from this DFI’s own public profile.</p>');
    }
    parts.push(renderNetworkLpBlock(d));

    var prefBits = [];
    if (d.stated_sector_priorities && d.stated_sector_priorities.length) {
      prefBits.push('<div class="pref-row"><span class="pref-label">Sector priorities:</span> ' +
        d.stated_sector_priorities.map(function (t) { return chip(sectorLabel(t), "chip-sector"); }).join("") + '</div>');
    }
    if (d.stated_geo_priorities && d.stated_geo_priorities.length) {
      prefBits.push('<div class="pref-row"><span class="pref-label">Geo priorities:</span> ' +
        d.stated_geo_priorities.map(function (t) { return chip(geoLabel(t), "chip-geo"); }).join("") + '</div>');
    }
    if (d.stated_thesis_excerpt) {
      prefBits.push('<blockquote class="thesis-excerpt">' + esc(d.stated_thesis_excerpt) + '</blockquote>');
    }
    if (prefBits.length) parts.push('<div class="dfi-prefs">' + prefBits.join("") + '</div>');

    var emf = d.emerging_manager_facility;
    if (emf && emf.exists && !defunct) {
      var emfParts = ['<span class="emf-label">Emerging-manager facility:</span>'];
      if (emf.program_name) emfParts.push('<strong>' + esc(emf.program_name) + '</strong>');
      if (emf.application_url) emfParts.push(sourceLink(emf.application_url, "apply"));
      if (emf.notes) emfParts.push('<p class="emf-notes muted">' + esc(String(emf.notes).replace(/\s+/g, " ")) + '</p>');
      parts.push('<div class="dfi-emf">' + emfParts.join(" ") + '</div>');
    }

    if (d.named_contact) {
      var c = esc(d.named_contact);
      if (d.named_contact_title) c += ' <span class="muted">(' + esc(d.named_contact_title) + ')</span>';
      parts.push('<div class="dfi-contact"><span class="rel-label">Named contact:</span> ' + c + '</div>');
    }

    var footBits = [];
    if (d.stated_thesis_url) footBits.push(sourceLink(d.stated_thesis_url, "Stated thesis"));
    if (d.public_newsroom_url) footBits.push(sourceLink(d.public_newsroom_url, "Newsroom"));
    else if (d.last_known_activity_url) footBits.push(sourceLink(d.last_known_activity_url, "Last activity"));
    if (d.last_known_activity_date) footBits.push('<span class="muted">Last activity ' + esc(fmtDate(d.last_known_activity_date)) + '</span>');
    if (d.last_seen_at) footBits.push('<span class="muted">Last seen ' + esc(fmtDate(d.last_seen_at)) + '</span>');
    if (footBits.length) parts.push('<div class="dfi-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    var dfiUrl = d.stated_thesis_url || d.public_newsroom_url || d.last_known_activity_url;
    var clickCls = dfiUrl ? " card-clickable" : "";
    var defunctCls = defunct ? " card-defunct" : "";
    var dataAttr = dfiUrl ? ' data-source-url="' + escAttr(dfiUrl) + '"' : "";
    var idAttr = d.slug ? ' id="dfi-' + escAttr(d.slug) + '"' : "";
    return '<article class="card dfi-card' + clickCls + defunctCls + '"' + idAttr + dataAttr + '>' + parts.join("") + '</article>';
  }

  function renderDeadlineRow(d, meta) {
    var parts = [];
    var dateLabel = deadlineStatus(d);
    var dateCls = d.deadline_date ? "deadline-date" : "deadline-date deadline-date-rolling";
    var verb = d.deadline_date ? dateVerb(d.kind) : "";
    var dateHtml = verb
      ? '<span class="deadline-date-verb">' + esc(verb) + '</span><span class="deadline-date-value">' + esc(dateLabel) + '</span>'
      : esc(dateLabel);
    parts.push('<div class="' + dateCls + '">' + dateHtml + '</div>');

    var main = [];
    main.push('<h3 class="deadline-title">' + esc(d.title) + '</h3>');
    var hdrBits = [];
    if (d.issuing_body) hdrBits.push('<span class="deadline-body">' + esc(d.issuing_body) + '</span>');
    if (d.country) hdrBits.push(badge(countryName(meta, d.country), "badge-country"));
    if (d.kind) hdrBits.push(badge(KIND_LABELS[d.kind] || d.kind.replace(/_/g, " "), "badge-kind badge-kind-" + esc(d.kind)));
    main.push('<div class="deadline-sub">' + hdrBits.join(" ") + '</div>');

    if (d.why_it_matters) {
      main.push('<p class="deadline-why">' + esc(String(d.why_it_matters).replace(/\s+/g, " ")) + '</p>');
    }

    var footBits = [];
    if (d.public_source_url) footBits.push(sourceLink(d.public_source_url, "Source"));
    if (d.source_kind) footBits.push('<span class="muted">' + esc(d.source_kind) + '</span>');
    if (d.last_verified_at) footBits.push('<span class="verified muted">Last verified ' + esc(fmtDate(d.last_verified_at)) + '</span>');
    if (footBits.length) main.push('<div class="deadline-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    parts.push('<div class="deadline-main">' + main.join("") + '</div>');

    var clickCls = d.public_source_url ? " card-clickable" : "";
    var dataAttr = d.public_source_url ? ' data-source-url="' + escAttr(d.public_source_url) + '"' : "";
    return '<article class="card deadline-row' + clickCls + '"' + dataAttr + '>' + parts.join("") + '</article>';
  }

  // ---- Whole-card click handler (delegated, install once per page) --------

  function installCardClick(rootSelector) {
    var rootEl = document.querySelector(rootSelector);
    if (!rootEl) return;
    rootEl.addEventListener("click", function (evt) {
      if (evt.target.closest("a")) return;
      if (evt.target.closest("summary")) return;
      var card = evt.target.closest(".card-clickable");
      if (!card) return;
      var url = card.getAttribute("data-source-url");
      if (!url) return;
      window.open(url, "_blank", "noopener,noreferrer");
    });
  }

  // ---- Country-filter helpers ---------------------------------------------

  function buildCountryOptions(meta, slotKey) {
    if (!meta || !meta.country_enum) return ['<option value="ALL" selected>All countries</option>'];
    var counts = (meta.country_counts || {})[slotKey] || {};
    var opts = ['<option value="ALL" selected>All countries</option>'];
    meta.country_enum.forEach(function (code) {
      var n = counts[code] || 0;
      if (n > 0) {
        var name = (meta.country_display_names || {})[code] || code;
        opts.push('<option value="' + escAttr(code) + '">' + esc(name) + ' (' + n + ')</option>');
      }
    });
    return opts;
  }

  // ---- Impact-area chart --------------------------------------------------
  //
  // Diverging horizontal bar chart. One row per sector. DFI count on the
  // left of the centre axis, peer-fund count on the right. Click a row to
  // open a drawer below the chart with the underlying DFIs and peer funds.

  function renderImpactChart(svgRoot, drawerRoot, rows, opts) {
    opts = opts || {};
    var useActive  = !!opts.useActive;         // homepage: true; /impact-areas: toggle
    var useCalling = !!opts.useCalling;        // /impact-areas: toggle, default off
    var dfiFilter  = opts.dfiFilter || null;   // array of dfi slugs OR null
    var prefix = (typeof window !== "undefined" && window.IFC_PATH_PREFIX) || "/";

    // ---- prepare rows: filter by DFI selection and re-tally counts ---------
    var working = rows.map(function (r) {
      var dfis = r.dfis || [];
      if (dfiFilter && dfiFilter.length) {
        dfis = dfis.filter(function (d) { return dfiFilter.indexOf(d.slug) !== -1; });
      }
      // Toggle filters compose: when both are on, the bar shows only DFIs
      // that are BOTH active in the last 3y AND have a current open call.
      var visibleDfis = dfis;
      if (useActive)  visibleDfis = visibleDfis.filter(function (d) { return d.is_active_3y; });
      if (useCalling) visibleDfis = visibleDfis.filter(function (d) { return d.is_calling; });
      var peerN = (r.peer_funds || []).length;
      var statedLps = r.stated_lps || [];
      return {
        slug: r.slug, label: r.label,
        peerN: peerN, dfiN: visibleDfis.length,
        statedN: statedLps.length,
        statedFoundationN: r.stated_foundation_count || 0,
        statedFamilyOfficeN: r.stated_family_office_count || 0,
        totalN: peerN + visibleDfis.length + statedLps.length,
        committedN: peerN + visibleDfis.length,
        peer_funds: r.peer_funds, dfis: visibleDfis, stated_lps: statedLps,
      };
    }).filter(function (r) {
      // If filtering by DFI, drop sectors that DFI doesn't fund.
      if (dfiFilter && dfiFilter.length) return r.dfiN > 0;
      // No filter: drop empty sectors.
      return r.totalN > 0;
    });

    // Sort by committed evidence first (DFIs + peer funds), then break ties
    // with stated interest. Keeps the credibility ranking intact while still
    // letting stated interest move sectors with no commitments above ones
    // with neither.
    working.sort(function (a, b) {
      if (a.committedN !== b.committedN) return b.committedN - a.committedN;
      if (a.peerN     !== b.peerN)       return b.peerN - a.peerN;
      return b.statedN - a.statedN;
    });

    var maxBar = 1;
    working.forEach(function (r) {
      if (r.peerN   > maxBar) maxBar = r.peerN;
      if (r.dfiN    > maxBar) maxBar = r.dfiN;
      if (r.statedN > maxBar) maxBar = r.statedN;
    });

    // ---- geometry ---------------------------------------------------------
    // The axis splits "capital sources" (left) from "fund vehicles" (right).
    // Each sector gets two stacked sub-rows:
    //   top    — DFIs committed (cyan, leftward) ◀ axis ▶ peer-fund precedents (amber)
    //   bottom — Stated interest: foundations + family offices (muted violet,
    //            leftward — same side as DFIs because they're also capital
    //            sources). Thinner bar signals it's intent, not committed.
    var ROW_H = 52, ROW_GAP = 8, LABEL_W = 155, GUTTER = 28;
    var BAR_AREA = 320;
    // Reserved space past the bar for the count label. The stated-interest
    // label is the widest case: "NN (NNf / NNfo)" ~ 16 chars in mono, plus
    // a 6px lead-in. Sized so the label never spills past the SVG viewBox.
    var NUM_MARGIN = 110;
    var BAR_W_MAX = BAR_AREA - NUM_MARGIN;
    var PADDING = 14;
    var width = LABEL_W + BAR_AREA + GUTTER + BAR_AREA + PADDING * 2;
    var height = working.length * (ROW_H + ROW_GAP) + PADDING * 2 + 38;
    var axisX = PADDING + LABEL_W + BAR_AREA + GUTTER / 2;

    var axisFlags = [];
    if (useActive)  axisFlags.push("active 3y");
    if (useCalling) axisFlags.push("calling now");
    var dfiAxisLabel = axisFlags.length
      ? "DFIs " + axisFlags.join(" + ") + " ◀"
      : "DFIs committed ◀";
    var peerAxisLabel = "▶ Peer-fund precedents";

    // ---- SVG ----------------------------------------------------------------
    var parts = [];
    parts.push('<svg class="impact-chart" viewBox="0 0 ' + width + ' ' + height +
      '" preserveAspectRatio="xMidYMin meet" role="img" aria-label="Sectors by committed DFIs, peer-fund precedents, and stated-interest LPs">');

    // axis labels (top — committed/precedent butterfly)
    parts.push('<text class="ic-axis-head ic-axis-head-dfi" x="' + (axisX - GUTTER / 2 - 6) + '" y="' + (PADDING + 12) + '" text-anchor="end">' + esc(dfiAxisLabel) + '</text>');
    parts.push('<text class="ic-axis-head ic-axis-head-peer" x="' + (axisX + GUTTER / 2 + 6) + '" y="' + (PADDING + 12) + '" text-anchor="start">' + esc(peerAxisLabel) + '</text>');
    // axis sub-label for the stated-interest band (left side, mirroring DFI)
    parts.push('<text class="ic-axis-head ic-axis-head-stated" x="' + (axisX - GUTTER / 2 - 6) + '" y="' + (PADDING + 26) + '" text-anchor="end">Stated interest (foundations + family offices) ◀</text>');

    // axis line
    var axisY1 = PADDING + 32;
    var axisY2 = height - PADDING;
    parts.push('<line class="ic-axis" x1="' + axisX + '" x2="' + axisX + '" y1="' + axisY1 + '" y2="' + axisY2 + '" />');

    working.forEach(function (r, i) {
      var rowTop = PADDING + 38 + i * (ROW_H + ROW_GAP);
      // Top sub-row: butterfly (committed evidence)
      var barY = rowTop + 4;
      var barH = 22;
      // Bottom sub-row: stated interest
      var statedBarY = rowTop + 32;
      var statedBarH = 12;

      // label (vertically centered across both sub-rows)
      parts.push('<text class="ic-label" x="' + (PADDING + LABEL_W - 8) + '" y="' + (rowTop + 24) + '" text-anchor="end">' +
        esc(r.label) + '</text>');

      // DFI bar (grows leftward from axis)
      var dfiW = r.dfiN > 0 ? Math.max(1, (r.dfiN / maxBar) * BAR_W_MAX) : 0;
      if (dfiW > 0) {
        parts.push('<rect class="ic-bar ic-bar-dfi" x="' + (axisX - GUTTER / 2 - dfiW) +
          '" y="' + barY + '" width="' + dfiW + '" height="' + barH + '" rx="1.5" />');
        parts.push('<text class="ic-num ic-num-dfi" x="' + (axisX - GUTTER / 2 - dfiW - 6) +
          '" y="' + (barY + barH - 6) + '" text-anchor="end">' + r.dfiN + '</text>');
      }

      // Peer bar (grows rightward from axis)
      var peerW = r.peerN > 0 ? Math.max(1, (r.peerN / maxBar) * BAR_W_MAX) : 0;
      if (peerW > 0) {
        parts.push('<rect class="ic-bar ic-bar-peer" x="' + (axisX + GUTTER / 2) +
          '" y="' + barY + '" width="' + peerW + '" height="' + barH + '" rx="1.5" />');
        parts.push('<text class="ic-num ic-num-peer" x="' + (axisX + GUTTER / 2 + peerW + 6) +
          '" y="' + (barY + barH - 6) + '" text-anchor="start">' + r.peerN + '</text>');
      }

      // Stated-interest bar (grows leftward from axis, same side as DFI —
      // both are capital-source counts. Thinner + muted to signal intent).
      var statedW = r.statedN > 0 ? Math.max(1, (r.statedN / maxBar) * BAR_W_MAX) : 0;
      if (statedW > 0) {
        parts.push('<rect class="ic-bar ic-bar-stated" x="' + (axisX - GUTTER / 2 - statedW) +
          '" y="' + statedBarY + '" width="' + statedW + '" height="' + statedBarH + '" rx="1.5" />');
        var statedLabel = r.statedN +
          ' (' + r.statedFoundationN + 'f / ' + r.statedFamilyOfficeN + 'fo)';
        parts.push('<text class="ic-num ic-num-stated" x="' + (axisX - GUTTER / 2 - statedW - 6) +
          '" y="' + (statedBarY + statedBarH - 2) + '" text-anchor="end">' + statedLabel + '</text>');
      }

      // click overlay (transparent, full row width — covers both sub-rows)
      parts.push('<rect class="ic-row-hit" data-sector="' + escAttr(r.slug) +
        '" x="' + PADDING + '" y="' + rowTop + '" width="' + (width - PADDING * 2) +
        '" height="' + ROW_H + '" />');
    });

    if (working.length === 0) {
      parts.push('<text class="ic-empty" x="' + (width / 2) + '" y="' + (height / 2) +
        '" text-anchor="middle">No sectors match the current filter.</text>');
    }

    parts.push('</svg>');

    svgRoot.innerHTML = parts.join("");

    // ---- click → drawer -----------------------------------------------------
    if (drawerRoot) {
      var current = null;
      function open(slug) {
        var row = working.filter(function (r) { return r.slug === slug; })[0];
        if (!row) { drawerRoot.innerHTML = ""; return; }
        current = slug;
        drawerRoot.innerHTML = renderImpactDrawer(row, prefix);
        // mark selected hit
        var hits = svgRoot.querySelectorAll(".ic-row-hit");
        for (var i = 0; i < hits.length; i++) {
          hits[i].classList.toggle("is-selected", hits[i].getAttribute("data-sector") === slug);
        }
        drawerRoot.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
      svgRoot.addEventListener("click", function (e) {
        var hit = e.target.closest(".ic-row-hit");
        if (!hit) return;
        var slug = hit.getAttribute("data-sector");
        if (slug === current) {
          // re-click closes
          drawerRoot.innerHTML = "";
          current = null;
          var hits2 = svgRoot.querySelectorAll(".ic-row-hit");
          for (var j = 0; j < hits2.length; j++) hits2[j].classList.remove("is-selected");
          return;
        }
        open(slug);
      });

      // Open the top sector by default if there's something to show.
      if (working.length > 0 && opts.openFirst !== false) open(working[0].slug);
    }

    return working;
  }

  function renderImpactDrawer(row, prefix) {
    function fundUrl(slug) { return prefix + "peer-funds/#fund-" + slug; }
    function dfiUrl(slug) { return prefix + "dfis/#dfi-" + slug; }
    function statusBadge(s) {
      if (!s) return "";
      return badge(s.replace(/_/g, " "), "badge-status badge-status-" + esc(s));
    }
    function flag(c) {
      if (!c) return "";
      return '<span class="ic-cc">' + esc(c) + '</span>';
    }
    function activeDot(active) {
      return active
        ? '<span class="ic-active-dot" title="Active in last 3y" aria-label="Active in last 3y">●</span>'
        : '<span class="ic-inactive-dot" title="No commit in last 3y" aria-label="No commit in last 3y">○</span>';
    }

    var dfiHtml = (row.dfis || []).map(function (d) {
      var meta = [];
      if (d.country) meta.push(flag(d.country));
      meta.push('<span class="ic-commit-n">' + d.commit_count + ' commit' + (d.commit_count === 1 ? '' : 's') + '</span>');
      if (d.last_commit_date) meta.push('<span class="muted">last ' + esc(fmtDate(d.last_commit_date)) + '</span>');
      if (d.is_calling) meta.push('<span class="ic-calling-tag" title="Currently has an open call / RFP / rolling application">calling</span>');
      return '<li class="ic-drawer-item ic-drawer-dfi">'
        + activeDot(d.is_active_3y)
        + '<a class="ic-drawer-name" href="' + escAttr(dfiUrl(d.slug)) + '">' + esc(d.name) + '</a>'
        + '<span class="ic-drawer-meta">' + meta.join('<span class="sep">·</span>') + '</span>'
        + '</li>';
    }).join("");

    var fundHtml = (row.peer_funds || []).map(function (f) {
      var meta = [];
      if (f.parent_ingo) meta.push('<span class="ic-drawer-ingo">' + esc(f.parent_ingo) + '</span>');
      if (f.parent_ingo_country) meta.push(flag(f.parent_ingo_country));
      var dateStr = f.first_close_date ? fmtDate(f.first_close_date) : (f.vintage ? String(f.vintage) : "");
      if (dateStr) meta.push('<span class="muted">' + esc(dateStr) + '</span>');
      if (f.size_usd_m != null) meta.push('<span class="ic-drawer-size">' + esc(fmtUSDm(f.size_usd_m)) + '</span>');
      meta.push(statusBadge(f.status));
      return '<li class="ic-drawer-item ic-drawer-fund">'
        + '<a class="ic-drawer-name" href="' + escAttr(fundUrl(f.slug)) + '">' + esc(f.name) + '</a>'
        + '<span class="ic-drawer-meta">' + meta.join('<span class="sep">·</span>') + '</span>'
        + '</li>';
    }).join("");

    var statedHtml = (row.stated_lps || []).map(function (lp) {
      var meta = [];
      if (lp.country) meta.push(flag(lp.country));
      var kindLabel = lp.kind === "foundation" ? "Foundation"
                    : lp.kind === "family_office" ? "Family office / FB / DAF"
                    : (lp.kind || "");
      if (kindLabel) meta.push('<span class="ic-stated-kind">' + esc(kindLabel) + '</span>');
      var hrefBase = lp.kind === "foundation" ? "foundations/#fdn-"
                  : lp.kind === "family_office" ? "family-offices/#famof-"
                  : "";
      var nameHtml = hrefBase
        ? '<a class="ic-drawer-name" href="' + escAttr(prefix + hrefBase + lp.slug) + '">' + esc(lp.name) + '</a>'
        : '<span class="ic-drawer-name">' + esc(lp.name) + '</span>';
      return '<li class="ic-drawer-item ic-drawer-stated">'
        + '<span class="ic-stated-dot" title="Stated interest, not a confirmed commitment" aria-label="Stated interest">◇</span>'
        + nameHtml
        + '<span class="ic-drawer-meta">' + meta.join('<span class="sep">·</span>') + '</span>'
        + '</li>';
    }).join("");

    var dfiHeader = (row.dfis || []).length
      ? 'DFIs committed (' + (row.dfis || []).length + ')'
      : 'DFIs committed (0 — no commitments on record yet)';
    var fundHeader = (row.peer_funds || []).length
      ? 'Peer-fund precedents (' + (row.peer_funds || []).length + ')'
      : 'Peer-fund precedents (0)';
    var statedHeader = (row.stated_lps || []).length
      ? 'Stated interest — foundations &amp; family offices (' + (row.stated_lps || []).length + ')'
      : 'Stated interest — foundations &amp; family offices (0)';

    return '<div class="ic-drawer">'
      + '<div class="ic-drawer-head">'
        + '<h3 class="ic-drawer-title">' + esc(row.label) + '</h3>'
        + '<div class="ic-drawer-stats">'
          + '<span><strong>' + (row.dfis || []).length + '</strong> DFIs</span>'
          + '<span class="sep">·</span>'
          + '<span><strong>' + (row.peer_funds || []).length + '</strong> peer funds</span>'
          + '<span class="sep">·</span>'
          + '<span><strong>' + (row.stated_lps || []).length + '</strong> LPs stated</span>'
        + '</div>'
      + '</div>'
      + '<div class="ic-drawer-cols ic-drawer-cols-3">'
        + '<div class="ic-drawer-col"><h4>' + dfiHeader + '</h4>'
          + (dfiHtml ? '<ul class="ic-drawer-list">' + dfiHtml + '</ul>'
                     : '<p class="muted">No DFI commitments to INGO-sponsored funds in this sector are in the public record yet.</p>')
        + '</div>'
        + '<div class="ic-drawer-col"><h4>' + fundHeader + '</h4>'
          + (fundHtml ? '<ul class="ic-drawer-list">' + fundHtml + '</ul>'
                     : '<p class="muted">No INGO-sponsored fund precedents tagged in this sector.</p>')
        + '</div>'
        + '<div class="ic-drawer-col"><h4>' + statedHeader + '</h4>'
          + (statedHtml ? '<ul class="ic-drawer-list">' + statedHtml + '</ul>'
                       : '<p class="muted">No foundations or family offices have publicly stated this as a priority theme.</p>')
        + '</div>'
      + '</div>'
      + '</div>';
  }

  // ---- Foundation card ---------------------------------------------------

  var FOUNDATION_TYPE_LABELS = {
    private: "Private foundation",
    corporate: "Corporate foundation",
    community: "Community foundation",
    operating: "Operating foundation",
    public_charity: "Public charity / pooled fund",
    supporting_org: "Supporting org / DAF",
    philanthropy: "Philanthropy LLC",
  };

  function renderLpCheckRange(d) {
    var lo = d.typical_check_usd_m_min, hi = d.typical_check_usd_m_max;
    if (lo == null && hi == null) return "";
    var rangeStr = "";
    if (lo != null && hi != null) rangeStr = fmtUSDm(lo) + "–" + fmtUSDm(hi);
    else if (lo != null) rangeStr = "from " + fmtUSDm(lo);
    else rangeStr = "up to " + fmtUSDm(hi);
    return '<span class="stat"><span class="stat-num">' + esc(rangeStr) + '</span><span class="stat-label">stated check size</span></span>';
  }

  function renderFoundationCard(d, meta) {
    var parts = [];
    parts.push('<h3 class="dfi-name">' + esc(d.name) + '</h3>');

    var hdrBits = [];
    if (d.country) hdrBits.push(badge(countryName(meta, d.country), "badge-country"));
    if (d.foundation_type && FOUNDATION_TYPE_LABELS[d.foundation_type]) {
      hdrBits.push(badge(FOUNDATION_TYPE_LABELS[d.foundation_type], "badge-remit"));
    }
    parts.push('<div class="dfi-sub">' + hdrBits.join(" ") + '</div>');

    var stats = [];
    var commits = d.known_ingo_gp_commits || [];
    if (commits.length > 0) {
      stats.push('<span class="stat"><span class="stat-num">' + commits.length + '</span><span class="stat-label">disclosed INGO-GP commitments</span></span>');
    }
    if (d.aum_usd_m != null) {
      var yr = d.aum_usd_m_year ? ' (' + d.aum_usd_m_year + ')' : '';
      stats.push('<span class="stat"><span class="stat-num">' + fmtUSDm(d.aum_usd_m) + '</span><span class="stat-label">AUM' + esc(yr) + '</span></span>');
    }
    var checkStat = renderLpCheckRange(d);
    if (checkStat) stats.push(checkStat);
    if (stats.length) parts.push('<div class="dfi-stats">' + stats.join("") + '</div>');

    var netLp = d._network_lp_funds || [];
    if (commits.length > 0) {
      var lis = commits.map(function (c) {
        var prefix = (typeof window !== "undefined" && window.IFC_PATH_PREFIX) || "/";
        var inner = c.peer_fund_slug
          ? '<a href="' + prefix + 'peer-funds/#fund-' + escAttr(c.peer_fund_slug) + '" class="commit-fund">' + esc(c.peer_fund_name) + '</a>'
          : '<span class="commit-fund">' + esc(c.peer_fund_name) + '</span>';
        var bits = [inner];
        if (c.parent_ingo) bits.push('<span class="commit-ingo muted">' + esc(c.parent_ingo) + '</span>');
        if (c.amount_usd_m != null) bits.push('<span class="commit-amt">' + esc(fmtUSDm(c.amount_usd_m)) + '</span>');
        if (c.commit_date) bits.push('<span class="commit-date muted">' + esc(fmtDate(c.commit_date)) + '</span>');
        if (c.public_source_url) bits.push(sourceLink(c.public_source_url, "source"));
        return '<li class="commit-row">' + bits.join('<span class="sep">·</span>') + '</li>';
      });
      parts.push('<details class="dfi-commits" open><summary>Disclosed INGO-GP commitments (' + commits.length + ')</summary><ul class="commit-list">' + lis.join("") + '</ul></details>');
    } else if (netLp.length > 0) {
      parts.push('<p class="dfi-commits-empty muted">No INGO-GP commitments curated to the registry yet, but this foundation is named as an LP on ' + netLp.length + ' peer impact fund' + (netLp.length === 1 ? "" : "s") + ' in scraped LP data — see below.</p>');
    } else {
      parts.push('<p class="dfi-commits-empty muted">No INGO-GP fund LP commitments located in public materials at last verification. Foundation may operate via grants, PRI, or MRI rather than fund-LP commitments — verify via 990s and annual reports.</p>');
    }
    parts.push(renderNetworkLpBlock(d));

    var prefBits = [];
    if (d.stated_priority_themes && d.stated_priority_themes.length) {
      prefBits.push('<div class="pref-row"><span class="pref-label">Priority themes:</span> ' +
        d.stated_priority_themes.map(function (t) { return chip(sectorLabel(t), "chip-sector"); }).join("") + '</div>');
    }
    if (d.stated_geo_focus && d.stated_geo_focus.length) {
      prefBits.push('<div class="pref-row"><span class="pref-label">Geo focus:</span> ' +
        d.stated_geo_focus.map(function (t) { return chip(geoLabel(t), "chip-geo"); }).join("") + '</div>');
    }
    if (d.stated_thesis_excerpt) {
      prefBits.push('<blockquote class="thesis-excerpt">' + esc(d.stated_thesis_excerpt) + '</blockquote>');
    }
    if (prefBits.length) parts.push('<div class="dfi-prefs">' + prefBits.join("") + '</div>');

    function renderProgram(p, label) {
      if (!p || !p.exists) return "";
      var bits = ['<span class="emf-label">' + esc(label) + ':</span>'];
      if (p.program_name) bits.push('<strong>' + esc(p.program_name) + '</strong>');
      if (p.application_url) bits.push(sourceLink(p.application_url, "apply"));
      if (p.notes) bits.push('<p class="emf-notes muted">' + esc(String(p.notes).replace(/\s+/g, " ")) + '</p>');
      return '<div class="dfi-emf">' + bits.join(" ") + '</div>';
    }
    parts.push(renderProgram(d.pri_program, "PRI program"));
    parts.push(renderProgram(d.mri_program, "MRI program"));

    var footBits = [];
    if (d.stated_thesis_url) footBits.push(sourceLink(d.stated_thesis_url, "Stated thesis"));
    if (d.public_newsroom_url) footBits.push(sourceLink(d.public_newsroom_url, "Newsroom"));
    if (d.last_seen_at) footBits.push('<span class="muted">Last seen ' + esc(fmtDate(d.last_seen_at)) + '</span>');
    if (footBits.length) parts.push('<div class="dfi-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    var url = d.stated_thesis_url || d.public_newsroom_url;
    var clickCls = url ? " card-clickable" : "";
    var dataAttr = url ? ' data-source-url="' + escAttr(url) + '"' : "";
    var idAttr = d.slug ? ' id="fdn-' + escAttr(d.slug) + '"' : "";
    return '<article class="card dfi-card' + clickCls + '"' + idAttr + dataAttr + '>' + parts.join("") + '</article>';
  }

  // ---- Family-office card ------------------------------------------------

  var FAMILY_OFFICE_CATEGORY_LABELS = {
    family_office: "Family office",
    faith_based: "Faith-based investor",
    daf: "DAF host",
    philanthropy_llc: "Philanthropy LLC",
    hnwi_collective: "HNWI collective",
  };

  function renderFamilyOfficeCard(d, meta) {
    var parts = [];
    parts.push('<h3 class="dfi-name">' + esc(d.name) + '</h3>');

    var hdrBits = [];
    if (d.country) hdrBits.push(badge(countryName(meta, d.country), "badge-country"));
    if (d.category && FAMILY_OFFICE_CATEGORY_LABELS[d.category]) {
      hdrBits.push(badge(FAMILY_OFFICE_CATEGORY_LABELS[d.category], "badge-remit"));
    }
    parts.push('<div class="dfi-sub">' + hdrBits.join(" ") + '</div>');

    var stats = [];
    var commits = d.known_ingo_gp_commits || [];
    if (commits.length > 0) {
      stats.push('<span class="stat"><span class="stat-num">' + commits.length + '</span><span class="stat-label">disclosed INGO-GP commitments</span></span>');
    }
    if (d.aum_usd_m != null) {
      var yr = d.aum_usd_m_year ? ' (' + d.aum_usd_m_year + ')' : '';
      stats.push('<span class="stat"><span class="stat-num">' + fmtUSDm(d.aum_usd_m) + '</span><span class="stat-label">AUM' + esc(yr) + '</span></span>');
    }
    var checkStat = renderLpCheckRange(d);
    if (checkStat) stats.push(checkStat);
    if (stats.length) parts.push('<div class="dfi-stats">' + stats.join("") + '</div>');

    var netLp = d._network_lp_funds || [];
    if (commits.length > 0) {
      var lis = commits.map(function (c) {
        var prefix = (typeof window !== "undefined" && window.IFC_PATH_PREFIX) || "/";
        var inner = c.peer_fund_slug
          ? '<a href="' + prefix + 'peer-funds/#fund-' + escAttr(c.peer_fund_slug) + '" class="commit-fund">' + esc(c.peer_fund_name) + '</a>'
          : '<span class="commit-fund">' + esc(c.peer_fund_name) + '</span>';
        var bits = [inner];
        if (c.parent_ingo) bits.push('<span class="commit-ingo muted">' + esc(c.parent_ingo) + '</span>');
        if (c.amount_usd_m != null) bits.push('<span class="commit-amt">' + esc(fmtUSDm(c.amount_usd_m)) + '</span>');
        if (c.commit_date) bits.push('<span class="commit-date muted">' + esc(fmtDate(c.commit_date)) + '</span>');
        if (c.public_source_url) bits.push(sourceLink(c.public_source_url, "source"));
        return '<li class="commit-row">' + bits.join('<span class="sep">·</span>') + '</li>';
      });
      parts.push('<details class="dfi-commits" open><summary>Disclosed INGO-GP commitments (' + commits.length + ')</summary><ul class="commit-list">' + lis.join("") + '</ul></details>');
    } else if (netLp.length > 0) {
      parts.push('<p class="dfi-commits-empty muted">No INGO-GP commitments curated to the registry yet, but this entity is named as an LP on ' + netLp.length + ' peer impact fund' + (netLp.length === 1 ? "" : "s") + ' in scraped LP data — see below.</p>');
    } else {
      parts.push('<p class="dfi-commits-empty muted">No INGO-GP fund LP commitments located in public materials at last verification. Family offices and faith-based investors typically disclose less than DFIs — direct outreach is the usual discovery path.</p>');
    }
    parts.push(renderNetworkLpBlock(d));

    var prefBits = [];
    if (d.stated_priority_themes && d.stated_priority_themes.length) {
      prefBits.push('<div class="pref-row"><span class="pref-label">Priority themes:</span> ' +
        d.stated_priority_themes.map(function (t) { return chip(sectorLabel(t), "chip-sector"); }).join("") + '</div>');
    }
    if (d.stated_geo_focus && d.stated_geo_focus.length) {
      prefBits.push('<div class="pref-row"><span class="pref-label">Geo focus:</span> ' +
        d.stated_geo_focus.map(function (t) { return chip(geoLabel(t), "chip-geo"); }).join("") + '</div>');
    }
    if (d.stated_thesis_excerpt) {
      prefBits.push('<blockquote class="thesis-excerpt">' + esc(d.stated_thesis_excerpt) + '</blockquote>');
    }
    if (prefBits.length) parts.push('<div class="dfi-prefs">' + prefBits.join("") + '</div>');

    var footBits = [];
    if (d.stated_thesis_url) footBits.push(sourceLink(d.stated_thesis_url, "Stated thesis"));
    if (d.public_newsroom_url) footBits.push(sourceLink(d.public_newsroom_url, "Newsroom"));
    if (d.last_seen_at) footBits.push('<span class="muted">Last seen ' + esc(fmtDate(d.last_seen_at)) + '</span>');
    if (footBits.length) parts.push('<div class="dfi-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    var url = d.stated_thesis_url || d.public_newsroom_url;
    var clickCls = url ? " card-clickable" : "";
    var dataAttr = url ? ' data-source-url="' + escAttr(url) + '"' : "";
    var idAttr = d.slug ? ' id="famof-' + escAttr(d.slug) + '"' : "";
    return '<article class="card dfi-card' + clickCls + '"' + idAttr + dataAttr + '>' + parts.join("") + '</article>';
  }

  // ---- Public API ---------------------------------------------------------

  root.IFC = {
    esc: esc, escAttr: escAttr, fmtDate: fmtDate, fmtUSDm: fmtUSDm,
    chip: chip, badge: badge, sourceLink: sourceLink, loadJSON: loadJSON,
    sectorLabel: sectorLabel, geoLabel: geoLabel, deadlineStatus: deadlineStatus,
    countryName: countryName, dateVerb: dateVerb,
    renderFundCard: renderFundCard,
    renderDfiCard: renderDfiCard,
    renderFoundationCard: renderFoundationCard,
    renderFamilyOfficeCard: renderFamilyOfficeCard,
    renderDeadlineRow: renderDeadlineRow,
    renderImpactChart: renderImpactChart,
    renderImpactDrawer: renderImpactDrawer,
    installCardClick: installCardClick,
    buildCountryOptions: buildCountryOptions,
  };
})(window);
