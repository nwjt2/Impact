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
    if (f.last_seen_at) footBits.push('<span class="verified muted">Last verified ' + esc(fmtDate(f.last_seen_at)) + '</span>');
    if (footBits.length) parts.push('<div class="fund-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    var clickCls = f.public_source_url ? " card-clickable" : "";
    var dataAttr = f.public_source_url ? ' data-source-url="' + escAttr(f.public_source_url) + '"' : "";
    return '<article class="card fund-card' + clickCls + '" id="fund-card-' + escAttr(f.slug) + '"' + dataAttr + '>' + parts.join("") + '</article>';
  }

  function renderDfiCard(d, meta) {
    var parts = [];
    parts.push('<h3 class="dfi-name">' + esc(d.name) + '</h3>');
    var hdrBits = [];
    if (d.country) hdrBits.push(badge(countryName(meta, d.country), "badge-country"));
    if (d.policy_remit) hdrBits.push(badge(d.policy_remit, "badge-remit"));
    parts.push('<div class="dfi-sub">' + hdrBits.join(" ") + '</div>');

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
    if (stats.length) parts.push('<div class="dfi-stats">' + stats.join("") + '</div>');

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
    }

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
    if (emf && emf.exists) {
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
    if (d.last_seen_at) footBits.push('<span class="verified muted">Last verified ' + esc(fmtDate(d.last_seen_at)) + '</span>');
    if (footBits.length) parts.push('<div class="dfi-foot">' + footBits.join('<span class="sep">·</span>') + '</div>');

    var dfiUrl = d.stated_thesis_url || d.public_newsroom_url || d.last_known_activity_url;
    var clickCls = dfiUrl ? " card-clickable" : "";
    var dataAttr = dfiUrl ? ' data-source-url="' + escAttr(dfiUrl) + '"' : "";
    return '<article class="card dfi-card' + clickCls + '"' + dataAttr + '>' + parts.join("") + '</article>';
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

  // ---- Public API ---------------------------------------------------------

  root.IFC = {
    esc: esc, escAttr: escAttr, fmtDate: fmtDate, fmtUSDm: fmtUSDm,
    chip: chip, badge: badge, sourceLink: sourceLink, loadJSON: loadJSON,
    sectorLabel: sectorLabel, geoLabel: geoLabel, deadlineStatus: deadlineStatus,
    countryName: countryName, dateVerb: dateVerb,
    renderFundCard: renderFundCard,
    renderDfiCard: renderDfiCard,
    renderDeadlineRow: renderDeadlineRow,
    installCardClick: installCardClick,
    buildCountryOptions: buildCountryOptions,
  };
})(window);
