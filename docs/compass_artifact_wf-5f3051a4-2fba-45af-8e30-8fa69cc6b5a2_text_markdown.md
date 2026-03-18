# Design brief: an emissions dashboard that reads like the Financial Times

The most impactful design decision for an Australian grid emissions intensity tool isn't choosing the right chart library — it's choosing the right **design tradition** to draw from. Generic SaaS dashboards fail sustainability managers and finance teams because they signal "software product" when the audience needs "authoritative data publication." This brief synthesizes design conventions from six institutional traditions — financial publishing, museum exhibitions, editorial data journalism, transit wayfinding, scientific publishing, and cartography — into concrete, implementable recommendations for a Streamlit application displaying AEMO NEM data.

**The core insight across all six traditions: color is for meaning, not decoration.** McKinsey uses "50 shades of blue" with a single accent to direct attention. BCG renders 7 bars in gray and 1 in green to prove the headline. Control room operators work on gray screens where color appears only when something demands action. The IPCC banned rainbow color scales entirely. Every authoritative source treats color as a scarce, semantic resource. Your dashboard should do the same.

---

## What institutional publications actually look like inside

### Consultancy and energy sector conventions

McKinsey's 2019 identity redesign by Wolff Olins introduced **Bower**, a custom serif by Radim Pesko, paired with McKinsey Sans — but their PowerPoint fallbacks are Georgia and Arial, proving that authority comes from *system*, not novelty. Their charts use monochromatic blue tones with a single accent color highlighting the key insight. Deloitte Insights, designed by Pentagram partner Giorgia Lupi, uses a **grid system inspired by tax ledgers** — evoking "rigor, order, and confidence" — and includes a bespoke handwriting typeface for chart annotations, mimicking a consultant's handwritten note on a graph. BCG's presentation philosophy distills to one principle: every slide understood in 10 seconds, with "action titles" stating the insight and charts providing visual proof.

The BP Statistical Review (now Energy Institute Statistical Review, published since 1952) and the IEA World Energy Outlook share critical data visualization conventions. Both favor **stacked area charts** for energy mix evolution, **line charts with differentiated line styles** for scenario projections (solid for stated policies, dashed for aspirational, dotted for net-zero), and minimal decorative elements. The IEA charts library at iea.org/data-and-statistics/charts is a masterclass in restraint. Every chart includes source citations as standard practice.

For sustainability reporting specifically, the design conventions include: **hero metrics** displayed as oversized isolated numbers ("-40% GHG emissions" at 48px+), progress tracking via year-over-year bars, materiality matrices as scatter plots, and framework alignment badges (GRI, TCFD, CDP logos). Landscape PDF orientation now dominates because it matches screen viewing. The critical detail: **third-party assurance statements** and **methodology footnotes** are not afterthoughts — they are primary trust signals.

### The energy source color problem has no universal answer

There is **no published universal hex-code standard** for energy source colors, but strong conventions have emerged from dominant data publishers. The most consistent associations across BP/Energy Institute, IEA, AEMO, OpenNEM, Our World in Data, and Ember:

| Energy source | Convention | Confidence |
|---|---|---|
| Coal (black) | Black / dark gray (#333333) | Very high |
| Coal (brown) | Brown (#8B6914) | High |
| Natural gas | Orange-amber (#FF8C00) | Moderate — varies most across sources |
| Solar (utility) | Gold (#FFD700) | Very high |
| Solar (rooftop) | Light yellow (#FFF44F) | High |
| Wind | Green (#3CB371) | Very high |
| Hydro | Blue (#4682B4) | Very high |
| Battery/storage | Purple (#9370DB) | Moderate |
| Nuclear | Orange or yellow | Low consensus |

OpenNEM (now Open Electricity) established the dominant Australian convention: PV Magazine described their stacked area charts as showing "mountains of black coal," "blue watercolour of Tasmanian hydro sources fringed with green wind and golden solar," and South Australia's "green wind-powered hills, capped by halos of solar, atop craggy bursts of gas." These associations are deeply intuitive to Australian energy audiences and should be adopted directly.

---

## Three traditions that transform how data feels

### Editorial data journalism treats annotation as a first-class element

The single biggest difference between editorial and dashboard data visualization is **annotation philosophy**. Archie Tse, NYT Graphics Director, stated the governing rule: "If you make a tooltip or rollover, assume no one will ever see it." Editorial charts embed explanatory text directly on the visualization — trend labels, callout boxes, contextual notes — rather than hiding meaning behind hover states.

The Financial Times published the **Visual Vocabulary** (ft-interactive.github.io/visual-vocabulary/), organizing 72+ chart types into 9 relationship categories: Deviation, Correlation, Ranking, Distribution, Change over Time, Part-to-Whole, Magnitude, Spatial, and Flow. For grid emissions data, the relevant categories are Change over Time (area/line charts for generation mix), Deviation (diverging bars showing performance against targets), and Part-to-Whole (stacked areas for fuel mix composition).

Editorial typography follows a consistent pattern across publications. The New York Times uses NYT Cheltenham (serif) for headlines and NYT Franklin (sans) for data labels. The Guardian uniquely uses its serif (Guardian Headline) for chart titles — matching the editorial voice. The Economist commissioned Econ Sans specifically for data visualization, with Econ Sans Condensed for tight chart labels. Bloomberg Green uses BW Haas Grotesk, distinct from Bloomberg's main Neue Haas Grotesk. In every case, **chart titles state the insight** ("Solar surpassed coal for the first time") rather than describing the metric ("Solar and Coal Generation 2020-2025").

Layout conventions across FT, NYT, Economist, and Guardian share: white or warm off-white backgrounds (#F5F4EF at the Economist), minimal grid lines in light gray, generous margins, source citations always visible at bottom-left, and chart backgrounds that match page background (no card borders boxing charts in). The Economist's signature red horizontal rule at the top of every chart is a subtle branding device worth noting — a single consistent accent element unifies dozens of chart types.

### Museum exhibitions prove that hierarchy defeats density

Museum exhibition typography follows a strict **three-level hierarchy**: Level 1 titles (largest, boldest, often sans-serif), Level 2 body content (readable sans-serif, always left-aligned), and Level 3 supporting text (captions, credits, smallest). This maps directly to dashboard design: KPI headlines → chart content → source citations. The critical museum principle is that **generous white space signals confidence** — cramming information reads as desperate, while spacious layouts read as authoritative.

The Science Museum London's Energy Gallery (2004, Casson Mann) and ACMI Melbourne's 2021 renewal offer directly transferable lessons. ACMI's experience design by Publicis Sapient/Second Story introduced **The Constellation** — a room-scale data visualization connecting collected items through human-curated (not algorithmic) relationships. The design philosophy: let users build personalized paths through data rather than presenting a single narrative. Te Papa's Climate Converter (DOTDOT studio) specifically addressed research showing that **climate content typically leaves audiences "feeling overwhelmed, scared, and trapped"** — so they designed for motivation and personal agency. An emissions dashboard should similarly frame data as enabling decisions, not inducing anxiety.

### Control rooms prove that gray is the default state

ISA-101, the international standard for HMI (Human-Machine Interface) design in energy grid control rooms, establishes the most transferable principle for emissions dashboards: **"Under normal operation, the screen should be somewhat boring."** The High-Performance HMI approach uses light gray backgrounds, dark gray for process elements, and reserves color exclusively for conditions requiring attention. Red means critical alarm. Orange means high priority. Yellow means caution. Normal operation is deliberately colorless.

The **four-level display hierarchy** maps perfectly to a dashboard: Level 1 (grid-wide overview showing total NEM emissions), Level 2 (region/state breakdown), Level 3 (individual generator or fuel source detail), Level 4 (historical trends and diagnostics). Navigation follows this hierarchy — users move from overview to detail, never the reverse.

Control room typography rules are strict: **sans-serif only**, consistent font sizes, numeric data with aligned decimal points, bold for primary values, lighter weight for labels. Color is never the sole differentiator — always paired with shape, text, or position to account for color vision deficiency. Bloomberg Terminal follows similar logic: their default amber on black ensures that the ~20,000 users with color vision deficiency can still parse all information.

---

## Color system: from cartography to implementation

### Emissions intensity should use a green-to-brown sequential ramp

Electricity Maps (electricitymaps.com) established the dominant convention for grid carbon intensity visualization: **dark green → light green → yellow → amber → brown → dark brown**, with gray for no data. Their Home Assistant integration defines severity thresholds at green: 0, yellow: 150, red: 300 gCO₂eq/kWh. The IPCC AR6 Visual Style Guide reinforces this approach: sequential palettes for ordered magnitude data, diverging palettes only when a meaningful midpoint exists (e.g., above/below a target).

For an Australian NEM context, recommended emissions intensity color ramp:

| Intensity (gCO₂eq/kWh) | Color | Hex |
|---|---|---|
| 0–100 (very clean) | Deep green | #2D8B5A |
| 100–200 | Yellow-green | #8BC34A |
| 200–350 | Amber | #FFC107 |
| 350–500 | Orange | #E67E22 |
| 500–700 | Dark brown | #8B4513 |
| 700+ | Near black | #3E2723 |
| No data | Medium gray | #9E9E9E |

This avoids pure green-to-red (inaccessible to the 8% of males with red-green color vision deficiency) while maintaining the intuitive mapping of cool=clean, warm=dirty. The IPCC explicitly banned rainbow color scales (#EndRainbow) because they create false boundaries, make yellow disproportionately prominent, and are perceptually non-uniform. **Use ColorBrewer-derived palettes** (colorbrewer2.org) for any additional categorical or sequential needs.

### The complete dashboard color palette

Building from control room principles ("gray is good") and editorial conventions (color for emphasis only):

| Role | Color | Hex | Usage |
|---|---|---|---|
| Primary text | Near-black | #1C2833 | Body copy, headings |
| Secondary text | Dark gray | #626262 | Subtitles, labels, annotations |
| Tertiary text | Medium gray | #808080 | Source citations, footnotes |
| Background | White | #FFFFFF | Main canvas |
| Surface/cards | Cool gray | #F4F6F9 | Card backgrounds, sidebar |
| Accent (primary) | Institutional blue | #1B4F72 | Interactive elements, links |
| Positive/clean | Muted teal-green | #27AE60 | Renewable generation, below-target |
| Warning/attention | Amber | #F39C12 | Approaching thresholds |
| Critical/alert | Muted red | #C0392B | Exceeding targets, high intensity |
| Neutral/benchmark | Cool gray | #95A5A6 | Reference lines, comparisons |

This is deliberately restrained — **five functional colors** plus three grays. BCG's principle applies: when everything is colored, nothing stands out.

---

## Typography: the case for IBM Plex and Source Serif 4

### Why these two typefaces specifically

The research reveals a clear pattern in what authoritative institutions actually use and what open-source alternatives match their aesthetic. IBM Plex Sans descends from the same grotesque lineage as Bloomberg's Neue Haas Grotesk and the FT's Metric — it was designed to replace Helvetica Neue at IBM with better **I/l/0/O disambiguation**, critical for emissions data where confusing a zero and an O in a dataset erodes trust. Source Serif 4 provides editorial gravitas similar to the FT's Financier — a transitional serif with optical sizes (Caption, Text, Subhead, Display) that behaves differently at 12px labels versus 32px headlines.

The pairing signals "rigorous, data-literate institution" rather than "tech startup." Both are on Google Fonts, fully open-source (SIL OFL), and have **tabular lining figures** — the single most important typographic feature for a data dashboard, ensuring numbers align vertically in columns.

### Full type system specification

```css
:root {
  --font-serif:  'Source Serif 4', 'Georgia', serif;
  --font-sans:   'IBM Plex Sans', 'Helvetica Neue', 'Arial', sans-serif;
  --font-mono:   'IBM Plex Mono', 'Menlo', monospace;
}
```

| Role | Font | Weight | Size | CSS notes |
|---|---|---|---|---|
| Dashboard title | Source Serif 4 | Bold 700 | 32px / 2rem | `line-height: 1.25` |
| Section headings | Source Serif 4 | SemiBold 600 | 20–24px / 1.25–1.5rem | `line-height: 1.25` |
| Chart titles | Source Serif 4 | SemiBold 600 | 16–18px / 1–1.125rem | Declarative — states insight |
| Chart subtitles | IBM Plex Sans | Regular 400 | 16px / 1rem | `color: #626262` |
| Body text | IBM Plex Sans | Regular 400 | 15–16px / ~1rem | `line-height: 1.5` |
| KPI numbers | IBM Plex Sans | SemiBold 600 | 32–40px / 2–2.5rem | `font-variant-numeric: tabular-nums lining-nums; letter-spacing: -0.02em` |
| KPI labels | IBM Plex Sans | Medium 500 | 14px / 0.875rem | `color: #626262` |
| Data labels/axes | IBM Plex Sans | Regular 400 | 12–14px / 0.75–0.875rem | `font-variant-numeric: tabular-nums` |
| Raw data/tables | IBM Plex Mono | Regular 400 | 13px / 0.8125rem | For emissions factors, raw AEMO data |
| Source citations | IBM Plex Sans | Regular 400 | 12px / 0.75rem | `color: #808080` |

**Critical CSS for tabular figures:**
```css
.data-value, .kpi-number, .table-cell {
  font-variant-numeric: tabular-nums lining-nums;
}
```

Note: Google Fonts API may strip OpenType features. **Self-host** IBM Plex from its GitHub repository if tabular figures don't activate via CSS. Download from github.com/IBM/plex.

### Alternative pairings if IBM Plex + Source Serif 4 doesn't fit

- **Government/civic model**: Public Sans for everything — used by NSW Government and the US Web Design System, purpose-built for institutional digital services. Stack: `'Public Sans', Arial, sans-serif`. Australian-appropriate given its NSW government lineage.
- **Pure editorial model**: Newsreader (headlines) + Libre Franklin (body/data) — Newsreader was purpose-built for editorial contexts by Production Type; Libre Franklin descends from Franklin Gothic, the foundational American news grotesque used by NYT and Pew Research.
- **Single-family simplicity**: Work Sans throughout — designed by Australian typographer Wei Huang, geometric grotesque optimized for screens, 9 weights, tabular figures confirmed.

---

## Layout architecture: the ISA-101 hierarchy applied to Streamlit

### Four-level information architecture

Borrowing directly from control room display hierarchy:

**Level 1 — NEM Overview** (the landing view): Total NEM emissions intensity (gCO₂eq/kWh) as hero KPI at 40px. Five state indicators (NSW, VIC, QLD, SA, TAS) with current intensity and delta. A single stacked area chart showing current generation mix. Timestamp showing data currency: "AEMO dispatch data as at 14:35 AEST, 16 March 2026." This view answers: "What is happening right now across the grid?"

**Level 2 — Regional detail**: State-level deep dive selected from Level 1. Stacked area chart of generation by fuel type over the selected time window (7d default, with 24h/30d/12m/all toggles). Emissions intensity line overlaid. Price line on secondary axis. This view answers: "What is the emissions profile of this region and how is it trending?"

**Level 3 — Source/facility detail**: Individual fuel source or generator analysis. Capacity factors, emissions factors, marginal vs. average intensity. This view answers: "What is driving the emissions in this region?"

**Level 4 — Methodology and data**: Calculation methodology, emissions factor sources (NGA Factors), AEMO data documentation, data quality notes. This view builds trust through transparency.

### Streamlit implementation structure

```python
st.set_page_config(layout="wide", page_title="NEM Emissions Intensity")
```

**Header row**: Dashboard title (Source Serif 4, 32px) + last-updated timestamp + AEMO data source badge

**KPI row** (`st.columns(5)`): One `st.metric()` per NEM region showing current gCO₂eq/kWh with delta from 24h ago. The hero metric (NEM-wide) gets the leftmost, widest column.

**Main content** (`st.columns([3, 2])`): Left column (60%) holds the primary time-series stacked area chart. Right column (40%) holds the current fuel mix breakdown and a bullet graph showing intensity vs. target.

**Sidebar** (`st.sidebar`): Time period selector, region filter, methodology toggle (location-based vs. market-based for Scope 2), and an expander with data source details.

### Streamlit theme configuration

```toml
[theme]
base = "light"
primaryColor = "#1B4F72"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F4F6F9"
textColor = "#1C2833"

[[theme.fontFace]]
family = "IBM Plex Sans"
url = "https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&display=swap"

[[theme.fontFace]]
family = "Source Serif 4"
url = "https://fonts.googleapis.com/css2?family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700&display=swap"

[[theme.fontFace]]
family = "IBM Plex Mono"
url = "https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap"
```

### Custom CSS injection for Streamlit

```css
/* Inject via st.markdown('<style>...</style>', unsafe_allow_html=True) */

/* Global typography */
html, body, [class*="css"] {
  font-family: 'IBM Plex Sans', 'Helvetica Neue', Arial, sans-serif;
}

/* Serif headings */
h1, h2, h3 {
  font-family: 'Source Serif 4', Georgia, serif;
  color: #1C2833;
  letter-spacing: -0.01em;
}

h1 { font-size: 2rem; font-weight: 700; line-height: 1.25; }
h2 { font-size: 1.5rem; font-weight: 600; line-height: 1.25; }
h3 { font-size: 1.125rem; font-weight: 600; line-height: 1.3; }

/* KPI metric styling */
[data-testid="stMetricValue"] {
  font-family: 'IBM Plex Sans', sans-serif;
  font-variant-numeric: tabular-nums lining-nums;
  font-weight: 600;
  font-size: 2rem;
  letter-spacing: -0.02em;
}

[data-testid="stMetricLabel"] {
  font-family: 'IBM Plex Sans', sans-serif;
  font-size: 0.875rem;
  font-weight: 500;
  color: #626262;
  text-transform: none;
}

/* Source citation bar */
.source-citation {
  font-family: 'IBM Plex Sans', sans-serif;
  font-size: 0.75rem;
  color: #808080;
  border-top: 1px solid #E5E7EB;
  padding-top: 0.5rem;
  margin-top: 1rem;
}

/* Reduce Streamlit default padding for denser layout */
.block-container {
  padding-top: 2rem;
  padding-bottom: 1rem;
  max-width: 1200px;
}

/* Hide Streamlit branding for cleaner institutional feel */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; }
```

---

## Chart styling conventions drawn from all six traditions

### What makes a chart feel institutional

Every chart should include five elements that generic SaaS dashboards typically omit:

1. **A declarative title** stating the insight, not the metric. "Grid emissions intensity fell 12% as wind generation peaked" rather than "Emissions Intensity Over Time." This is the single most transferable convention from editorial data journalism.

2. **A subtitle** providing methodological context in muted gray. "Location-based Scope 2 intensity, 30-minute dispatch intervals, NEM regions" at 14px in #626262.

3. **Direct labels** on lines and areas rather than separate legends. When a stacked area chart shows five fuel types, label the largest areas directly within the colored region. Reserve legends only when direct labeling would create clutter.

4. **A source citation** at bottom-left in 12px gray: "Source: AEMO NEM dispatch data via OpenNEM. Emissions factors: National Greenhouse Accounts Factors 2025."

5. **Annotation callouts** pointing to significant events — "Liddell closure, April 2023" or "Record solar penetration, 12 Jan 2026" — placed directly on the chart with a subtle connecting line to the relevant data point.

### Plotly/Altair theming to match

For Plotly charts in Streamlit:

```python
import plotly.graph_objects as go
import plotly.io as pio

institutional_template = go.layout.Template(
    layout=go.Layout(
        font=dict(family="IBM Plex Sans", size=14, color="#1C2833"),
        title=dict(
            font=dict(family="Source Serif 4", size=18, color="#1C2833"),
            x=0, xanchor="left",
            pad=dict(l=0, t=0)
        ),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        xaxis=dict(
            gridcolor="#E5E7EB", gridwidth=0.5,
            linecolor="#1C2833", linewidth=0.5,
            tickfont=dict(size=12, color="#626262"),
            title_font=dict(size=13, color="#626262")
        ),
        yaxis=dict(
            gridcolor="#E5E7EB", gridwidth=0.5,
            linecolor="#1C2833", linewidth=0.5,
            tickfont=dict(size=12, color="#626262"),
            title_font=dict(size=13, color="#626262")
        ),
        margin=dict(l=60, r=20, t=60, b=60),
        colorway=[
            "#333333",  # Black coal
            "#8B6914",  # Brown coal
            "#FF8C00",  # Gas
            "#4682B4",  # Hydro
            "#3CB371",  # Wind
            "#FFD700",  # Solar utility
            "#FFF44F",  # Solar rooftop
            "#9370DB",  # Battery
        ],
        hoverlabel=dict(
            font=dict(family="IBM Plex Sans", size=13),
            bgcolor="white"
        )
    )
)
pio.templates["institutional"] = institutional_template
pio.templates.default = "institutional"
```

### Chart type selection guide for emissions data

Drawing from the FT Visual Vocabulary categories:

- **Current generation mix**: Stacked area chart (Part-to-Whole over Time) — the OpenNEM signature chart
- **Emissions intensity trend**: Line chart with shaded confidence band if forecasting
- **Performance vs. target**: Bullet graph (Stephen Few's invention) — compact, shows actual vs. target with qualitative ranges
- **Regional comparison**: Small multiples — five identical area charts, one per NEM region, sharing axes
- **Year-over-year progress**: Diverging bar chart (Deviation) — bars extend left (worse than target) or right (better)
- **Energy flow**: Sankey diagram — from fuel source → generation → transmission → consumption
- **Emissions breakdown by scope**: Waterfall chart — Scope 1 + Scope 2 + Scope 3 building to total

---

## The trust architecture: why details matter more than aesthetics

### Eight design choices that signal institutional authority

Financial decision-makers — the sustainability managers and finance teams using this tool — evaluate credibility through **design semiotics** before reading a single data point. Based on the research across Bloomberg Terminal conventions, ESG platform design, and Edward Tufte's principles:

- **Data provenance on every view.** "Source: AEMO" is not enough. Specify the dataset, update frequency, and methodology. "AEMO NEM Dispatch data, 5-minute intervals, updated every 5 minutes via OpenNEM API. Emissions factors: NGA Factors 2025, DISER."
- **Timestamp of last update.** Control rooms show this because stale data kills. Format: "Last updated: 16 Mar 2026, 14:35 AEST" — not "2 minutes ago."
- **Appropriate precision.** Show emissions intensity to one decimal place (423.7 gCO₂eq/kWh), not zero (424) or three (423.712). False precision erodes trust as much as false simplicity.
- **Consistent units throughout.** Pick gCO₂eq/kWh for intensity and tCO₂e for absolute emissions. Never switch between kg and tonnes or g and kg within the same view.
- **Methodology toggle.** Scope 2 can be reported location-based or market-based. Finance teams need both. Make the toggle visible, not buried.
- **Context for every metric.** A number without comparison is meaningless. Always show: current value, delta from prior period, target/benchmark, and where the value sits in its historical range.
- **Framework alignment indicators.** If the data supports GHG Protocol, NGER, or ISSB disclosures, badge it. This is how sustainability reports build trust.
- **No animations or transitions on data.** Control rooms and Bloomberg Terminal share this principle: data should appear instantly, not fade or slide in. Animation suggests entertainment; immediacy suggests operational seriousness.

### What Stephen Few and Edward Tufte would say about your dashboard

Few's core contribution: the **bullet graph** as a replacement for gauges and meters, and the principle that everything should fit on a single screen without scrolling. His 13 common dashboard mistakes include: choosing ineffective measures, supplying inadequate context, displaying excessive detail, using inappropriate chart types (no gauges, no 3D, no pie charts), introducing meaningless decoration, and misusing color.

Tufte's data-ink ratio principle — maximize the proportion of pixels representing actual data — translates directly: remove card borders, reduce grid lines to the minimum needed for reading values, eliminate redundant legends when direct labeling works, and use sparklines (word-sized inline charts) for compact trend indication in KPI cards.

---

## Where to find the reference materials

The most valuable references for ongoing design development, organized by immediate utility:

- **FT Visual Vocabulary** (chart type selection): ft-interactive.github.io/visual-vocabulary/
- **IPCC AR6 Visual Style Guide** (color, uncertainty, scientific figure design): ipcc.ch/site/assets/uploads/2022/09/IPCC_AR6_WGI_VisualStyleGuide_2022.pdf
- **Electricity Maps** (the gold standard for real-time grid emissions visualization): app.electricitymaps.com/map
- **Open Electricity / OpenNEM** (the Australian-specific reference): openelectricity.org.au
- **Deloitte Insights design system** (Pentagram case study): pentagram.com/work/deloitte-insights/story
- **Datawrapper blog on fonts for data viz**: datawrapper.de/blog/fonts-for-data-visualization
- **ISA-101 High-Performance HMI principles** (control room design for dashboards): documented in the High Performance HMI Handbook by Hollifield & Habibi
- **ColorBrewer** (perceptually uniform, accessible palettes): colorbrewer2.org
- **IPCC SSP pathway colors** (programmatic access via pyam): pyam-iamc.readthedocs.io/en/stable/tutorials/ipcc_colors.html
- **Australian Government Design System** (typography scale, accessibility): design.system.gov.au
- **NSW Design System** (data visualization guidance): designsystem.nsw.gov.au

## Conclusion: borrow from publishing, not from software

The dashboard traditions that will serve sustainability managers and finance teams best are not Stripe, Linear, or Notion — they are the Financial Times, the IPCC, AEMO's control room, and Deloitte's annual report. The concrete implementation path is straightforward: Source Serif 4 for headings that signal editorial authority, IBM Plex Sans with tabular figures for data that aligns and reads precisely, a five-color functional palette anchored in institutional blue with semantic use of green/amber/red, declarative chart titles that state insights rather than describe metrics, and the ISA-101 principle that **gray is the default state and color is earned by meaning**. The most radical design choice is restraint itself — every element your dashboard *doesn't* include builds more trust than the elements it does.