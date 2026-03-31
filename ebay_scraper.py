'use client'
import { useState, useEffect } from 'react'
import Link from 'next/link'
import { supabase, formatPrice, formatPct } from '@/lib/supabase'
import InlineChat from '@/components/InlineChat'
import PriceChart from '@/components/PriceChart'
import CardStructuredData from '@/components/CardStructuredData'
import { getSetAssets } from '@/lib/setAssets'

// ─── Constants ────────────────────────────────────────────────────────────────

// Card types that are NOT Pokémon — no species link for these
const NON_POKEMON_PREFIXES = [
  'energy', 'basic energy', 'special energy',
  'trainer', 'supporter', 'item', 'stadium', 'tool',
  'poké ball', 'pokeball', 'full heal', 'potion', 'revive',
  'professor', 'lillie', 'cynthia', 'marnie', 'hop', 'boss',
  'ultra ball', 'quick ball', 'level ball', 'nest ball',
  'great ball', 'master ball', 'timer ball',
]

const NON_POKEMON_SUFFIXES = [
  'energy', ' trainer', ' supporter', ' item', ' stadium',
]

// ─── Helpers ────────────────────────────────────────────────────────────────

function extractVariant(cardName: string): string | null {
  const match = cardName.match(/\[([^\]]+)\]/)
  return match ? match[1] : null
}

function buildEbayUrl(cardName: string, setName: string, cardNumber: string | null, region: 'UK' | 'US', mode: 'sold' | 'forsale') {
  const numberSuffix = cardNumber && !cardName.includes(`#${cardNumber}`) ? ` #${cardNumber}` : ''
  const q = encodeURIComponent(`${cardName}${numberSuffix} ${setName} pokemon card`)
  if (region === 'UK') {
    const base = 'https://www.ebay.co.uk/sch/i.html'
    return mode === 'sold'
      ? `${base}?_nkw=${q}&LH_Sold=1&LH_Complete=1&_sacat=2536`
      : `${base}?_nkw=${q}&LH_BIN=1&_sacat=2536`
  } else {
    const base = 'https://www.ebay.com/sch/i.html'
    return mode === 'sold'
      ? `${base}?_nkw=${q}&LH_Sold=1&LH_Complete=1&_sacat=2536`
      : `${base}?_nkw=${q}&LH_BIN=1&_sacat=2536`
  }
}

// Extract the base Pokémon name for species linking
// "Charizard ex" → "charizard", "Umbreon VMAX #215" → "umbreon"
function extractSpeciesSlug(cardName: string): string | null {
  const lower = cardName.toLowerCase()

  // Check if this is a non-Pokémon card
  for (const prefix of NON_POKEMON_PREFIXES) {
    if (lower.startsWith(prefix)) return null
  }
  for (const suffix of NON_POKEMON_SUFFIXES) {
    if (lower.endsWith(suffix)) return null
  }

  // Strip number, brackets, and suffixes to get base name
  const cleaned = cardName
    .replace(/\s*#\d+.*$/, '')           // remove #123 onwards
    .replace(/\[.*?\]/g, '')             // remove [Reverse Holo] etc
    .replace(/\b(ex|EX|V|VMAX|VSTAR|GX|Tag Team|Prime|LV\.X|Break|Legend|SP|FB|GL|C|4|δ)\b.*/g, '') // stop at mechanic suffix
    .trim()

  if (!cleaned) return null

  // Take first word as species slug
  const slug = cleaned.split(/\s+/)[0].toLowerCase().replace(/[^a-z0-9-]/g, '')
  if (slug.length < 3) return null

  return slug
}

// ─── Data quality policy ─────────────────────────────────────────────────────
type PctQuality = 'clean' | 'warn' | 'suppress'

const PERIOD_THRESHOLDS: Record<string, { warn: number; suppress: number }> = {
  '7d':   { warn: 50,       suppress: 200 },
  '30d':  { warn: 100,      suppress: 300 },
  '90d':  { warn: 200,      suppress: 500 },
  '180d': { warn: 300,      suppress: 700 },
  '1y':   { warn: 400,      suppress: 900 },
  '2y':   { warn: Infinity, suppress: Infinity },
  '5y':   { warn: Infinity, suppress: Infinity },
}

function pctQuality(pct: number | null, period = '30d'): PctQuality {
  if (pct == null) return 'clean'
  const abs = Math.abs(pct)
  const thresholds = PERIOD_THRESHOLDS[period] ?? PERIOD_THRESHOLDS['30d']
  if (abs > thresholds.suppress) return 'suppress'
  if (abs > thresholds.warn) return 'warn'
  return 'clean'
}

// ─── Fallback insight generator ───────────────────────────────────────────────
// Builds a meaningful 2-3 sentence insight from available data
// Used when get_card_insight RPC returns null
function generateFallbackInsight(
  card: any,
  trend: any,
  metrics: any,
  psaPop: any,
): string | null {
  if (!card) return null

  const raw = metrics?.results?.[0] || metrics || {}
  const parts: string[] = []

  const cardName = card.card_name
  const setName  = card.set_name
  const rawUsd   = card.raw_usd ? (card.raw_usd / 100).toFixed(2) : null
  const psa10Usd = card.psa10_usd ? (card.psa10_usd / 100).toFixed(2) : null

  // Opening — price context
  if (rawUsd) {
    if (psa10Usd) {
      const multiple = (card.psa10_usd / card.raw_usd).toFixed(1)
      parts.push(
        `${cardName} from ${setName} currently trades at $${rawUsd} raw, with PSA 10 copies reaching $${psa10Usd} — a ${multiple}x grade premium.`
      )
    } else {
      parts.push(`${cardName} from ${setName} currently trades at $${rawUsd} raw.`)
    }
  }

  // Trend context
  const pct30d = trend?.raw_pct_30d
  const pct90d = trend?.raw_pct_90d
  const q30 = pctQuality(pct30d, '30d')
  const q90 = pctQuality(pct90d, '90d')

  if (pct30d != null && q30 === 'clean' && q90 === 'clean' && pct90d != null) {
    const dir30 = pct30d > 0 ? 'up' : 'down'
    const dir90 = pct90d > 0 ? 'up' : 'down'
    if (Math.sign(pct30d) === Math.sign(pct90d)) {
      parts.push(
        `The card has moved ${dir30} ${Math.abs(pct30d).toFixed(1)}% over the past 30 days and ${dir90} ${Math.abs(pct90d).toFixed(1)}% over 90 days, suggesting ${pct30d > 0 ? 'sustained buying interest' : 'continued softness'}.`
      )
    } else {
      parts.push(
        `Price is ${dir30} ${Math.abs(pct30d).toFixed(1)}% in the last 30 days, though the 90-day trend tells a different story at ${pct90d > 0 ? '+' : ''}${pct90d.toFixed(1)}% — worth watching for a clearer direction.`
      )
    }
  } else if (pct30d != null && q30 === 'clean') {
    const dir = pct30d > 0 ? 'gained' : 'lost'
    parts.push(
      `The card has ${dir} ${Math.abs(pct30d).toFixed(1)}% over the past 30 days.`
    )
  }

  // PSA pop / grading context
  if (psaPop && psaPop.total_graded > 0 && psaPop.gem_rate != null) {
    const gemRate = parseFloat(psaPop.gem_rate)
    const total = psaPop.total_graded.toLocaleString()
    if (gemRate < 5) {
      parts.push(
        `Only ${gemRate.toFixed(1)}% of ${total} graded copies have achieved PSA 10, making gem-quality examples genuinely scarce.`
      )
    } else if (gemRate > 40) {
      parts.push(
        `With ${gemRate.toFixed(1)}% of ${total} graded copies receiving PSA 10, gem examples are relatively plentiful — which moderates the grade premium.`
      )
    } else if (card.psa10_usd && card.raw_usd) {
      parts.push(
        `PSA population stands at ${total} graded with a ${gemRate.toFixed(1)}% gem rate.`
      )
    }
  }

  // ATH / drawdown context
  const drawdown = raw.drawdown_pct != null ? parseFloat(raw.drawdown_pct) : null
  if (drawdown != null && drawdown < -30 && parts.length < 3) {
    parts.push(
      `At ${drawdown.toFixed(0)}% below its all-time high, the card sits at a notable discount to peak — a potential entry point for patient collectors.`
    )
  } else if (drawdown != null && drawdown > -5 && parts.length < 3) {
    parts.push(`The card is trading near its all-time high.`)
  }

  // Volume / liquidity
  const salesMonthly = raw.sales_30d ?? raw.volume_30d ?? null
  if (salesMonthly != null && parts.length < 3) {
    if (salesMonthly < 2) {
      parts.push(`Volume is very thin at roughly ${salesMonthly} sale per month — treat any single data point with caution.`)
    } else if (salesMonthly >= 20) {
      parts.push(`With around ${salesMonthly} sales per month, this is a highly liquid card with a reliable price signal.`)
    }
  }

  if (parts.length === 0) return null
  return parts.slice(0, 3).join(' ')
}

// ─── Sub-components ──────────────────────────────────────────────────────────

function TrendCell({ val, label }: { val: number | null; label: string }) {
  const quality = pctQuality(val, label)
  if (val == null || quality === 'suppress') {
    return (
      <div>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2, fontFamily: "'Figtree', sans-serif" }}>{label}</div>
        <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--border)', fontFamily: "'Figtree', sans-serif" }}>—</div>
      </div>
    )
  }
  if (quality === 'warn') {
    return (
      <div title="Unusually large move for this timeframe — may reflect a stale price anchor rather than real market activity. Check sold listings.">
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2, fontFamily: "'Figtree', sans-serif" }}>{label}</div>
        <div style={{ fontSize: 15, fontWeight: 700, color: '#e07b39', fontFamily: "'Figtree', sans-serif" }}>
          ⚠ {val > 0 ? '+' : ''}{val.toFixed(1)}%
        </div>
      </div>
    )
  }
  const f = formatPct(val)
  return (
    <div>
      <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2, fontFamily: "'Figtree', sans-serif" }}>{label}</div>
      <div style={{ fontSize: 15, fontWeight: 700, color: f.color, fontFamily: "'Figtree', sans-serif" }}>{f.text}</div>
    </div>
  )
}

function HeroBanner({ trend, hasInsight }: { trend: any; hasInsight: boolean }) {
  if (!trend || hasInsight) return null

  const periods = [
    { label: '30d', val: trend.raw_pct_30d },
    { label: '7d',  val: trend.raw_pct_7d  },
    { label: '90d', val: trend.raw_pct_90d  },
  ]
  const main = periods.find(p => p.val != null)
  if (!main || main.val == null) return null

  const quality = pctQuality(main.val, main.label)
  if (quality === 'suppress') return null

  const isUp = main.val > 0
  const absPct = Math.abs(main.val)

  if (quality === 'warn') {
    return (
      <div style={{
        background: 'rgba(224,123,57,0.08)', border: '1px solid rgba(224,123,57,0.25)',
        borderLeft: '3px solid #e07b39', borderRadius: 10,
        padding: '10px 14px', marginBottom: 16,
        fontSize: 13, lineHeight: 1.6, color: 'var(--text)',
        fontFamily: "'Figtree', sans-serif",
      }}>
        ⚠️ <strong>{isUp ? 'Up' : 'Down'} {absPct.toFixed(0)}% in {main.label}</strong> — unusually large move.
        This may reflect a stale price anchor rather than real market activity.
        Check recent sold listings before drawing conclusions.
      </div>
    )
  }

  return (
    <div style={{
      background: isUp ? 'rgba(39,174,96,0.06)' : 'rgba(239,68,68,0.06)',
      border: `1px solid ${isUp ? 'rgba(39,174,96,0.2)' : 'rgba(239,68,68,0.2)'}`,
      borderLeft: `3px solid ${isUp ? 'var(--green)' : '#ef4444'}`,
      borderRadius: 10, padding: '10px 14px', marginBottom: 16,
      fontSize: 13, lineHeight: 1.6, color: 'var(--text)',
      fontFamily: "'Figtree', sans-serif",
    }}>
      <strong>{isUp ? 'Up' : 'Down'} {absPct.toFixed(1)}% in {main.label}</strong>
    </div>
  )
}

function SectionLabel({ children, style = {} }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{
      fontSize: 10, fontWeight: 800, textTransform: 'uppercase',
      letterSpacing: 1.8, color: 'var(--text-muted)', marginBottom: 14,
      fontFamily: "'Figtree', sans-serif", ...style,
    }}>{children}</div>
  )
}

function Panel({ children, style = {} }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{
      background: 'var(--card)', borderRadius: 14, border: '1px solid var(--border)',
      padding: '18px 20px', marginTop: 20, ...style,
    }}>{children}</div>
  )
}

function EbayButton({ href, label, flag, variant = 'secondary' }: {
  href: string; label: string; flag: string; variant?: 'primary' | 'secondary'
}) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer" style={{
      display: 'inline-flex', alignItems: 'center', gap: 6,
      padding: '8px 14px', borderRadius: 10, textDecoration: 'none',
      fontSize: 12, fontWeight: 700, fontFamily: "'Figtree', sans-serif",
      border: '1px solid var(--border)',
      background: variant === 'primary' ? 'var(--primary)' : 'var(--bg-light)',
      color: variant === 'primary' ? '#fff' : 'var(--text)',
      whiteSpace: 'nowrap', transition: 'opacity 0.15s',
    }}
      onMouseEnter={e => { (e.currentTarget as HTMLAnchorElement).style.opacity = '0.8' }}
      onMouseLeave={e => { (e.currentTarget as HTMLAnchorElement).style.opacity = '1' }}
    >
      <span style={{ fontSize: 13 }}>{flag}</span>{label}
    </a>
  )
}

function MeterBar({ value, color = 'var(--primary)' }: { value: number; color?: string }) {
  const pct = Math.min(100, Math.max(0, value))
  return (
    <div style={{ height: 6, background: 'var(--bg-light)', borderRadius: 99, overflow: 'hidden' }}>
      <div style={{
        height: '100%', width: `${pct}%`, background: color,
        borderRadius: 99, transition: 'width 0.6s ease',
      }} />
    </div>
  )
}

function StatTile({ label, value, sub, highlight = false }: {
  label: string; value: string; sub?: string; highlight?: boolean
}) {
  return (
    <div style={{
      background: highlight ? 'rgba(26,95,173,0.06)' : 'var(--bg-light)',
      border: `1px solid ${highlight ? 'rgba(26,95,173,0.2)' : 'transparent'}`,
      borderRadius: 10, padding: '12px 14px',
    }}>
      <div style={{
        fontSize: 18, fontWeight: 800, color: 'var(--text)',
        fontFamily: "'Figtree', sans-serif", lineHeight: 1, marginBottom: 4,
      }}>{value}</div>
      <div style={{
        fontSize: 10, fontWeight: 700, color: 'var(--text-muted)',
        fontFamily: "'Figtree', sans-serif", textTransform: 'uppercase', letterSpacing: 1,
      }}>{label}</div>
      {sub && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3, fontFamily: "'Figtree', sans-serif" }}>{sub}</div>}
    </div>
  )
}

function SignalRow({ label, value, type }: { label: string; value: string; type: 'good' | 'warn' | 'neutral' }) {
  const colors = {
    good:    { text: 'var(--green)',  bg: 'rgba(39,174,96,0.08)'  },
    warn:    { text: '#e07b39',       bg: 'rgba(224,123,57,0.08)' },
    neutral: { text: 'var(--text)',   bg: 'var(--bg-light)'       },
  }
  const c = colors[type]
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
      <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>{label}</span>
      <span style={{
        fontSize: 12, fontWeight: 700, fontFamily: "'Figtree', sans-serif",
        color: c.text, background: c.bg, padding: '2px 8px', borderRadius: 20,
        whiteSpace: 'nowrap',
      }}>{value}</span>
    </div>
  )
}

// ─── Main ────────────────────────────────────────────────────────────────────

export default function CardPageClient({ setName, cardUrlSlug }: { setName: string; cardUrlSlug: string }) {
  const [card, setCard] = useState<any>(null)
  const [trend, setTrend] = useState<any>(null)
  const [metrics, setMetrics] = useState<any>(null)
  const [priceHistory, setPriceHistory] = useState<any[]>([])
  const [insight, setInsight] = useState<string | null>(null)
  const [psaPop, setPsaPop] = useState<any | null>(null)
  const [ebayListings, setEbayListings] = useState<any[]>([])
  const [ebayDeals, setEbayDeals] = useState<any[]>([])
  const [highConfidenceListingCount, setHighConfidenceListingCount] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function loadCard() {
      const { data: cardData } = await supabase.rpc('get_card_detail_by_url_slug', {
        p_set_name: setName,
        p_card_url_slug: cardUrlSlug,
      })
      if (!cardData) { setLoading(false); return }
      setCard(cardData)

      const slug = cardData.card_slug

      const [trendRes, metricsRes, histRes, insightRes] = await Promise.all([
        supabase.rpc('get_card_trends_detail', { slug }),
        supabase.rpc('get_card_metrics', { card_slug: cardData.card_slug }),
        supabase.rpc('get_card_price_history', { slug }),
        supabase.rpc('get_card_insight', { slug }),
      ])

      if (trendRes.data) setTrend(trendRes.data)
      if (metricsRes.data) {
        const m = Array.isArray(metricsRes.data) ? metricsRes.data[0] : metricsRes.data
        setMetrics(m)
      }
      if (histRes.data) setPriceHistory(histRes.data)
      if (insightRes.data) setInsight(insightRes.data)

      // PSA population
      const baseName = cardData.card_name.split('[')[0].split('#')[0].trim()
      const variant = extractVariant(cardData.card_name)
      const setNameClean = cardData.set_name.replace(/^Pokemon /, '')

      let popQuery = supabase
        .from('psa_population')
        .select('card_name, variant, set_name, card_number, psa_7, psa_8, psa_9, psa_10, total_graded, gem_rate')
        .ilike('card_name', `%${baseName}%`)
        .ilike('set_name', `%${setNameClean}%`)
        .gt('total_graded', 0)

      if (variant) {
        popQuery = popQuery.ilike('variant', `%${variant}%`)
      } else {
        popQuery = popQuery.or('variant.is.null,variant.eq.,variant.ilike.Standard%')
      }

      const { data: popData } = await popQuery.order('total_graded', { ascending: false }).limit(1)
      if (popData && popData.length > 0) setPsaPop(popData[0])

      // ── eBay: HIGH confidence only ──────────────────────────────────
      // First try deals
      const { data: dealsData } = await supabase
        .from('daily_deals')
        .select('*')
        .eq('card_slug', slug)
        .order('discount_pct', { ascending: false })
        .limit(5)

      if (dealsData && dealsData.length > 0) {
        setEbayDeals(dealsData)
      } else {
        // Only high confidence listings — no medium
        const { data: listingsData } = await supabase
          .from('ebay_listings')
          .select('*')
          .eq('card_slug', slug)
          .eq('match_confidence', 'high')   // HIGH only
          .order('total_cost_cents', { ascending: true })
          .limit(5)
        if (listingsData) setEbayListings(listingsData)
      }

      // Count high-confidence listings
      const { count } = await supabase
        .from('ebay_listings')
        .select('*', { count: 'exact', head: true })
        .eq('card_slug', slug)
        .eq('match_confidence', 'high')
      if (count != null) setHighConfidenceListingCount(count)

      setLoading(false)
    }
    loadCard()
  }, [setName, cardUrlSlug])

  if (loading) return (
    <div style={{ maxWidth: 960, margin: '0 auto', padding: '60px 24px' }}>
      <div style={{ display: 'flex', gap: 32, flexWrap: 'wrap' }}>
        <div className="skeleton" style={{ width: 220, height: 308, borderRadius: 10 }} />
        <div style={{ flex: 1, minWidth: 280 }}>
          <div className="skeleton" style={{ height: 32, width: '60%', marginBottom: 12 }} />
          <div className="skeleton" style={{ height: 18, width: '40%', marginBottom: 24 }} />
          <div className="skeleton" style={{ height: 160, borderRadius: 12 }} />
        </div>
      </div>
    </div>
  )

  if (!card) return (
    <div style={{ maxWidth: 800, margin: '0 auto', padding: '60px 24px', textAlign: 'center' }}>
      <h1 style={{ fontFamily: "'Playfair Display', serif", fontSize: 28, marginBottom: 12 }}>Card not found</h1>
      <p style={{ color: 'var(--text-muted)' }}>This card doesn&apos;t exist in our database.</p>
      <Link href="/browse" style={{ color: 'var(--accent)', textDecoration: 'none', fontWeight: 600 }}>Browse sets →</Link>
    </div>
  )

  // ── Derived values ──────────────────────────────────────────────────────────

  const raw = metrics?.results?.[0] || metrics || {}

  const grades = [
    { label: 'Raw',     value: card.raw_usd   },
    { label: 'PSA 7',   value: card.psa7_usd  },
    { label: 'PSA 8',   value: card.psa8_usd  },
    { label: 'PSA 9',   value: card.psa9_usd  },
    { label: 'PSA 10',  value: card.psa10_usd },
    { label: 'CGC 9.5', value: card.cgc95_usd },
  ]

  const hasAnyTrend = trend && (
    trend.raw_pct_7d !== null || trend.raw_pct_30d !== null ||
    trend.raw_pct_90d !== null || trend.raw_pct_180d !== null || trend.raw_pct_365d !== null
  )

  const trendPeriods = hasAnyTrend ? [
    { label: '7d',   val: trend.raw_pct_7d   },
    { label: '30d',  val: trend.raw_pct_30d  },
    { label: '90d',  val: trend.raw_pct_90d  },
    { label: '180d', val: trend.raw_pct_180d },
    { label: '1y',   val: trend.raw_pct_365d },
    { label: '2y',   val: trend.raw_pct_2y   },
    { label: '5y',   val: trend.raw_pct_5y   },
  ] : []

  const psa10Multiple = card.psa10_usd && card.raw_usd ? card.psa10_usd / card.raw_usd : null
  const psa9Multiple  = card.psa9_usd  && card.raw_usd ? card.psa9_usd  / card.raw_usd : null

  const gradingCostCents = 2500
  const gradingProfitCents = card.raw_usd && card.psa10_usd
    ? card.psa10_usd - card.raw_usd - gradingCostCents
    : null

  const hasLongTermData = trend && (trend.raw_365d_ago != null || trend.raw_pct_365d != null)
  const drawdown = raw.drawdown_pct != null && hasLongTermData ? parseFloat(raw.drawdown_pct) : null
  const athUsd = drawdown != null && card.raw_usd
    ? Math.round((card.raw_usd / 100) / (1 + drawdown / 100))
    : null

  const salesMonthly = raw.sales_30d ?? raw.volume_30d ?? null
  const liquidityScore = salesMonthly != null ? Math.min(100, Math.round((salesMonthly / 20) * 100)) : null
  const liquidityLabel = liquidityScore == null ? null
    : liquidityScore >= 70 ? 'High — sells quickly'
    : liquidityScore >= 35 ? 'Medium — may take a few weeks'
    : 'Low — patient seller needed'

  const gemRate = psaPop?.gem_rate ? parseFloat(psaPop.gem_rate) : null

  const expectedValueCents: number | null = card.raw_usd && card.psa10_usd && gemRate != null
    ? (() => {
        const p10 = gemRate / 100
        const p9  = (1 - p10) * 0.6
        const p9val = card.psa9_usd ?? Math.round(card.psa10_usd * 0.25)
        const expectedReturn = (p10 * card.psa10_usd) + (p9 * p9val) + ((1 - p10 - p9) * card.raw_usd * 0.8)
        return Math.round(expectedReturn - card.raw_usd - gradingCostCents)
      })()
    : null

  const isLotteryCard  = psa10Multiple != null && psa10Multiple > 15
  const totalGraded    = psaPop?.total_graded ?? null
  const psa10Count     = psaPop?.psa_10 ?? null

  const gradeMultiples = [
    { label: 'PSA 9',  value: card.psa9_usd,  multiple: psa9Multiple  },
    { label: 'PSA 10', value: card.psa10_usd, multiple: psa10Multiple },
  ].filter(g => g.value && g.multiple)

  const signals: { label: string; value: string; type: 'good' | 'warn' | 'neutral' }[] = []

  if (drawdown != null) {
    if (drawdown < -40)      signals.push({ label: 'vs All-Time High', value: `${drawdown.toFixed(0)}% below peak`,  type: 'good'    })
    else if (drawdown < -15) signals.push({ label: 'vs All-Time High', value: `${drawdown.toFixed(0)}% below peak`,  type: 'neutral' })
    else                     signals.push({ label: 'vs All-Time High', value: `Near peak (${drawdown.toFixed(0)}%)`, type: 'warn'    })
  }

  const pct30dQuality = pctQuality(trend?.raw_pct_30d, '30d')
  if (raw.slope_30d != null && pct30dQuality !== 'suppress') {
    const slope = parseFloat(raw.slope_30d)
    if      (slope > 50)  signals.push({ label: '30d Momentum', value: 'Rising strongly', type: 'good'    })
    else if (slope > 0)   signals.push({ label: '30d Momentum', value: 'Trending up',     type: 'good'    })
    else if (slope < -50) signals.push({ label: '30d Momentum', value: 'Falling sharply', type: 'warn'    })
    else                  signals.push({ label: '30d Momentum', value: 'Flat / sideways', type: 'neutral' })
  }

  if (gemRate != null && card.psa10_usd && card.raw_usd) {
    if      (gemRate < 5 && psa10Multiple && psa10Multiple > 3)
      signals.push({ label: 'PSA 10 Rarity',  value: `${gemRate.toFixed(1)}% gem rate — scarce`,    type: 'good'    })
    else if (gemRate > 40)
      signals.push({ label: 'PSA 10 Supply',  value: `${gemRate.toFixed(1)}% gem rate — plentiful`, type: 'warn'    })
    else
      signals.push({ label: 'PSA 10 Gem Rate',value: `${gemRate.toFixed(1)}%`,                       type: 'neutral' })
  }
  if (psa10Multiple != null) {
    if      (psa10Multiple > 4)   signals.push({ label: 'Best Grade Value', value: `PSA 9 — PSA 10 is ${psa10Multiple.toFixed(1)}x raw`,    type: 'neutral' })
    else if (psa10Multiple < 1.8) signals.push({ label: 'Best Grade Value', value: `PSA 10 great value (${psa10Multiple.toFixed(1)}x raw)`, type: 'good'    })
  }
  if (liquidityLabel) {
    const lType = liquidityScore! >= 70 ? 'good' : liquidityScore! >= 35 ? 'neutral' : 'warn'
    signals.push({ label: 'Liquidity', value: liquidityLabel, type: lType })
  }

  const hasDeals    = ebayDeals.length > 0
  const hasListings = ebayListings.length > 0
  const cardNumber  = card.card_number ? ` #${card.card_number}` : ''
  const prefillMessage = `I'm looking at ${card.card_name}${cardNumber} from ${card.set_name}`

  const ebayUkForSale = buildEbayUrl(card.card_name, card.set_name, card.card_number, 'UK', 'forsale')
  const ebayUkSold    = buildEbayUrl(card.card_name, card.set_name, card.card_number, 'UK', 'sold')
  const ebayUsForSale = buildEbayUrl(card.card_name, card.set_name, card.card_number, 'US', 'forsale')
  const ebayUsSold    = buildEbayUrl(card.card_name, card.set_name, card.card_number, 'US', 'sold')

  // ── Set assets ─────────────────────────────────────────────────────────────
  const { logoUrl, symbolUrl } = getSetAssets(card.set_name)

  // ── Species link ───────────────────────────────────────────────────────────
  const speciesSlug = extractSpeciesSlug(card.card_name)

  // ── Insight: RPC first, fallback if null ───────────────────────────────────
  const displayInsight = insight || generateFallbackInsight(card, trend, metrics, psaPop)

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 960, margin: '0 auto', padding: '36px 24px' }}>
      <CardStructuredData card={card} />

      {/* ── Breadcrumb: set logo + symbol + name + species link ── */}
      <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <Link
          href={`/set/${encodeURIComponent(card.set_name)}`}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            textDecoration: 'none', color: 'var(--text-muted)',
            fontSize: 13, fontFamily: "'Figtree', sans-serif",
            padding: '5px 12px 5px 8px',
            background: 'var(--card)', border: '1px solid var(--border)',
            borderRadius: 20, transition: 'border-color 0.15s',
          }}
          onMouseEnter={e => { (e.currentTarget as HTMLAnchorElement).style.borderColor = 'var(--primary)' }}
          onMouseLeave={e => { (e.currentTarget as HTMLAnchorElement).style.borderColor = 'var(--border)' }}
        >
          {logoUrl ? (
            <img src={logoUrl} alt={card.set_name} style={{ height: 20, width: 'auto', objectFit: 'contain', maxWidth: 90 }} loading="lazy" />
          ) : (
            <span style={{ fontSize: 13 }}>←</span>
          )}
          {symbolUrl && (
            <img src={symbolUrl} alt="" style={{ width: 16, height: 16, objectFit: 'contain' }} loading="lazy" />
          )}
          <span>{card.set_name}</span>
        </Link>

        {/* Species link — only for Pokémon cards */}
        {speciesSlug && (
          <Link
            href={`/pokemon/${speciesSlug}`}
            style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              textDecoration: 'none', color: 'var(--text-muted)',
              fontSize: 13, fontFamily: "'Figtree', sans-serif",
              padding: '5px 14px',
              background: 'var(--card)', border: '1px solid var(--border)',
              borderRadius: 20, transition: 'border-color 0.15s',
            }}
            onMouseEnter={e => { (e.currentTarget as HTMLAnchorElement).style.borderColor = 'var(--primary)' }}
            onMouseLeave={e => { (e.currentTarget as HTMLAnchorElement).style.borderColor = 'var(--border)' }}
          >
            <span style={{ fontSize: 12 }}>🃏</span>
            All {speciesSlug.charAt(0).toUpperCase() + speciesSlug.slice(1)} cards
          </Link>
        )}
      </div>

      <div style={{ margin: '0 0 28px' }}>
        <InlineChat cardContext={`${card.card_name} from ${card.set_name}`} prefillMessage={prefillMessage} />
      </div>

      {/* ── Hero: image + core data ─────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 28, flexWrap: 'wrap', alignItems: 'flex-start' }}>
        <div style={{ flex: '0 0 auto' }}>
          {card.image_url ? (
            <img src={card.image_url} alt={card.card_name} style={{
              width: 220, borderRadius: 12, boxShadow: '0 8px 32px rgba(0,0,0,0.15)',
            }} />
          ) : (
            <div style={{
              width: 220, height: 308, background: 'var(--bg)', borderRadius: 12,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: 'var(--text-muted)', fontSize: 40, border: '1px solid var(--border)',
            }}>🃏</div>
          )}
        </div>

        <div style={{ flex: 1, minWidth: 280 }}>
          <h1 style={{
            fontFamily: "'Playfair Display', serif", fontSize: 26,
            margin: '0 0 4px', color: 'var(--text)', letterSpacing: '-0.3px',
          }}>{card.card_name}</h1>
          <p style={{ color: 'var(--text-muted)', fontSize: 14, margin: '0 0 16px', fontFamily: "'Figtree', sans-serif" }}>
            {card.set_name}{card.card_number ? ` · #${card.card_number}` : ''}
          </p>

          <HeroBanner trend={trend} hasInsight={!!displayInsight} />

          {/* Insight — RPC or fallback */}
          {displayInsight && (
            <div style={{
              background: 'var(--card)', border: '1px solid var(--border)',
              borderLeft: '3px solid var(--primary)', borderRadius: 10,
              padding: '10px 14px', marginBottom: 16,
              fontSize: 13, lineHeight: 1.6, color: 'var(--text)',
              fontFamily: "'Figtree', sans-serif",
            }}>{displayInsight}</div>
          )}

          {/* Prices */}
          <div style={{
            background: 'var(--card)', borderRadius: 12, border: '1px solid var(--border)',
            padding: '14px 16px', marginBottom: 14,
          }}>
            <SectionLabel>Current Prices (USD)</SectionLabel>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10 }}>
              {grades.map(g => (
                <div key={g.label}>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2, fontFamily: "'Figtree', sans-serif" }}>{g.label}</div>
                  <div style={{
                    fontSize: 16, fontWeight: 700, fontFamily: "'Figtree', sans-serif",
                    color: g.value ? 'var(--text)' : 'var(--border)',
                  }}>{formatPrice(g.value)}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Trend */}
          {trendPeriods.length > 0 && (
            <div style={{
              background: 'var(--card)', borderRadius: 12, border: '1px solid var(--border)',
              padding: '14px 16px',
            }}>
              <SectionLabel>Raw Price Trend</SectionLabel>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
                {trendPeriods.map(t => <TrendCell key={t.label} val={t.val} label={t.label} />)}
              </div>
              {trendPeriods.some(t => pctQuality(t.val, t.label) === 'suppress') && (
                <p style={{ fontSize: 11, color: 'var(--text-muted)', margin: '10px 0 0', fontFamily: "'Figtree', sans-serif", lineHeight: 1.5 }}>
                  — Some periods are hidden because the price movement was too large to be reliable.
                  This usually means the card traded infrequently and the comparison price is outdated.
                  Check recent eBay sold listings for the real picture.
                </p>
              )}
              {trendPeriods.some(t => pctQuality(t.val, t.label) === 'warn') && !trendPeriods.some(t => pctQuality(t.val, t.label) === 'suppress') && (
                <p style={{ fontSize: 11, color: '#e07b39', margin: '10px 0 0', fontFamily: "'Figtree', sans-serif", lineHeight: 1.5 }}>
                  ⚠ Large moves shown in amber — verify against recent sold listings before drawing conclusions.
                </p>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── Collector Intel ──────────────────────────────────────────── */}
      {(() => {
        const tiles: { label: string; value: string; sub?: string; highlight?: boolean; color?: string }[] = []

        if (psa10Multiple != null) tiles.push({
          label: 'Raw → PSA 10', value: `${psa10Multiple.toFixed(1)}x`,
          sub: psa10Multiple < 2 ? 'Great grade value' : psa10Multiple > 10 ? 'High-risk grade' : 'Grade multiplier',
          highlight: psa10Multiple < 2,
        })
        if (psa9Multiple != null) tiles.push({
          label: 'Raw → PSA 9', value: `${psa9Multiple.toFixed(1)}x`, sub: 'Grade multiplier',
        })
        if (gemRate != null) tiles.push({
          label: 'Gem Rate (PSA 10)', value: `${gemRate.toFixed(1)}%`,
          sub: gemRate < 5 ? 'Hard to gem — scarce' : gemRate > 40 ? 'Common gem — high supply' : 'Average difficulty',
          highlight: gemRate < 5,
        })
        if (psaPop?.total_graded) tiles.push({
          label: 'Total PSA Graded', value: psaPop.total_graded.toLocaleString(),
          sub: psaPop.psa_10 ? `${psaPop.psa_10.toLocaleString()} PSA 10s` : undefined,
        })
        if (drawdown != null) tiles.push({
          label: 'vs All-Time High', value: `${drawdown.toFixed(0)}%`,
          sub: athUsd ? `ATH ~$${athUsd}` : 'from peak',
          highlight: drawdown < -30,
        })
        if (salesMonthly != null) tiles.push({
          label: 'Avg Sales / Month', value: `~${salesMonthly}`,
          sub: salesMonthly >= 10 ? 'Liquid market' : salesMonthly >= 3 ? 'Moderate volume' : 'Thin market',
        })
        if (highConfidenceListingCount != null && highConfidenceListingCount > 0) tiles.push({
          label: 'Live Listings', value: highConfidenceListingCount.toString(), sub: 'Verified matches',
        })
        const pct30dQualityLocal = pctQuality(trend?.raw_pct_30d, '30d')
        if (trend?.raw_pct_30d != null && pct30dQualityLocal === 'clean') tiles.push({
          label: '30d Price Move',
          value: `${trend.raw_pct_30d > 0 ? '+' : ''}${trend.raw_pct_30d.toFixed(1)}%`,
          sub: trend.raw_pct_30d > 0 ? 'Rising' : 'Falling',
          color: trend.raw_pct_30d > 0 ? 'var(--green)' : '#ef4444',
        })
        if (raw.volatility_30d != null) {
          const v = parseFloat(raw.volatility_30d)
          tiles.push({
            label: 'Price Stability',
            value: v < 5 ? 'Steady' : v < 15 ? 'Moderate' : 'Volatile',
            sub: `${v.toFixed(1)}% daily swing`,
          })
        }
        if (liquidityScore != null) tiles.push({
          label: 'Liquidity',
          value: liquidityScore >= 70 ? 'High' : liquidityScore >= 35 ? 'Medium' : 'Low',
          sub: liquidityScore >= 70 ? 'Sells quickly' : liquidityScore >= 35 ? 'May take weeks' : 'Patient seller needed',
          color: liquidityScore >= 70 ? 'var(--green)' : liquidityScore < 35 ? '#ef4444' : undefined,
        })
        if (expectedValueCents != null) tiles.push({
          label: 'Expected Grade Value',
          value: `${expectedValueCents > 0 ? '+' : ''}$${(expectedValueCents / 100).toFixed(0)}`,
          sub: gemRate != null ? `at ${gemRate.toFixed(1)}% gem rate` : 'probability-weighted',
          highlight: expectedValueCents > 0,
        })

        if (tiles.length === 0) return null

        return (
          <Panel>
            <SectionLabel>Collector Intel</SectionLabel>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 10, marginBottom: 8 }}>
              {tiles.map((t, i) => (
                <div key={i} style={{
                  background: t.highlight ? 'rgba(26,95,173,0.06)' : 'var(--bg-light)',
                  border: `1px solid ${t.highlight ? 'rgba(26,95,173,0.2)' : 'transparent'}`,
                  borderRadius: 10, padding: '12px 14px',
                }}>
                  <div style={{ fontSize: 18, fontWeight: 800, lineHeight: 1, marginBottom: 4, fontFamily: "'Figtree', sans-serif", color: t.color ?? 'var(--text)' }}>{t.value}</div>
                  <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif", textTransform: 'uppercase', letterSpacing: 1 }}>{t.label}</div>
                  {t.sub && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3, fontFamily: "'Figtree', sans-serif" }}>{t.sub}</div>}
                </div>
              ))}
            </div>
            <p style={{ fontSize: 11, color: 'var(--text-muted)', margin: '14px 0 0', fontFamily: "'Figtree', sans-serif", lineHeight: 1.5 }}>
              Calculated from price history, PSA population data, and market trends. Not financial advice.
            </p>
          </Panel>
        )
      })()}

      {/* ── Grading Calculator ──────────────────────────────────────── */}
      {card.raw_usd && card.psa10_usd && (
        <Panel>
          <SectionLabel>Grading Calculator</SectionLabel>
          <p style={{ fontSize: 13, color: 'var(--text-muted)', margin: '0 0 16px', fontFamily: "'Figtree', sans-serif", lineHeight: 1.5 }}>
            Based on PSA Standard tier (~$25 USD). Assumes a perfect PSA 10 result — actual outcome depends on card condition.
          </p>

          {isLotteryCard && (
            <div style={{ marginBottom: 16, padding: '10px 14px', background: 'rgba(224,123,57,0.08)', border: '1px solid rgba(224,123,57,0.25)', borderRadius: 10, fontSize: 13, fontFamily: "'Figtree', sans-serif", color: 'var(--text)', lineHeight: 1.6 }}>
              ⚠️ <strong>High-variance grade.</strong> PSA 10 is {psa10Multiple?.toFixed(0)}x the raw price
              {gemRate != null ? ` but only ${gemRate.toFixed(1)}% of copies gem` : ''}.
              The profit if PSA 10 figure below is the best-case outcome — expected value accounts for the realistic probability.
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 10, marginBottom: 18 }}>
            <StatTile label="Buy Raw"       value={`$${(card.raw_usd / 100).toFixed(0)}`} />
            <StatTile label="Grading Fee"   value="~$25" sub="PSA Standard" />
            <StatTile label="PSA 10 Value"  value={`$${(card.psa10_usd / 100).toFixed(0)}`} />
            <StatTile
              label="Best Case (PSA 10)"
              value={gradingProfitCents != null ? `${gradingProfitCents > 0 ? '+' : ''}$${(gradingProfitCents / 100).toFixed(0)}` : '—'}
              sub="if you pull a 10"
            />
            {expectedValueCents != null && (
              <StatTile
                label="Expected Value"
                value={`${expectedValueCents > 0 ? '+' : ''}$${(expectedValueCents / 100).toFixed(0)}`}
                sub={gemRate != null ? `at ${gemRate.toFixed(1)}% gem rate` : 'probability-weighted'}
                highlight={expectedValueCents > 0}
              />
            )}
          </div>

          {gradeMultiples.length > 0 && (
            <div>
              <SectionLabel>Grade Multiples vs Raw</SectionLabel>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                {gradeMultiples.map(g => (
                  <div key={g.label}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5 }}>
                      <span style={{ fontSize: 13, fontFamily: "'Figtree', sans-serif", color: 'var(--text)', fontWeight: 600 }}>
                        {g.label}
                        <span style={{ fontWeight: 400, color: 'var(--text-muted)', marginLeft: 6 }}>${(g.value / 100).toFixed(0)}</span>
                      </span>
                      <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "'Figtree', sans-serif", color: 'var(--text-muted)' }}>
                        {g.multiple?.toFixed(1)}x raw
                      </span>
                    </div>
                    <MeterBar
                      value={Math.min((g.multiple! / 6) * 100, 100)}
                      color={g.multiple! < 2 ? 'var(--green)' : g.multiple! < 4 ? '#f59e0b' : 'var(--primary)'}
                    />
                  </div>
                ))}
              </div>
            </div>
          )}

          {gemRate != null && totalGraded != null && (
            <div style={{ marginTop: 16, padding: '12px 14px', background: gemRate < 10 ? 'rgba(39,174,96,0.06)' : 'var(--bg-light)', border: `1px solid ${gemRate < 10 ? 'rgba(39,174,96,0.2)' : 'var(--border)'}`, borderRadius: 10, fontSize: 13, fontFamily: "'Figtree', sans-serif", color: 'var(--text)', lineHeight: 1.6 }}>
              <strong>{gemRate.toFixed(1)}% gem rate</strong> — {psa10Count?.toLocaleString()} PSA 10s out of {totalGraded?.toLocaleString()} graded.
              {' '}{gemRate < 10
                ? 'Hard card to grade — scarcity keeps the PSA 10 premium justified.'
                : gemRate > 40
                ? 'PSA 10s are relatively common — strong supply.'
                : 'Average difficulty to gem.'}
            </div>
          )}
        </Panel>
      )}

      {/* ── PSA Population ──────────────────────────────────────────── */}
      {psaPop && (
        <Panel>
          <SectionLabel>
            PSA Population{psaPop.variant && psaPop.variant !== 'Standard' ? ` — ${psaPop.variant}` : ''}
          </SectionLabel>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 10, marginBottom: 16 }}>
            {[
              { label: 'PSA 7',  value: psaPop.psa_7  },
              { label: 'PSA 8',  value: psaPop.psa_8  },
              { label: 'PSA 9',  value: psaPop.psa_9  },
              { label: 'PSA 10', value: psaPop.psa_10 },
              { label: 'Total',  value: psaPop.total_graded },
            ].map(stat => (
              <div key={stat.label} style={{ background: 'var(--bg-light)', borderRadius: 10, padding: '12px 14px' }}>
                <div style={{ fontSize: 18, fontWeight: 700, color: 'var(--text)', fontFamily: "'Figtree', sans-serif", lineHeight: 1 }}>
                  {stat.value?.toLocaleString() ?? '—'}
                </div>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4, fontFamily: "'Figtree', sans-serif", textTransform: 'uppercase', letterSpacing: 1, fontWeight: 700 }}>
                  {stat.label}
                </div>
              </div>
            ))}
          </div>

          {psaPop.total_graded > 0 && (
            <div style={{ marginBottom: 14 }}>
              <div style={{ display: 'flex', height: 8, borderRadius: 99, overflow: 'hidden', gap: 1, marginBottom: 8 }}>
                {[
                  { count: psaPop.psa_7,  color: '#94a3b8' },
                  { count: psaPop.psa_8,  color: '#60a5fa' },
                  { count: psaPop.psa_9,  color: '#34d399' },
                  { count: psaPop.psa_10, color: '#22c55e' },
                ].map((seg, i) => {
                  const pct = (seg.count / psaPop.total_graded) * 100
                  if (pct < 1) return null
                  return <div key={i} style={{ flex: `0 0 ${pct}%`, background: seg.color, height: '100%' }} />
                })}
              </div>
              <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
                {[
                  { label: 'PSA 7',  color: '#94a3b8', count: psaPop.psa_7  },
                  { label: 'PSA 8',  color: '#60a5fa', count: psaPop.psa_8  },
                  { label: 'PSA 9',  color: '#34d399', count: psaPop.psa_9  },
                  { label: 'PSA 10', color: '#22c55e', count: psaPop.psa_10 },
                ].map((item, i) => (
                  <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                    <div style={{ width: 8, height: 8, borderRadius: 2, background: item.color }} />
                    <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>
                      {item.label} ({((item.count / psaPop.total_graded) * 100).toFixed(0)}%)
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {gemRate != null && (
            <div style={{ fontSize: 13, fontFamily: "'Figtree', sans-serif", color: 'var(--text-muted)' }}>
              Gem rate:{' '}
              <strong style={{ color: gemRate < 5 ? 'var(--green)' : gemRate >= 20 ? 'var(--text)' : '#f59e0b' }}>
                {gemRate.toFixed(1)}%
              </strong>
              {' '}of all graded copies received PSA 10.
            </div>
          )}
        </Panel>
      )}

      {/* ── eBay Search ─────────────────────────────────────────────── */}
      <Panel>
        <SectionLabel>Search eBay</SectionLabel>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, alignItems: 'flex-start' }}>
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text-muted)', marginBottom: 7, fontFamily: "'Figtree', sans-serif" }}>🇬🇧 eBay UK</div>
            <div style={{ display: 'flex', gap: 8 }}>
              <EbayButton href={ebayUkForSale} label="For Sale"      flag="🛒" variant="primary" />
              <EbayButton href={ebayUkSold}    label="Sold Listings" flag="✅" />
            </div>
          </div>
          <div style={{ width: 1, background: 'var(--border)', alignSelf: 'stretch', margin: '0 4px' }} />
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text-muted)', marginBottom: 7, fontFamily: "'Figtree', sans-serif" }}>🇺🇸 eBay US</div>
            <div style={{ display: 'flex', gap: 8 }}>
              <EbayButton href={ebayUsForSale} label="For Sale"      flag="🛒" variant="primary" />
              <EbayButton href={ebayUsSold}    label="Sold Listings" flag="✅" />
            </div>
          </div>
        </div>
        <p style={{ fontSize: 11, color: 'var(--text-muted)', margin: '12px 0 0', fontFamily: "'Figtree', sans-serif" }}>
          Opens eBay pre-searched for this card. Always verify the listing matches before buying.
        </p>
      </Panel>

      {/* ── Live Deals / Listings (HIGH confidence only) ─────────────── */}
      {(hasDeals || hasListings) && (
        <Panel>
          <SectionLabel style={{ color: hasDeals ? 'var(--green)' : undefined }}>
            {hasDeals ? '🔥 Live Deals' : 'Live Listings'}
          </SectionLabel>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {(hasDeals ? ebayDeals : ebayListings).map((item: any, i: number) => {
              const price    = hasDeals ? (item.listing_price_cents / 100).toFixed(2) : (item.total_cost_cents / 100).toFixed(2)
              const currency = item.currency === 'GBP' ? '£' : '$'
              const discount = hasDeals ? item.discount_pct : null
              const fairValue = hasDeals && item.fair_value_cents ? (item.fair_value_cents / 100).toFixed(2) : null
              return (
                <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 12px', background: 'var(--bg-light)', borderRadius: 10, gap: 12, flexWrap: 'wrap' }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 3 }}>
                      <span style={{ fontSize: 14, fontWeight: 700, fontFamily: "'Figtree', sans-serif" }}>{currency}{price}</span>
                      {discount && (
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--green)', background: 'rgba(39,174,96,0.1)', padding: '1px 7px', borderRadius: 20, fontFamily: "'Figtree', sans-serif" }}>
                          {discount.toFixed(0)}% off
                        </span>
                      )}
                      {fairValue && (
                        <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>vs {currency}{fairValue} fair value</span>
                      )}
                    </div>
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>
                      {item.condition || 'Ungraded'} · {item.seller_username} ({item.seller_feedback_score?.toLocaleString()} feedback)
                    </div>
                  </div>
                  {item.item_web_url && (
                    <a href={item.item_web_url} target="_blank" rel="noopener noreferrer" style={{ background: 'var(--primary)', color: '#fff', padding: '6px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, textDecoration: 'none', fontFamily: "'Figtree', sans-serif", whiteSpace: 'nowrap' }}>
                      View on eBay
                    </a>
                  )}
                </div>
              )
            })}
          </div>
          <p style={{ fontSize: 11, color: 'var(--text-muted)', margin: '10px 0 0', fontFamily: "'Figtree', sans-serif" }}>
            {hasDeals
              ? 'Deals are pre-screened for 15%+ discount against market value. Verified sellers only. Always check the listing before buying.'
              : 'Verified high-confidence matches only. Prices scraped daily — always check the listing before buying.'
            }
          </p>
        </Panel>
      )}

      {/* ── Price History Chart ──────────────────────────────────────── */}
      {priceHistory.length > 1 && (
        <Panel style={{ paddingBottom: 32 }}>
          <SectionLabel>Price History</SectionLabel>
          <PriceChart data={priceHistory} height={280} />
        </Panel>
      )}
    </div>
  )
}
