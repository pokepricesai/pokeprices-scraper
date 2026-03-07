// app/HomeClient.tsx
'use client'
import { useState, useEffect } from 'react'
import Link from 'next/link'
import { supabase, formatPrice, formatPct } from '@/lib/supabase'
import InlineChat from '@/components/InlineChat'
import SearchBar from '@/components/SearchBar'

const upcomingReleases = [
  { name: 'Perfect Order', date: 'Mar 27, 2026', confirmed: true, type: 'Main Set' },
  { name: 'Chaos Rising', date: 'May 22, 2026', confirmed: true, type: 'Main Set' },
  { name: 'Abyss Eye', date: 'Jul 2026 (est)', confirmed: false, type: 'Main Set' },
  { name: 'Celebration Collection', date: 'Nov 2026 (est)', confirmed: false, type: 'Special Set' },
]

const features = [
  { icon: '📊', title: 'Real Sales Data', desc: 'Daily prices from actual completed sales across major marketplaces.' },
  { icon: '💷', title: 'True Landed Cost', desc: 'VAT, shipping, customs, handling — the full picture for UK buyers.' },
  { icon: '📈', title: 'Trend Analysis', desc: '30 days, 6 months, or 5 years. Spot market movement early.' },
  { icon: '💎', title: 'Grading Intelligence', desc: 'Pop counts, grade premiums, and honest UK grading advice.' },
]

const faqs = [
  { q: 'Where does the pricing data come from?', a: 'All prices are sourced from actual completed sales on major marketplaces. We update daily so you always have current market values.' },
  { q: "Is this really free?", a: "Genuinely free. No login, no email capture, no premium tier. Revenue comes from optional affiliate links when you're ready to buy." },
  { q: 'Do you cover UK import costs?', a: 'Yes. We factor in VAT, Royal Mail handling fees, shipping, and customs so you see the true landed cost.' },
  { q: 'What grading companies do you track?', a: 'We track PSA and CGC prices and population data. Our grading advice covers PSA, CGC, BGS, SGC, and ACE.' },
]

interface MarketIndexRow {
  date: string
  total_raw_usd: number
  median_raw_usd: number
  raw_pct_30d?: number
}

interface WeeklyReportRow {
  category: string
  card_name: string
  set_name: string
  current_price: number
  metric_value: number
  metric_label: string
  card_slug: string
  card_url_slug: string
}

interface HeatmapCard {
  card_slug: string
  card_url_slug: string
  card_name: string
  set_name: string
  current_price: number
  pct_change: number
  price_usd: number
  color_band: string
  trend_quality: string
  is_recovery: boolean
}

interface HiddenGem {
  card_slug: string
  card_url_slug: string
  card_name: string
  set_name: string
  current_price: number
  pct_30d: number
  psa10_pop: number
  sales_30d: number
  live_listings: number
  gem_score: number
}

function Sparkles() {
  return (
    <>
      {[
        { top: '8%', left: '10%', size: 6, delay: '0s' },
        { top: '15%', right: '15%', size: 8, delay: '0.8s' },
        { top: '25%', left: '20%', size: 5, delay: '1.6s' },
        { top: '12%', right: '30%', size: 7, delay: '0.4s' },
        { top: '30%', left: '5%', size: 4, delay: '1.2s' },
        { top: '20%', right: '8%', size: 6, delay: '2s' },
        { top: '5%', left: '40%', size: 5, delay: '0.6s' },
        { top: '35%', right: '20%', size: 4, delay: '1.4s' },
      ].map((s, i) => (
        <div key={i} style={{
          position: 'absolute', ...s, width: s.size, height: s.size,
          background: 'white',
          clipPath: 'polygon(50% 0%, 61% 35%, 98% 35%, 68% 57%, 79% 91%, 50% 70%, 21% 91%, 32% 57%, 2% 35%, 39% 35%)',
          animation: `twinkle 2.5s ease-in-out ${s.delay} infinite`,
          pointerEvents: 'none', opacity: 0.6,
        }} />
      ))}
    </>
  )
}

// ── Mini sparkline SVG ────────────────────────────────────────
function Sparkline({ data, color = '#22c55e', height = 48 }: { data: number[], color?: string, height?: number }) {
  if (!data || data.length < 2) return null
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const w = 120, h = height
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w
    const y = h - ((v - min) / range) * (h - 6) - 3
    return `${x.toFixed(1)},${y.toFixed(1)}`
  })
  const lastPt = pts[pts.length - 1].split(',')
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} style={{ overflow: 'visible' }}>
      <polyline points={pts.join(' ')} fill="none" stroke={color} strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={lastPt[0]} cy={lastPt[1]} r={3} fill={color} />
    </svg>
  )
}

function heatColor(band: string) {
  switch (band) {
    case 'strong_up':   return { bg: 'rgba(34,197,94,0.15)',   border: 'rgba(34,197,94,0.3)',   text: '#16a34a' }
    case 'up':          return { bg: 'rgba(34,197,94,0.07)',   border: 'rgba(34,197,94,0.18)',  text: '#22c55e' }
    case 'strong_down': return { bg: 'rgba(239,68,68,0.15)',   border: 'rgba(239,68,68,0.3)',   text: '#dc2626' }
    case 'down':        return { bg: 'rgba(239,68,68,0.07)',   border: 'rgba(239,68,68,0.18)',  text: '#ef4444' }
    default:            return { bg: 'rgba(148,163,184,0.06)', border: 'rgba(148,163,184,0.18)', text: '#94a3b8' }
  }
}

function categoryMeta(cat: string) {
  switch (cat) {
    case 'top_riser':     return { label: '🚀 Top Riser (30d)',  color: '#22c55e' }
    case 'top_faller':    return { label: '📉 Top Faller (30d)', color: '#ef4444' }
    case 'most_volatile': return { label: '⚡ Most Volatile',  color: '#f59e0b' }
    case 'new_ath':       return { label: '🏆 New High',       color: '#a78bfa' }
    case 'most_traded':   return { label: '🔥 Most Traded',     color: '#3b82f6' }
    default:              return { label: cat,                  color: '#94a3b8' }
  }
}

export default function HomeClient() {
  const nextRelease = new Date('2026-03-27T00:00:00')
  const [countdown, setCountdown] = useState({ days: 0, hours: 0, mins: 0 })
  const [marketIndex, setMarketIndex] = useState<MarketIndexRow[]>([])
  const [totalMarket, setTotalMarket] = useState<{ value: number, pct30d: number | null } | null>(null)
  const [weeklyReport, setWeeklyReport] = useState<WeeklyReportRow[]>([])
  const [heatmap, setHeatmap] = useState<HeatmapCard[]>([])
  const [heatPeriod, setHeatPeriod] = useState<'7d' | '30d' | '90d'>('30d')
  const [hiddenGems, setHiddenGems] = useState<HiddenGem[]>([])

  // Countdown timer
  useEffect(() => {
    const tick = () => {
      const diff = nextRelease.getTime() - Date.now()
      if (diff > 0) setCountdown({
        days: Math.floor(diff / 86400000),
        hours: Math.floor((diff % 86400000) / 3600000),
        mins: Math.floor((diff % 3600000) / 60000),
      })
    }
    tick()
    const id = setInterval(tick, 60000)
    return () => clearInterval(id)
  }, [])

  // Analytics: market index + hidden gems (fast, load together)
  useEffect(() => {
    async function loadAnalytics() {
      const [indexRes, gemsRes] = await Promise.all([
        supabase.from('market_index')
          .select('date, total_raw_usd, median_raw_usd, raw_pct_30d')
          .order('date', { ascending: true })
          .limit(80),
        supabase.rpc('get_hidden_gems', { lim: 6 }),
      ])
      if (indexRes.data && indexRes.data.length > 0) {
        setMarketIndex(indexRes.data)
        const latest = indexRes.data[indexRes.data.length - 1]
        const prev = indexRes.data.length >= 2 ? indexRes.data[indexRes.data.length - 2] : null
        const pct30d = latest.raw_pct_30d != null
          ? Number(latest.raw_pct_30d)
          : prev ? ((latest.total_raw_usd - prev.total_raw_usd) / prev.total_raw_usd * 100) : null
        setTotalMarket({ value: latest.total_raw_usd, pct30d })
      }
      if (gemsRes.data && gemsRes.data.length > 0) setHiddenGems(gemsRes.data)
    }
    loadAnalytics()
  }, [])

  // Weekly report — separate with retry (heavier query, occasionally slow)
  useEffect(() => {
    let cancelled = false
    async function load(attempt = 1) {
      const { data } = await supabase.rpc('get_weekly_market_report')
      if (cancelled) return
      if (data && data.length > 0) {
        setWeeklyReport(data)
      } else if (attempt < 3) {
        setTimeout(() => load(attempt + 1), 1500 * attempt)
      }
    }
    load()
    return () => { cancelled = true }
  }, [])

  // Heatmap — reload when period changes
  useEffect(() => {
    async function loadHeatmap() {
      const res = await supabase.rpc('get_heatmap_cards', {
        period: heatPeriod, grade_filter: 'all', price_max: 0, lim: 60,
      })
      if (res.data && res.data.length > 0) setHeatmap(res.data)
    }
    loadHeatmap()
  }, [heatPeriod])

  const sparklineData = marketIndex.slice(-30).map(r => r.total_raw_usd / 100)
  const marketUp = (totalMarket?.pct30d ?? 0) >= 0

  return (
    <>
      {/* ── HERO ──────────────────────────────────────────────── */}
      <section style={{
        background: 'linear-gradient(170deg, #1a5fad 0%, #3b8fe8 35%, #6ab0f5 60%, #9dcbfa 80%, var(--bg) 100%)',
        padding: '40px 24px 70px', position: 'relative', overflow: 'hidden',
      }}>
        <Sparkles />
        <div style={{ maxWidth: 720, margin: '0 auto', textAlign: 'center', position: 'relative', zIndex: 1 }}>
          <img src="/logo.png" alt="PokePrices" style={{
            height: 120, margin: '0 auto 16px', display: 'block',
            filter: 'drop-shadow(0 4px 12px rgba(0,0,0,0.2))',
            animation: 'float 4s ease-in-out infinite',
          }} />
          <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginBottom: 18, flexWrap: 'wrap' }}>
            {['100% Free', 'No Login', 'No Data Collection'].map(pill => (
              <span key={pill} style={{
                background: 'rgba(255,255,255,0.15)', color: '#fff', fontSize: 11, fontWeight: 700,
                padding: '4px 12px', borderRadius: 20, border: '1px solid rgba(255,255,255,0.2)',
                letterSpacing: 0.3, backdropFilter: 'blur(4px)',
              }}>{pill}</span>
            ))}
          </div>
          <h1 style={{
            fontSize: 38, color: '#fff', margin: '0 0 10px', lineHeight: 1.15,
            textShadow: '0 2px 10px rgba(0,0,0,0.15)', fontFamily: "'Playfair Display', serif",
          }}>
            Know what your cards<br />are <span style={{ color: 'var(--accent)' }}>really</span> worth
          </h1>
          <p style={{ color: 'rgba(255,255,255,0.7)', fontSize: 15, margin: '0 0 20px', lineHeight: 1.6, fontFamily: "'Figtree', sans-serif" }}>
            Real market data for 40,000+ Pokemon cards. Ask anything — prices, trends, grading advice.
          </p>
          <div style={{ marginBottom: 16 }}>
            <SearchBar placeholder="Search cards or sets…" />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, margin: '0 auto 16px', maxWidth: 560 }}>
            <div style={{ flex: 1, height: 1, background: 'rgba(255,255,255,0.15)' }} />
            <span style={{ color: 'rgba(255,255,255,0.4)', fontSize: 12, fontFamily: "'Figtree', sans-serif", fontWeight: 600, letterSpacing: 0.5 }}>
              or ask the chatbot
            </span>
            <div style={{ flex: 1, height: 1, background: 'rgba(255,255,255,0.15)' }} />
          </div>
          <InlineChat />
          <p style={{ color: 'rgba(255,255,255,0.35)', fontSize: 11, marginTop: 12, fontFamily: "'Figtree', sans-serif" }}>
            Updated daily from actual sold listings
          </p>
        </div>
      </section>

      {/* ── MARKET INDEX BANNER ───────────────────────────────── */}
      {totalMarket && (
        <section style={{ padding: '0 24px', maxWidth: 960, margin: '-28px auto 0', position: 'relative', zIndex: 10 }}>
          <div style={{
            background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 18,
            boxShadow: '0 4px 24px rgba(26,95,173,0.10)', padding: '20px 28px',
            display: 'grid', gridTemplateColumns: '1fr auto', gap: 24, alignItems: 'center',
          }}>
            <div>
              <p style={{ color: 'var(--text-muted)', fontSize: 11, fontWeight: 700, letterSpacing: 2, textTransform: 'uppercase', margin: '0 0 6px', fontFamily: "'Figtree', sans-serif" }}>
                Pokémon TCG Market Index
              </p>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 12, flexWrap: 'wrap' }}>
                <span style={{ fontSize: 34, fontWeight: 900, color: 'var(--text)', fontFamily: "'Figtree', sans-serif", letterSpacing: -1 }}>
                  ${(totalMarket.value / 100).toLocaleString('en-US', { maximumFractionDigits: 0 })}
                </span>
                {totalMarket.pct30d != null && (
                  <span style={{
                    fontSize: 13, fontWeight: 700,
                    color: marketUp ? '#22c55e' : '#ef4444',
                    background: marketUp ? 'rgba(34,197,94,0.1)' : 'rgba(239,68,68,0.1)',
                    padding: '3px 10px', borderRadius: 20, fontFamily: "'Figtree', sans-serif",
                  }}>
                    {marketUp ? '▲' : '▼'} {Math.abs(totalMarket.pct30d).toFixed(1)}% 30d
                  </span>
                )}
              </div>
              <p style={{ color: 'var(--text-muted)', fontSize: 12, margin: '4px 0 0', fontFamily: "'Figtree', sans-serif" }}>
                Total tracked raw card value · {marketIndex.length > 0 ? `${marketIndex.length} data points from ${marketIndex[0].date.slice(0, 7)}` : ''}
              </p>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 4 }}>
              <Sparkline data={sparklineData} color={marketUp ? '#22c55e' : '#ef4444'} height={48} />
              <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>30 day trend</span>
            </div>
          </div>
        </section>
      )}

      {/* ── WEEKLY MARKET REPORT ──────────────────────────────── */}
      {weeklyReport.length > 0 && (
        <section style={{ padding: '32px 24px 12px', maxWidth: 960, margin: '0 auto' }}>
          <h2 style={{ fontSize: 18, fontWeight: 800, margin: '0 0 4px', fontFamily: "'Figtree', sans-serif" }}>
            This week in the market
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: '0 0 14px', fontFamily: "'Figtree', sans-serif" }}>
            Auto-generated daily from price data — no editorial, just numbers
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 10 }}>
            {weeklyReport.map(row => {
              const { label, color } = categoryMeta(row.category)
              const priceUsd = row.current_price / 100
              return (
                <Link key={row.category} href={`/set/${encodeURIComponent(row.set_name)}/card/${row.card_url_slug || row.card_slug}`} style={{ textDecoration: 'none' }}>
                  <div style={{
                    background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 14,
                    padding: '14px 16px', height: '100%', boxSizing: 'border-box',
                    transition: 'transform 0.15s, box-shadow 0.15s', cursor: 'pointer',
                  }}
                    onMouseEnter={e => { const el = e.currentTarget as HTMLDivElement; el.style.transform = 'translateY(-2px)'; el.style.boxShadow = '0 6px 20px rgba(0,0,0,0.08)' }}
                    onMouseLeave={e => { const el = e.currentTarget as HTMLDivElement; el.style.transform = ''; el.style.boxShadow = '' }}
                  >
                    <div style={{ fontSize: 11, fontWeight: 800, color, letterSpacing: 0.5, marginBottom: 7, fontFamily: "'Figtree', sans-serif", textTransform: 'uppercase' }}>
                      {label}
                    </div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text)', lineHeight: 1.3, marginBottom: 4, fontFamily: "'Figtree', sans-serif",
                      display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                      {row.card_name}
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8, fontFamily: "'Figtree', sans-serif" }}>
                      {row.set_name}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: 'auto' }}>
                      <span style={{ fontSize: 14, fontWeight: 800, color: 'var(--text)', fontFamily: "'Figtree', sans-serif" }}>
                        ${priceUsd >= 100 ? priceUsd.toFixed(0) : priceUsd.toFixed(2)}
                      </span>
                      <span style={{ fontSize: 11, color, fontWeight: 700, fontFamily: "'Figtree', sans-serif", textAlign: 'right', maxWidth: 90 }}>
                        {row.metric_label}
                      </span>
                    </div>
                  </div>
                </Link>
              )
            })}
          </div>
        </section>
      )}

      {/* ── MARKET HEATMAP ────────────────────────────────────── */}
      <section style={{ padding: '8px 24px 40px', maxWidth: 960, margin: '0 auto' }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 16, flexWrap: 'wrap', gap: 12 }}>
          <div>
            <h2 style={{ fontSize: 24, margin: '0 0 4px', fontFamily: "'Playfair Display', serif" }}>Market Heatmap</h2>
            <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: 0, fontFamily: "'Figtree', sans-serif" }}>
              Price movement across the market — green rising, red falling
            </p>
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            {(['7d', '30d', '90d'] as const).map(p => (
              <button key={p} onClick={() => setHeatPeriod(p)} style={{
                padding: '6px 14px', borderRadius: 20, fontSize: 12, fontWeight: 700,
                border: '1px solid var(--border)', cursor: 'pointer', fontFamily: "'Figtree', sans-serif",
                background: heatPeriod === p ? '#1a5fad' : 'var(--card)',
                color: heatPeriod === p ? '#fff' : 'var(--text-muted)',
                transition: 'all 0.15s',
              }}>{p}</button>
            ))}
          </div>
        </div>
        {heatmap.length > 0 ? (
          <>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))', gap: 6 }}>
              {heatmap.slice(0, 48).map(card => {
                const { bg, border, text } = heatColor(card.color_band)
                return (
                  <Link key={card.card_slug} href={`/set/${encodeURIComponent(card.set_name)}/card/${card.card_url_slug || card.card_slug}`} style={{ textDecoration: 'none' }}>
                    <div style={{ background: bg, border: `1px solid ${border}`, borderRadius: 10, padding: '10px 10px 8px', cursor: 'pointer', transition: 'opacity 0.12s' }}
                      onMouseEnter={e => (e.currentTarget as HTMLDivElement).style.opacity = '0.75'}
                      onMouseLeave={e => (e.currentTarget as HTMLDivElement).style.opacity = '1'}
                    >
                      <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text)', lineHeight: 1.3, marginBottom: 3, fontFamily: "'Figtree', sans-serif",
                        display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                        {card.card_name}
                      </div>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 5, fontFamily: "'Figtree', sans-serif", whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {card.set_name}
                      </div>
                      {card.is_recovery && (
                        <div style={{ fontSize: 9, fontWeight: 700, color: '#f59e0b', letterSpacing: 0.3, marginBottom: 4, fontFamily: "'Figtree', sans-serif" }}>
                          ↩ RECOVERY
                        </div>
                      )}
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                        <span style={{ fontSize: 11, fontWeight: 800, color: 'var(--text)', fontFamily: "'Figtree', sans-serif" }}>
                          ${card.price_usd >= 100 ? Math.round(card.price_usd) : card.price_usd.toFixed(2)}
                        </span>
                        <span style={{ fontSize: 12, fontWeight: 800, color: text, fontFamily: "'Figtree', sans-serif" }}>
                          {card.pct_change > 0 ? '+' : ''}{card.pct_change.toFixed(1)}%
                        </span>
                      </div>
                    </div>
                  </Link>
                )
              })}
            </div>
            <div style={{ display: 'flex', gap: 16, marginTop: 14, justifyContent: 'center', flexWrap: 'wrap' }}>
              {[
                { band: 'strong_up', label: '+10% or more' },
                { band: 'up', label: '+2% to +10%' },
                { band: 'flat', label: 'Flat (±2%)' },
                { band: 'down', label: '-2% to -10%' },
                { band: 'strong_down', label: '-10% or more' },
              ].map(({ band, label }) => {
                const { bg, border } = heatColor(band)
                return (
                  <div key={band} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                    <div style={{ width: 12, height: 12, borderRadius: 3, background: bg, border: `1px solid ${border}` }} />
                    <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>{label}</span>
                  </div>
                )
              })}
            </div>
          </>
        ) : (
          <div style={{ textAlign: 'center', padding: '48px 0', color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>
            Loading heatmap…
          </div>
        )}
      </section>

      {/* ── HIDDEN GEMS ───────────────────────────────────────── */}
      {hiddenGems.length > 0 && (
        <section style={{ padding: '0 24px 44px', maxWidth: 960, margin: '0 auto' }}>
          <div style={{
            background: 'linear-gradient(135deg, rgba(167,139,250,0.07), rgba(59,130,246,0.05))',
            border: '1px solid rgba(167,139,250,0.18)', borderRadius: 18, padding: '24px',
          }}>
            <h2 style={{ fontSize: 20, margin: '0 0 4px', fontFamily: "'Playfair Display', serif" }}>💎 Hidden Gems</h2>
            <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: '0 0 16px', fontFamily: "'Figtree', sans-serif" }}>
              Rising price · low pop · under the radar — cards collectors are sleeping on
            </p>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 10 }}>
              {hiddenGems.map(gem => {
                const priceUsd = gem.current_price / 100
                return (
                  <Link key={gem.card_slug} href={`/set/${encodeURIComponent(gem.set_name)}/card/${gem.card_url_slug || gem.card_slug}`} style={{ textDecoration: 'none' }}>
                    <div style={{
                      background: 'var(--card)', borderRadius: 12, border: '1px solid var(--border)',
                      padding: '12px 14px', display: 'flex', alignItems: 'center', gap: 12,
                      cursor: 'pointer', transition: 'transform 0.15s',
                    }}
                      onMouseEnter={e => (e.currentTarget as HTMLDivElement).style.transform = 'translateY(-2px)'}
                      onMouseLeave={e => (e.currentTarget as HTMLDivElement).style.transform = ''}
                    >
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text)', lineHeight: 1.3, fontFamily: "'Figtree', sans-serif", whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {gem.card_name}
                        </div>
                        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2, fontFamily: "'Figtree', sans-serif" }}>
                          {gem.set_name}
                          {gem.psa10_pop > 0 && <span style={{ marginLeft: 6, color: '#a78bfa' }}>pop {gem.psa10_pop}</span>}
                        </div>
                      </div>
                      <div style={{ textAlign: 'right', flexShrink: 0 }}>
                        <div style={{ fontSize: 14, fontWeight: 800, color: 'var(--text)', fontFamily: "'Figtree', sans-serif" }}>
                          ${priceUsd >= 100 ? priceUsd.toFixed(0) : priceUsd.toFixed(2)}
                        </div>
                        {gem.pct_30d != null && (
                          <div style={{ fontSize: 12, fontWeight: 700, color: '#22c55e', fontFamily: "'Figtree', sans-serif" }}>
                            +{Number(gem.pct_30d).toFixed(1)}% 30d
                          </div>
                        )}
                      </div>
                      <div style={{
                        width: 34, height: 34, borderRadius: '50%', flexShrink: 0,
                        background: 'linear-gradient(135deg, #a78bfa, #3b82f6)',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        fontSize: 11, fontWeight: 900, color: '#fff', fontFamily: "'Figtree', sans-serif",
                      }}>
                        {gem.gem_score}
                      </div>
                    </div>
                  </Link>
                )
              })}
            </div>
          </div>
        </section>
      )}

      {/* ── NEXT RELEASE ──────────────────────────────────────── */}
      <section style={{ padding: '0 24px 40px', maxWidth: 900, margin: '0 auto' }}>
        <div style={{ background: 'var(--card)', borderRadius: 18, border: '1px solid var(--border)', overflow: 'hidden', boxShadow: '0 2px 15px rgba(37,99,168,0.06)' }}>
          <div style={{ background: 'linear-gradient(135deg, #1a5fad, #2874c8)', padding: '20px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 14 }}>
            <div>
              <p style={{ color: 'rgba(255,255,255,0.5)', fontSize: 11, letterSpacing: 2, margin: '0 0 2px', textTransform: 'uppercase', fontWeight: 700, fontFamily: "'Figtree', sans-serif" }}>Next Release</p>
              <h3 style={{ color: '#fff', fontSize: 22, margin: 0, fontWeight: 800, fontFamily: "'Playfair Display', serif" }}>Perfect Order</h3>
              <p style={{ color: 'rgba(255,255,255,0.6)', fontSize: 13, margin: '2px 0 0', fontFamily: "'Figtree', sans-serif" }}>March 27, 2026</p>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              {[{ val: countdown.days, label: 'Days' }, { val: countdown.hours, label: 'Hrs' }, { val: countdown.mins, label: 'Min' }].map(t => (
                <div key={t.label} style={{ background: 'rgba(255,255,255,0.1)', borderRadius: 12, padding: '8px 12px', textAlign: 'center', minWidth: 46, border: '1px solid rgba(255,203,5,0.2)' }}>
                  <div style={{ color: 'var(--accent)', fontSize: 20, fontWeight: 800, lineHeight: 1, fontFamily: "'Figtree', sans-serif" }}>{t.val}</div>
                  <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 10, marginTop: 2, textTransform: 'uppercase', letterSpacing: 1, fontWeight: 700, fontFamily: "'Figtree', sans-serif" }}>{t.label}</div>
                </div>
              ))}
            </div>
          </div>
          <div style={{ padding: '16px 24px' }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 10 }}>
              {upcomingReleases.map(r => (
                <div key={r.name} style={{ padding: '12px 14px', background: 'var(--bg-light)', borderRadius: 12, border: '1px solid var(--border-light)' }}>
                  <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)', marginBottom: 2, fontFamily: "'Figtree', sans-serif" }}>{r.name}</div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: "'Figtree', sans-serif" }}>
                    {r.date}
                    {!r.confirmed && <span style={{ background: 'rgba(255,165,0,0.12)', color: '#b8741f', fontSize: 10, padding: '1px 6px', borderRadius: 4, marginLeft: 6, fontWeight: 700 }}>Rumoured</span>}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* ── BUILT DIFFERENT ───────────────────────────────────── */}
      <section style={{ padding: '16px 24px 44px', maxWidth: 900, margin: '0 auto' }}>
        <h2 style={{ fontSize: 24, textAlign: 'center', margin: '0 0 6px', fontFamily: "'Playfair Display', serif" }}>Built different</h2>
        <p style={{ color: 'var(--text-muted)', textAlign: 'center', fontSize: 14, margin: '0 0 28px', fontFamily: "'Figtree', sans-serif" }}>No login. No paywall. No data collection.</p>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 14 }}>
          {features.map((f, i) => (
            <div key={f.title} className={`card-hover animate-fade-in-up delay-${i + 1}`} style={{ background: 'var(--card)', borderRadius: 16, padding: '22px 18px', border: '1px solid var(--border)', textAlign: 'center' }}>
              <div style={{ fontSize: 30, marginBottom: 8 }}>{f.icon}</div>
              <h3 style={{ fontSize: 15, fontWeight: 800, color: 'var(--text)', margin: '0 0 6px', fontFamily: "'Figtree', sans-serif" }}>{f.title}</h3>
              <p style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5, margin: 0, fontFamily: "'Figtree', sans-serif" }}>{f.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ── STATS BAR ─────────────────────────────────────────── */}
      <section style={{ background: 'linear-gradient(135deg, #1a5fad, #2874c8)', padding: '30px 24px' }}>
        <div style={{ maxWidth: 900, margin: '0 auto', display: 'flex', justifyContent: 'space-around', flexWrap: 'wrap', gap: 20 }}>
          {[
            { val: '40,000+', label: 'Cards Tracked' },
            { val: '156', label: 'Sets Covered' },
            { val: '5+ Years', label: 'Price History' },
            { val: 'Daily', label: 'Price Updates' },
            { val: totalMarket ? `$${(totalMarket.value / 100 / 1000000).toFixed(1)}M` : '—', label: 'Market Tracked' },
          ].map(s => (
            <div key={s.label} style={{ textAlign: 'center' }}>
              <div style={{ color: 'var(--accent)', fontSize: 26, fontWeight: 900, fontFamily: "'Figtree', sans-serif" }}>{s.val}</div>
              <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 12, marginTop: 2, letterSpacing: 0.5, fontWeight: 700, fontFamily: "'Figtree', sans-serif" }}>{s.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── FAQ ───────────────────────────────────────────────── */}
      <section style={{ padding: '44px 24px', maxWidth: 680, margin: '0 auto' }}>
        <h2 style={{ fontSize: 24, textAlign: 'center', margin: '0 0 24px', fontFamily: "'Playfair Display', serif" }}>Questions collectors ask</h2>
        {faqs.map((faq, i) => (
          <details key={i} style={{ background: 'var(--card)', borderRadius: 14, border: '1px solid var(--border)', marginBottom: 8, overflow: 'hidden' }}>
            <summary style={{ padding: '14px 18px', fontSize: 14, fontWeight: 700, color: 'var(--text)', cursor: 'pointer', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontFamily: "'Figtree', sans-serif" }}>
              {faq.q}
              <span style={{ color: 'var(--text-muted)', fontSize: 18, fontWeight: 300, marginLeft: 8 }}>+</span>
            </summary>
            <div style={{ padding: '0 18px 14px', fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.6, fontFamily: "'Figtree', sans-serif" }}>{faq.a}</div>
          </details>
        ))}
      </section>
    </>
  )
}
