import { useState } from 'react'
import {
  Activity,
  Bell,
  LayoutDashboard,
  Network,
  Sparkles,
  SlidersHorizontal,
} from 'lucide-react'
import { Header } from './components/Header'
import { AboutModal } from './components/AboutModal'
import { useOperator, useRefreshInterval } from './hooks/useOperator'
import { PlantOverview } from './views/PlantOverview'
import { LineView } from './views/LineView'
import { AssetDetail } from './views/AssetDetail'
import { AlertConsole } from './views/AlertConsole'
import { ScenarioControls } from './views/ScenarioControls'
import { Observability } from './views/Observability'
import { AskAssistant } from './views/AskAssistant'

export type Route =
  | { view: 'overview' }
  | { view: 'line'; lineId: string }
  | { view: 'asset'; assetId: string; lineId?: string }
  | { view: 'alerts'; assetId?: string; lineId?: string }
  | { view: 'scenario' }
  | { view: 'observability' }
  | { view: 'genie' }

export type Navigate = (r: Route) => void

const NAV: { key: Route['view']; label: string; icon: typeof Activity; route: Route }[] = [
  { key: 'overview', label: 'Plant Overview', icon: LayoutDashboard, route: { view: 'overview' } },
  { key: 'alerts', label: 'Alert Console', icon: Bell, route: { view: 'alerts' } },
  { key: 'genie', label: 'Ask the Assistant', icon: Sparkles, route: { view: 'genie' } },
  { key: 'scenario', label: 'Scenario Controls', icon: SlidersHorizontal, route: { view: 'scenario' } },
  {
    key: 'observability',
    label: 'Architecture & Observability',
    icon: Network,
    route: { view: 'observability' },
  },
]

export default function App() {
  const [route, setRoute] = useState<Route>({ view: 'overview' })
  const [operator, setOperator] = useOperator()
  const [interval, setInterval] = useRefreshInterval()
  const [aboutOpen, setAboutOpen] = useState(false)

  const navigate: Navigate = (r) => {
    setRoute(r)
    window.scrollTo({ top: 0 })
  }

  const activeNav: Route['view'] =
    route.view === 'line' || route.view === 'asset' ? 'overview' : route.view

  return (
    <div className="flex min-h-screen flex-col bg-slate-100">
      <Header
        operator={operator}
        onOperatorChange={setOperator}
        interval={interval}
        onIntervalChange={setInterval}
        onAbout={() => setAboutOpen(true)}
      />

      <div className="mx-auto flex w-full max-w-[1600px] flex-1 gap-0">
        {/* Left nav */}
        <nav
          aria-label="Primary"
          className="hidden w-56 shrink-0 border-r border-slate-200 bg-white/70 p-3 md:block"
        >
          <ul className="space-y-1">
            {NAV.map((item) => {
              const Icon = item.icon
              const active = activeNav === item.key
              return (
                <li key={item.key}>
                  <button
                    onClick={() => navigate(item.route)}
                    aria-current={active ? 'page' : undefined}
                    className={`flex w-full items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm font-medium transition ${
                      active
                        ? 'bg-sky-50 text-sky-700'
                        : 'text-slate-600 hover:bg-slate-100'
                    }`}
                  >
                    <Icon size={17} />
                    {item.label}
                  </button>
                </li>
              )
            })}
          </ul>
        </nav>

        {/* Mobile nav */}
        <div className="md:hidden">
          <div className="scroll-x fixed bottom-0 left-0 z-40 w-full border-t border-slate-200 bg-white">
            <div className="flex min-w-max">
              {NAV.map((item) => {
                const Icon = item.icon
                const active = activeNav === item.key
                return (
                  <button
                    key={item.key}
                    onClick={() => navigate(item.route)}
                    className={`flex flex-1 flex-col items-center gap-0.5 px-3 py-2 text-[10px] font-medium ${
                      active ? 'text-sky-700' : 'text-slate-500'
                    }`}
                  >
                    <Icon size={18} />
                    {item.label.split(' ')[0]}
                  </button>
                )
              })}
            </div>
          </div>
        </div>

        {/* Content */}
        <main className="min-w-0 flex-1 px-4 py-5 pb-20 md:pb-5">
          {route.view === 'overview' && <PlantOverview navigate={navigate} interval={interval} />}
          {route.view === 'line' && (
            <LineView lineId={route.lineId} navigate={navigate} interval={interval} />
          )}
          {route.view === 'asset' && (
            <AssetDetail
              assetId={route.assetId}
              lineId={route.lineId}
              navigate={navigate}
              interval={interval}
            />
          )}
          {route.view === 'alerts' && (
            <AlertConsole
              navigate={navigate}
              interval={interval}
              operator={operator}
              initialAssetId={route.assetId}
              initialLineId={route.lineId}
            />
          )}
          {route.view === 'scenario' && (
            <ScenarioControls interval={interval} operator={operator} />
          )}
          {route.view === 'observability' && <Observability interval={interval} />}
          {route.view === 'genie' && <AskAssistant />}
        </main>
      </div>

      <AboutModal open={aboutOpen} onClose={() => setAboutOpen(false)} />
    </div>
  )
}
