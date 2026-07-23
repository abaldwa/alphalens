import { create } from 'zustand'

/**
 * Single global piece of client state in the app: the ticker currently
 * shown on the /charts (Symbol Overview) route. `TickerLink` writes to
 * this store instead of encoding the ticker as a URL query param, so the
 * mounted TradingView widget can re-symbolize reactively rather than
 * remounting on navigation.
 */
interface TickerState {
  ticker: string | null
  setTicker: (ticker: string) => void
}

export const useTickerStore = create<TickerState>((set) => ({
  ticker: null,
  setTicker: (ticker) => set({ ticker }),
}))
