import type { Merged } from './types'

export const ROUTE = 'portal'

export function candidate(latest: Merged | null): { stable?: string; canary?: string; target?: string; hosts: string[] } {
  const route = latest?.routes[ROUTE]
  const stable = route?.stable
  const other = latest?.dimensions.find((d) => d.version !== stable)
  const canary = route?.canary ?? other?.version
  const hosts = (latest?.dimensions ?? []).filter((d) => d.version === canary).map((d) => d.id)
  return { stable, canary, target: hosts[0], hosts }
}
