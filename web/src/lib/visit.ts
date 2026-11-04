export type DemoStep = 'fired' | 'broke-dimension'

export function reportStep(step: DemoStep): void {
  document.dispatchEvent(new CustomEvent('najafov:action', { detail: step }))
}
